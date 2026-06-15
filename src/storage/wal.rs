use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

pub const DELETED: u8 = 0x01;
pub const IS_CREATE: u8 = 0x02;
pub const HAS_LEASE: u8 = 0x04;

// ── Overlong varint helpers ───────────────────────────────────────

/// Encode a u32 as a 5-byte overlong protobuf varint.
/// 5 bytes × 7 bits = 35 bits, enough for any u32 (max 32 bits).
/// The last byte only contributes 4 bits (bits 28-31).
pub fn encode_overlong_u32(v: u32) -> [u8; 5] {
    let mut buf = [0u8; 5];
    for (i, b) in buf.iter_mut().enumerate().take(4) {
        *b = ((v >> (i * 7)) as u8 & 0x7f) | 0x80;
    }
    buf[4] = ((v >> 28) as u8) & 0x0f;
    buf
}

/// Decode a fixed 5-byte overlong protobuf varint.
pub fn decode_overlong_u32(buf: &[u8]) -> Option<u32> {
    if buf.len() < 5 {
        return None;
    }
    let mut v = 0u64;
    for (i, &b) in buf.iter().enumerate().take(5) {
        v |= ((b & 0x7f) as u64) << (i * 7);
    }
    Some(v as u32)
}

/// Encode a u64 as a 10-byte overlong protobuf varint.
/// 10 bytes × 7 bits = 70 bits, enough for any u64 (max 64 bits).
/// The last byte only contributes 1 bit (bit 63).
pub fn encode_overlong_u64(v: u64) -> [u8; 10] {
    let mut buf = [0u8; 10];
    for (i, b) in buf.iter_mut().enumerate().take(9) {
        *b = ((v >> (i * 7)) as u8 & 0x7f) | 0x80;
    }
    buf[9] = ((v >> 63) as u8) & 0x01;
    buf
}

/// Decode a fixed 10-byte overlong protobuf varint.
pub fn decode_overlong_u64(buf: &[u8]) -> Option<u64> {
    if buf.len() < 10 {
        return None;
    }
    let mut v = 0u64;
    for (i, &b) in buf.iter().enumerate().take(10) {
        v |= ((b & 0x7f) as u64) << (i * 7);
    }
    Some(v)
}

/// Encode a u64 as a standard minimal protobuf varint.
pub fn encode_varint(mut v: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(10);
    loop {
        if v < 0x80 {
            buf.push(v as u8);
            break;
        }
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf
}

/// Decode a standard protobuf varint from the start of `buf`.
pub fn decode_varint(buf: &[u8]) -> Option<(u64, usize)> {
    let mut value = 0u64;
    let mut shift = 0;
    for (i, &b) in buf.iter().enumerate() {
        value |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 {
            return Some((value, i + 1));
        }
        shift += 7;
        if shift > 63 {
            return None;
        }
    }
    None
}

// ── Protobuf kv_bytes encoder ─────────────────────────────────────

/// Encodes a `mvccpb.KeyValue` protobuf message with overlong varints
/// for key_length (4B) and mod_revision (8B). Returns (kv_bytes,
/// key_offset, mod_rev_offset) where offsets point to the varint data
/// within kv_bytes (past the field tag).
pub fn encode_kv(
    key: &[u8],
    value: &[u8],
    create_revision: i64,
    mod_revision: i64,
    version: i64,
    lease: i64,
) -> (Vec<u8>, u16, u16) {
    let mut buf = Vec::new();

    // Field 1: key (bytes, wire type 2, field number 1)
    buf.push(0x0a); // tag = (1 << 3) | 2
    let key_offset = buf.len() as u16;
    buf.extend_from_slice(&encode_overlong_u32(key.len() as u32));
    buf.extend_from_slice(key);

    // Field 2: create_revision (int64, wire type 0, field number 2)
    buf.push(0x10); // tag = (2 << 3) | 0
    buf.extend_from_slice(&encode_varint(create_revision as u64));

    // Field 3: mod_revision (int64, wire type 0, field number 3)
    buf.push(0x18); // tag = (3 << 3) | 0
    let mod_rev_offset = buf.len() as u16;
    buf.extend_from_slice(&encode_overlong_u64(mod_revision as u64));

    // Field 4: version (int64, wire type 0, field number 4)
    buf.push(0x20); // tag = (4 << 3) | 0
    buf.extend_from_slice(&encode_varint(version as u64));

    // Field 5: value (bytes, wire type 2, field number 5)
    buf.push(0x2a); // tag = (5 << 3) | 2
    buf.extend_from_slice(&encode_varint(value.len() as u64));
    buf.extend_from_slice(value);

    // Field 6: lease (int64, wire type 0, field number 6)
    buf.push(0x30); // tag = (6 << 3) | 0
    buf.extend_from_slice(&encode_varint(lease as u64));

    (buf, key_offset, mod_rev_offset)
}

pub fn read_key_from_kv(kv_bytes: &[u8], key_offset: u16) -> Option<&[u8]> {
    let ofs = key_offset as usize;
    if ofs >= kv_bytes.len() {
        return None;
    }
    let (key_len, varint_size) = decode_varint(&kv_bytes[ofs..])?;
    let key_len = key_len as usize;
    let start = ofs + varint_size;
    if start + key_len > kv_bytes.len() {
        return None;
    }
    Some(&kv_bytes[start..start + key_len])
}

pub fn read_mod_revision_from_kv(kv_bytes: &[u8], mod_rev_offset: u16) -> Option<i64> {
    let ofs = mod_rev_offset as usize;
    if ofs + 10 > kv_bytes.len() {
        return None;
    }
    decode_overlong_u64(&kv_bytes[ofs..]).map(|v| v as i64)
}

// ── KvWalRecord (protobuf-native WAL format) ──────────────────────

pub const KV_HEADER_SIZE: usize = 9; // flags(1) + key_offset(2) + mod_rev_offset(2) + rec_len(4)
pub const KV_CRC_SIZE: usize = 4;

/// A WAL record in the protobuf-native format.
///
/// Layout on disk:
///   [flags(1) | key_offset(2) | mod_rev_offset(2) | rec_len(4) | kv_bytes(N) | crc32(4)]
///
/// `kv_bytes` is a valid `mvccpb.KeyValue` protobuf message with
/// overlong varints for key_length and mod_revision (allowing O(1)
/// field access during scan via the header offsets).
#[derive(Debug, Clone)]
pub struct KvWalRecord {
    pub flags: u8,
    pub kv_bytes: Vec<u8>,
    pub key_offset: u16,
    pub mod_rev_offset: u16,
    pub rec_len: u32,
    pub crc: u32,
}

impl KvWalRecord {
    /// Create a new record from its components. Computes kv_bytes,
    /// offsets, rec_len, and CRC.
    pub fn new(
        flags: u8,
        key: &[u8],
        value: &[u8],
        create_revision: i64,
        mod_revision: i64,
        version: i64,
        lease: i64,
    ) -> Self {
        let (kv_bytes, key_offset, mod_rev_offset) =
            encode_kv(key, value, create_revision, mod_revision, version, lease);
        let rec_len = (KV_HEADER_SIZE + kv_bytes.len() + KV_CRC_SIZE) as u32;

        let mut crc_data = Vec::with_capacity(KV_HEADER_SIZE + kv_bytes.len());
        crc_data.push(flags);
        crc_data.extend_from_slice(&key_offset.to_le_bytes());
        crc_data.extend_from_slice(&mod_rev_offset.to_le_bytes());
        crc_data.extend_from_slice(&rec_len.to_le_bytes());
        crc_data.extend_from_slice(&kv_bytes);
        let crc = crc32c(&crc_data);

        Self {
            flags,
            kv_bytes,
            key_offset,
            mod_rev_offset,
            rec_len,
            crc,
        }
    }

    /// Serialize the record to bytes for writing to the WAL.
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.rec_len as usize);
        buf.push(self.flags);
        buf.extend_from_slice(&self.key_offset.to_le_bytes());
        buf.extend_from_slice(&self.mod_rev_offset.to_le_bytes());
        buf.extend_from_slice(&self.rec_len.to_le_bytes());
        buf.extend_from_slice(&self.kv_bytes);
        buf.extend_from_slice(&self.crc.to_le_bytes());
        buf
    }

    /// Deserialize a record from raw bytes. Returns (record, bytes_consumed).
    pub fn deserialize(data: &[u8]) -> io::Result<(Self, usize)> {
        if data.len() < KV_HEADER_SIZE + KV_CRC_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "record too short",
            ));
        }

        let flags = data[0];
        let key_offset = u16::from_le_bytes([data[1], data[2]]);
        let mod_rev_offset = u16::from_le_bytes([data[3], data[4]]);
        let rec_len = u32::from_le_bytes([data[5], data[6], data[7], data[8]]);

        let min_rec_len = KV_HEADER_SIZE + KV_CRC_SIZE;
        if (rec_len as usize) < min_rec_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "rec_len too small",
            ));
        }

        if (rec_len as usize) > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "record extends past buffer",
            ));
        }

        let kv_len = rec_len as usize - KV_HEADER_SIZE - KV_CRC_SIZE;
        let kv_start = KV_HEADER_SIZE;
        let kv_end = kv_start + kv_len;
        let crc_start = kv_end;

        let kv_bytes = data[kv_start..kv_end].to_vec();
        let stored_crc = u32::from_le_bytes([
            data[crc_start],
            data[crc_start + 1],
            data[crc_start + 2],
            data[crc_start + 3],
        ]);

        let computed = crc32c(&data[..kv_end]);
        if computed != stored_crc {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "CRC mismatch"));
        }

        if (key_offset as usize) >= kv_bytes.len()
            || (mod_rev_offset as usize) + 10 > kv_bytes.len()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "offset out of bounds",
            ));
        }

        Ok((
            Self {
                flags,
                kv_bytes,
                key_offset,
                mod_rev_offset,
                rec_len,
                crc: stored_crc,
            },
            rec_len as usize,
        ))
    }

    /// Extract the key from kv_bytes using the stored offset (O(1), no protobuf decode).
    pub fn key(&self) -> Option<&[u8]> {
        read_key_from_kv(&self.kv_bytes, self.key_offset)
    }

    /// Extract the mod_revision from kv_bytes using the stored offset (O(1)).
    pub fn mod_revision(&self) -> Option<i64> {
        read_mod_revision_from_kv(&self.kv_bytes, self.mod_rev_offset)
    }
}

// ── WalFile ───────────────────────────────────────────────────────

#[derive(Debug)]
pub struct WalFile {
    pub file: std::fs::File,
    pub path: String,
}

impl WalFile {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path.as_ref())?;
        Ok(Self {
            file,
            path: path.as_ref().to_string_lossy().to_string(),
        })
    }

    pub fn scan_kv<F>(&mut self, offset: u64, f: F) -> io::Result<u64>
    where
        F: FnMut(&KvWalRecord),
    {
        self.file.seek(SeekFrom::Start(offset))?;
        let mut buf = Vec::new();
        self.file.read_to_end(&mut buf)?;
        let mut f = f;
        let mut ofs = 0;
        while ofs < buf.len() {
            match KvWalRecord::deserialize(&buf[ofs..]) {
                Ok((rec, consumed)) => {
                    f(&rec);
                    ofs += consumed;
                }
                Err(e) => {
                    tracing::warn!(
                        wal_offset = offset + ofs as u64,
                        error = %e,
                        "kvwal_record_error"
                    );
                    break;
                }
            }
        }
        Ok(offset + buf.len() as u64)
    }

    pub fn scan_kv_collect(&mut self) -> io::Result<Vec<KvWalRecord>> {
        let mut records = Vec::new();
        self.scan_kv(0, |rec| records.push(rec.clone()))?;
        Ok(records)
    }

    pub fn append_kv(&mut self, rec: &KvWalRecord) -> io::Result<()> {
        let data = rec.serialize();
        self.file.write_all(&data)?;
        self.file.sync_all()?;
        Ok(())
    }

    pub fn append_kv_batch(&mut self, recs: &[KvWalRecord]) -> io::Result<()> {
        for rec in recs {
            let data = rec.serialize();
            self.file.write_all(&data)?;
        }
        self.file.sync_all()?;
        Ok(())
    }
}

// ── Legacy old-format WalRecord (for migration tool only) ─────────

pub const MAGIC: u16 = 0x5255;

#[derive(Debug, Clone)]
pub struct WalRecord {
    pub revision: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub flags: u8,
    pub lease_id: Option<i64>,
}

impl WalRecord {
    pub fn serialize(&self) -> Vec<u8> {
        const HEADER_SIZE: usize = 23;
        let lease_size: usize = if self.flags & HAS_LEASE != 0 { 8 } else { 0 };
        let total = HEADER_SIZE + self.key.len() + self.value.len() + lease_size;
        let mut buf = Vec::with_capacity(total);

        buf.extend_from_slice(&MAGIC.to_le_bytes());
        buf.extend_from_slice(&self.revision.to_le_bytes());
        let crc_ofs = buf.len();
        buf.extend_from_slice(&[0u8; 4]);
        buf.extend_from_slice(&(self.key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&(self.value.len() as u32).to_le_bytes());
        buf.push(self.flags);
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.value);

        if let Some(lid) = self.lease_id {
            buf.extend_from_slice(&lid.to_le_bytes());
        }

        let crc = crc32c(&buf[22..]);
        buf[crc_ofs..crc_ofs + 4].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    pub fn deserialize(data: &[u8]) -> io::Result<(Self, usize)> {
        const HEADER_SIZE: usize = 23;
        if data.len() < HEADER_SIZE {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short record"));
        }

        let mut ofs = 0;
        let magic = u16::from_le_bytes([data[ofs], data[ofs + 1]]);
        ofs += 2;
        if magic != MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad magic"));
        }

        let revision = u64::from_le_bytes(data[ofs..ofs + 8].try_into().unwrap());
        ofs += 8;

        let _stored_crc = u32::from_le_bytes(data[ofs..ofs + 4].try_into().unwrap());
        ofs += 4;

        let key_len = u32::from_le_bytes(data[ofs..ofs + 4].try_into().unwrap()) as usize;
        ofs += 4;

        let val_len = u32::from_le_bytes(data[ofs..ofs + 4].try_into().unwrap()) as usize;
        ofs += 4;

        let flags = data[ofs];
        ofs += 1;

        let flags_ofs = ofs - 1;
        let payload_end = ofs + key_len + val_len;
        if data.len() < payload_end {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated payload",
            ));
        }

        let key = data[ofs..ofs + key_len].to_vec();
        ofs += key_len;
        let value = data[ofs..ofs + val_len].to_vec();
        ofs += val_len;

        let lease_id = if flags & HAS_LEASE != 0 {
            if data.len() < ofs + 8 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "truncated lease",
                ));
            }
            let lid = i64::from_le_bytes(data[ofs..ofs + 8].try_into().unwrap());
            ofs += 8;
            Some(lid)
        } else {
            None
        };

        let computed = crc32c(&data[flags_ofs..ofs]);
        if computed != _stored_crc {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "CRC mismatch"));
        }

        Ok((
            WalRecord {
                revision,
                key,
                value,
                flags,
                lease_id,
            },
            ofs,
        ))
    }
}

// ── CRC32C ─────────────────────────────────────────────────────────

pub fn crc32c(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFFFFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0x82F63B78;
            } else {
                crc >>= 1;
            }
        }
    }
    crc ^ 0xFFFFFFFF
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_overlong_u32_roundtrip() {
        let cases = [
            0u32,
            1,
            127,
            128,
            255,
            65535,
            1 << 20,
            (1 << 28) - 1,
            12345678,
            0xDEADBEAF,
        ];
        for &v in &cases {
            let enc = encode_overlong_u32(v);
            let dec = decode_overlong_u32(&enc).unwrap();
            assert_eq!(v, dec, "u32 roundtrip failed for {v}");
        }
    }

    #[test]
    fn test_overlong_u32_short_buffer() {
        assert!(decode_overlong_u32(&[0; 3]).is_none());
        assert!(decode_overlong_u32(&[]).is_none());
    }

    #[test]
    fn test_overlong_u32_prost_compatible() {
        let enc = encode_overlong_u32(42);
        let (dec, consumed) = decode_varint(&enc).unwrap();
        assert_eq!(dec, 42);
        assert_eq!(consumed, 5, "standard decoder consumed all 5 bytes");
    }

    #[test]
    fn test_overlong_u64_roundtrip() {
        let cases = [
            0u64,
            1,
            127,
            128,
            65535,
            1 << 32,
            (1 << 56) - 1,
            12345678901234567890,
        ];
        for &v in &cases {
            let enc = encode_overlong_u64(v);
            let dec = decode_overlong_u64(&enc).unwrap();
            assert_eq!(v, dec, "u64 roundtrip failed for {v}");
        }
    }

    #[test]
    fn test_overlong_u64_short_buffer() {
        assert!(decode_overlong_u64(&[0; 7]).is_none());
        assert!(decode_overlong_u64(&[]).is_none());
    }

    #[test]
    fn test_standard_varint_roundtrip() {
        let cases = [0u64, 1, 127, 128, 255, 65535, 1 << 32, u64::MAX];
        for &v in &cases {
            let enc = encode_varint(v);
            let (dec, consumed) = decode_varint(&enc).unwrap();
            assert_eq!(v, dec, "varint roundtrip failed for {v}");
            assert_eq!(consumed, enc.len());
        }
    }

    #[test]
    fn test_standard_varint_decode_overlong() {
        let enc = encode_overlong_u32(42);
        let (dec, consumed) = decode_varint(&enc).unwrap();
        assert_eq!(dec, 42);
        assert_eq!(consumed, 5);
    }

    #[test]
    fn test_encode_kv_roundtrip() {
        use crate::proto::mvccpb;
        use prost::Message;

        let key = b"test_key";
        let value = b"test_value";
        let (kv_bytes, key_ofs, rev_ofs) = encode_kv(key, value, 1, 100, 5, 999);

        let decoded = mvccpb::KeyValue::decode(&kv_bytes[..]).unwrap();
        assert_eq!(decoded.key, key);
        assert_eq!(decoded.value, value);
        assert_eq!(decoded.create_revision, 1);
        assert_eq!(decoded.mod_revision, 100);
        assert_eq!(decoded.version, 5);
        assert_eq!(decoded.lease, 999);

        let read_key = read_key_from_kv(&kv_bytes, key_ofs).unwrap();
        assert_eq!(read_key, key);
        let read_rev = read_mod_revision_from_kv(&kv_bytes, rev_ofs).unwrap();
        assert_eq!(read_rev, 100);
    }

    #[test]
    fn test_encode_kv_empty_values() {
        use crate::proto::mvccpb;
        use prost::Message;

        let (kv_bytes, key_ofs, rev_ofs) = encode_kv(b"k", b"", 1, 2, 3, 0);
        let decoded = mvccpb::KeyValue::decode(&kv_bytes[..]).unwrap();
        assert_eq!(decoded.key, b"k");
        assert_eq!(decoded.value, b"");
        assert_eq!(decoded.lease, 0);

        let read_key = read_key_from_kv(&kv_bytes, key_ofs).unwrap();
        assert_eq!(read_key, b"k");
        let read_rev = read_mod_revision_from_kv(&kv_bytes, rev_ofs).unwrap();
        assert_eq!(read_rev, 2);
    }

    #[test]
    fn test_encode_kv_large_values() {
        use crate::proto::mvccpb;
        use prost::Message;

        let key = vec![0xABu8; 1000];
        let value = vec![0xCDu8; 65535];
        let (kv_bytes, key_ofs, rev_ofs) = encode_kv(&key, &value, 1, 2, 3, 0);

        let decoded = mvccpb::KeyValue::decode(&kv_bytes[..]).unwrap();
        assert_eq!(decoded.key.len(), 1000);
        assert_eq!(decoded.value.len(), 65535);

        let read_key = read_key_from_kv(&kv_bytes, key_ofs).unwrap();
        assert_eq!(read_key.len(), 1000);
        let read_rev = read_mod_revision_from_kv(&kv_bytes, rev_ofs).unwrap();
        assert_eq!(read_rev, 2);
    }

    #[test]
    fn test_read_key_invalid_offsets() {
        let (kv_bytes, _, _) = encode_kv(b"key", b"val", 1, 2, 3, 0);
        assert!(read_key_from_kv(&kv_bytes, 9999).is_none());
        assert!(read_mod_revision_from_kv(&kv_bytes, 9999).is_none());
    }

    #[test]
    fn test_kvwal_record_roundtrip() {
        let rec = KvWalRecord::new(IS_CREATE, b"my_key", b"my_value", 1, 42, 5, 0);

        let serialized = rec.serialize();
        let (deserialized, consumed) = KvWalRecord::deserialize(&serialized).unwrap();
        assert_eq!(consumed, serialized.len());
        assert_eq!(deserialized.flags, IS_CREATE);
        assert_eq!(deserialized.key(), Some(&b"my_key"[..]));
        assert_eq!(deserialized.mod_revision(), Some(42));
        assert_eq!(deserialized.kv_bytes, rec.kv_bytes);
        assert_eq!(deserialized.crc, rec.crc);
    }

    #[test]
    fn test_kvwal_record_with_lease() {
        let rec = KvWalRecord::new(
            IS_CREATE | HAS_LEASE,
            b"lease_key",
            b"lease_val",
            10,
            20,
            1,
            777,
        );

        let serialized = rec.serialize();
        let (deserialized, _) = KvWalRecord::deserialize(&serialized).unwrap();
        assert_eq!(deserialized.flags, IS_CREATE | HAS_LEASE);
        assert_eq!(deserialized.key(), Some(&b"lease_key"[..]));
        assert_eq!(deserialized.mod_revision(), Some(20));
    }

    #[test]
    fn test_kvwal_record_deleted() {
        let rec = KvWalRecord::new(DELETED, b"del_key", b"", 5, 6, 1, 0);

        let serialized = rec.serialize();
        let (deserialized, _) = KvWalRecord::deserialize(&serialized).unwrap();
        assert_eq!(deserialized.flags, DELETED);
        assert_eq!(deserialized.key(), Some(&b"del_key"[..]));
        assert_eq!(deserialized.mod_revision(), Some(6));
    }

    #[test]
    fn test_kvwal_record_crc_corruption() {
        let rec = KvWalRecord::new(IS_CREATE, b"key", b"value", 1, 2, 3, 0);
        let mut serialized = rec.serialize();
        serialized[KV_HEADER_SIZE + 2] ^= 0xFF;

        match KvWalRecord::deserialize(&serialized) {
            Ok(_) => panic!("expected CRC mismatch error"),
            Err(e) => assert!(e.to_string().contains("CRC"), "unexpected error: {e}"),
        }
    }

    #[test]
    fn test_kvwal_record_empty() {
        let rec = KvWalRecord::new(IS_CREATE, b"", b"", 0, 0, 0, 0);
        let serialized = rec.serialize();
        let (deserialized, _) = KvWalRecord::deserialize(&serialized).unwrap();
        assert_eq!(deserialized.key(), Some(&b""[..]));
        assert_eq!(deserialized.mod_revision(), Some(0));
    }

    #[test]
    fn test_kvwal_record_large_key_value() {
        let key = vec![0x42u8; 5000];
        let value = vec![0x99u8; 100000];
        let rec = KvWalRecord::new(IS_CREATE, &key, &value, 1, 999999, 42, 0);

        let serialized = rec.serialize();
        let (deserialized, consumed) = KvWalRecord::deserialize(&serialized).unwrap();
        assert_eq!(consumed, serialized.len());
        assert_eq!(deserialized.key(), Some(&key[..]));
        assert_eq!(deserialized.mod_revision(), Some(999999));
    }

    fn temp_wal_path() -> std::path::PathBuf {
        let dir = std::env::temp_dir();
        let name = format!("rudurru_test_{}.wal", std::process::id());
        let path = dir.join(name);
        let _ = std::fs::remove_file(&path);
        path
    }

    #[test]
    fn test_walfile_append_scan_kv() {
        let path = temp_wal_path();
        {
            let mut wal = WalFile::open(&path).unwrap();
            let rec1 = KvWalRecord::new(IS_CREATE, b"alpha", b"a", 1, 10, 1, 0);
            let rec2 = KvWalRecord::new(IS_CREATE, b"beta", b"b", 2, 20, 2, 0);
            let rec3 = KvWalRecord::new(DELETED, b"gamma", b"", 3, 30, 1, 0);
            wal.append_kv(&rec1).unwrap();
            wal.append_kv(&rec2).unwrap();
            wal.append_kv(&rec3).unwrap();
        }
        {
            let mut wal = WalFile::open(&path).unwrap();
            let records = wal.scan_kv_collect().unwrap();
            assert_eq!(records.len(), 3);
            assert_eq!(records[0].key(), Some(&b"alpha"[..]));
            assert_eq!(records[0].mod_revision(), Some(10));
            assert_eq!(records[1].key(), Some(&b"beta"[..]));
            assert_eq!(records[1].mod_revision(), Some(20));
            assert_eq!(records[2].flags, DELETED);
            assert_eq!(records[2].key(), Some(&b"gamma"[..]));
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_walfile_append_kv_batch() {
        let path = temp_wal_path();
        {
            let mut wal = WalFile::open(&path).unwrap();
            let recs = vec![
                KvWalRecord::new(IS_CREATE, b"a", b"1", 1, 1, 1, 0),
                KvWalRecord::new(IS_CREATE, b"b", b"2", 2, 2, 2, 0),
            ];
            wal.append_kv_batch(&recs).unwrap();
        }
        {
            let mut wal = WalFile::open(&path).unwrap();
            let records = wal.scan_kv_collect().unwrap();
            assert_eq!(records.len(), 2);
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_walfile_scan_kv_empty() {
        let path = temp_wal_path();
        {
            let mut wal = WalFile::open(&path).unwrap();
            let records = wal.scan_kv_collect().unwrap();
            assert!(records.is_empty());
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_walfile_scan_kv_partial_corruption() {
        let path = temp_wal_path();
        {
            let mut wal = WalFile::open(&path).unwrap();
            let rec1 = KvWalRecord::new(IS_CREATE, b"good", b"data", 1, 1, 1, 0);
            wal.append_kv(&rec1).unwrap();
            use std::io::Write;
            wal.file
                .write_all(b"GARBAGE_DATA_THAT_IS_NOT_A_VALID_RECORD")
                .unwrap();
            wal.file.sync_all().unwrap();
            let rec2 = KvWalRecord::new(IS_CREATE, b"after", b"garbage", 2, 2, 2, 0);
            wal.append_kv(&rec2).unwrap();
        }
        {
            let mut wal = WalFile::open(&path).unwrap();
            let records = wal.scan_kv_collect().unwrap();
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].key(), Some(&b"good"[..]));
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_walfile_scan_kv_skip_offset() {
        let path = temp_wal_path();
        {
            let mut wal = WalFile::open(&path).unwrap();
            let rec1 = KvWalRecord::new(IS_CREATE, b"first", b"1", 1, 1, 1, 0);
            let rec2 = KvWalRecord::new(IS_CREATE, b"second", b"2", 2, 2, 2, 0);
            wal.append_kv(&rec1).unwrap();
            wal.append_kv(&rec2).unwrap();
        }
        {
            let mut wal = WalFile::open(&path).unwrap();
            let all = wal.scan_kv_collect().unwrap();
            let rec1_size = all[0].rec_len;
            assert_eq!(all.len(), 2);
            drop(all);

            let mut wal2 = WalFile::open(&path).unwrap();
            let mut records = Vec::new();
            wal2.scan_kv(rec1_size as u64, |r| records.push(r.clone()))
                .unwrap();
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].key(), Some(&b"second"[..]));
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_kvwal_prost_compat_all_fields() {
        use crate::proto::mvccpb;
        use prost::Message;

        let rec = KvWalRecord::new(
            IS_CREATE | HAS_LEASE,
            b"compat_key",
            b"compat_value",
            42,
            100,
            7,
            888,
        );

        let decoded = mvccpb::KeyValue::decode(&rec.kv_bytes[..]).unwrap();
        assert_eq!(decoded.key, b"compat_key");
        assert_eq!(decoded.value, b"compat_value");
        assert_eq!(decoded.create_revision, 42);
        assert_eq!(decoded.mod_revision, 100);
        assert_eq!(decoded.version, 7);
        assert_eq!(decoded.lease, 888);
    }

    #[test]
    fn test_kvwal_prost_compat_max_values() {
        use crate::proto::mvccpb;
        use prost::Message;

        let rec = KvWalRecord::new(
            IS_CREATE,
            b"k",
            b"v",
            i64::MAX,
            i64::MAX,
            i64::MAX,
            i64::MAX,
        );

        let decoded = mvccpb::KeyValue::decode(&rec.kv_bytes[..]).unwrap();
        assert_eq!(decoded.create_revision, i64::MAX);
        assert_eq!(decoded.mod_revision, i64::MAX);
        assert_eq!(decoded.version, i64::MAX);
        assert_eq!(decoded.lease, i64::MAX);
    }

    #[test]
    fn test_kvwal_crc_consistency() {
        let rec = KvWalRecord::new(IS_CREATE, b"crc_test", b"crc_value", 1, 2, 3, 0);
        let serialized = rec.serialize();

        let kv_end = KV_HEADER_SIZE + rec.kv_bytes.len();
        let manual_crc = crc32c(&serialized[..kv_end]);
        assert_eq!(manual_crc, rec.crc, "stored CRC doesn't match manual CRC");

        let (deserialized, _) = KvWalRecord::deserialize(&serialized).unwrap();
        assert_eq!(deserialized.crc, rec.crc);
    }

    #[test]
    fn test_deserialize_rec_len_underflow() {
        let rec = KvWalRecord {
            flags: DELETED,
            kv_bytes: vec![0x0a, 0x01, 0x61],
            key_offset: 1,
            mod_rev_offset: 0,
            rec_len: KV_HEADER_SIZE as u32 + KV_CRC_SIZE as u32 - 1,
            crc: 0,
        };
        let serialized = rec.serialize();
        let result = KvWalRecord::deserialize(&serialized);
        assert!(result.is_err(), "should reject rec_len < min");
        let err = result.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
