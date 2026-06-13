use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

pub const MAGIC: u16 = 0x5255; // "RU"
pub const DELETED: u8 = 0x01;
pub const IS_CREATE: u8 = 0x02;
pub const HAS_LEASE: u8 = 0x04;

const HEADER_SIZE: usize = 23; // magic(2) + revision(8) + crc32(4) + key_len(4) + val_len(4) + flags(1)

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
        let lease_size: usize = if self.flags & HAS_LEASE != 0 { 8 } else { 0 };
        let total = HEADER_SIZE + self.key.len() + self.value.len() + lease_size;
        let mut buf = Vec::with_capacity(total);

        buf.extend_from_slice(&MAGIC.to_le_bytes());       // 2
        buf.extend_from_slice(&self.revision.to_le_bytes()); // 8
        // CRC placeholder
        let crc_ofs = buf.len();
        buf.extend_from_slice(&[0u8; 4]);                   // 4
        buf.extend_from_slice(&(self.key.len() as u32).to_le_bytes()); // 4
        buf.extend_from_slice(&(self.value.len() as u32).to_le_bytes()); // 4
        buf.push(self.flags);                                // 1
        buf.extend_from_slice(&self.key);                    // N
        buf.extend_from_slice(&self.value);                  // M

        if let Some(lid) = self.lease_id {
            buf.extend_from_slice(&lid.to_le_bytes());       // 8
        }

        // Compute CRC32C of flags + key + value
        let crc = crc32c(&buf[17..]); // from flags byte onward
        buf[crc_ofs..crc_ofs + 4].copy_from_slice(&crc.to_le_bytes());

        buf
    }

    pub fn deserialize(data: &[u8]) -> io::Result<(Self, usize)> {
        if data.len() < HEADER_SIZE {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short record"));
        }

        let mut ofs = 0;
        let magic = u16::from_le_bytes([data[ofs], data[ofs + 1]]);
        ofs += 2;
        if magic != MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad magic"));
        }

        let revision = u64::from_le_bytes(
            data[ofs..ofs + 8].try_into().unwrap(),
        );
        ofs += 8;

        let _stored_crc = u32::from_le_bytes(
            data[ofs..ofs + 4].try_into().unwrap(),
        );
        ofs += 4;

        let key_len = u32::from_le_bytes(
            data[ofs..ofs + 4].try_into().unwrap(),
        ) as usize;
        ofs += 4;

        let val_len = u32::from_le_bytes(
            data[ofs..ofs + 4].try_into().unwrap(),
        ) as usize;
        ofs += 4;

        let flags = data[ofs];
        ofs += 1;

        let payload_start = ofs;
        let payload_end = ofs + key_len + val_len;
        if data.len() < payload_end {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated payload"));
        }

        let key = data[ofs..ofs + key_len].to_vec();
        ofs += key_len;
        let value = data[ofs..ofs + val_len].to_vec();
        ofs += val_len;

        let lease_id = if flags & HAS_LEASE != 0 {
            if data.len() < ofs + 8 {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated lease"));
            }
            let lid = i64::from_le_bytes(data[ofs..ofs + 8].try_into().unwrap());
            ofs += 8;
            Some(lid)
        } else {
            None
        };

        // Verify CRC32C
        let computed = crc32c(&data[payload_start..ofs]);
        if computed != _stored_crc {
            // Log and skip corrupted record
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

/// Append-only WAL file.
#[derive(Debug)]
pub struct WalFile {
    file: std::fs::File,
    path: String,
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

    /// Scan all records from the WAL file.
    pub fn scan(&mut self) -> io::Result<Vec<WalRecord>> {
        self.file.seek(SeekFrom::Start(0))?;

        let mut records = Vec::new();
        let mut buf = Vec::new();
        self.file.read_to_end(&mut buf)?;

        let mut ofs = 0;
        while ofs < buf.len() {
            match WalRecord::deserialize(&buf[ofs..]) {
                Ok((rec, consumed)) => {
                    records.push(rec);
                    ofs += consumed;
                }
                Err(_) => {
                    // Corrupted or truncated record — stop scanning
                    break;
                }
            }
        }

        Ok(records)
    }

    /// Append a single record to the WAL.
    pub fn append(&mut self, rec: &WalRecord) -> io::Result<()> {
        let data = rec.serialize();
        self.file.write_all(&data)?;
        self.file.sync_all()?;
        Ok(())
    }

    /// Append multiple records in a single writev-style call.
    pub fn append_batch(&mut self, recs: &[WalRecord]) -> io::Result<()> {
        for rec in recs {
            let data = rec.serialize();
            self.file.write_all(&data)?;
        }
        self.file.sync_all()?;
        Ok(())
    }
}

fn crc32c(data: &[u8]) -> u32 {
    // Use a simple CRC32 (not CRC32C). Good enough for integrity checking.
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
