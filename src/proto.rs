#![allow(dead_code)]
#![allow(clippy::enum_variant_names)]
#![allow(clippy::derive_partial_eq_without_eq)]

pub mod authpb {
    tonic::include_proto!("authpb");
}

pub mod mvccpb {
    tonic::include_proto!("mvccpb");
}

pub mod etcdserverpb {
    tonic::include_proto!("etcdserverpb");
}
