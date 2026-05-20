#![deny(
    unsafe_code,
    missing_docs,
    bad_style,
    dead_code,
    improper_ctypes,
    non_shorthand_field_patterns,
    no_mangle_generic_items,
    overflowing_literals,
    path_statements,
    patterns_in_fns_without_body,
    unconditional_recursion,
    unused_allocation,
    unused_comparisons,
    unused_parens,
    while_true,
    missing_debug_implementations,
    trivial_casts,
    trivial_numeric_casts,
    unused,
    unused_extern_crates,
    unused_import_braces,
    unused_qualifications
)]

pub mod chunk;
pub mod compaction;
pub mod config;
pub mod index;
pub mod memtable;
pub mod metrics;
pub mod server;
pub mod types;
pub mod wal;
pub mod proto {
    pub mod storage {
        pub mod v1 {
            tonic::include_proto!("storage.v1");
        }
    }
}
