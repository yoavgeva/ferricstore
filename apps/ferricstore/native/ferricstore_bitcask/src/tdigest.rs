//! TDigest is implemented in pure Elixir (Ferricstore.TDigest.Core).
//! No Rust NIFs needed — data is stored in Bitcask via Raft.

use rustler::Env;

/// Stub — no resources to register (pure Elixir implementation).
pub fn register_resource(_env: Env) {}

/// Stub — no mmap resources.
pub fn register_mmap_resource(_env: Env) {}
