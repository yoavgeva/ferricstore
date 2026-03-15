#![deny(clippy::all, clippy::pedantic)]
#![deny(unsafe_code)]

pub mod compaction;
pub mod hint;
pub mod io_backend;
pub mod keydir;
pub mod log;
pub mod store;

use rustler::{Binary, Encoder, Env, NifResult, OwnedBinary, ResourceArc, Term};
use std::sync::Mutex;

use store::Store;

mod atoms {
    rustler::atoms! {
        ok,
        error,
        nil,
        true_ = "true",
        false_ = "false",
    }
}

/// Wraps `Store` in a `Mutex` so it satisfies `Send + Sync` for `ResourceArc`.
///
/// ## Ownership model
///
/// Each Elixir shard `GenServer` owns **one** `StoreResource` for its keyspace
/// partition. There is no sharing across shards — the Mutex is uncontested in
/// the steady state. Concurrent access to the same shard resource would indicate
/// a bug in the Elixir routing layer.
///
/// The Mutex exists only to satisfy Rust's `Send + Sync` bounds required by
/// `ResourceArc`; in practice it is always acquired by exactly one BEAM process
/// (the shard `GenServer`) at a time.
struct StoreResource(Mutex<Store>);

#[allow(non_local_definitions)]
fn load(env: Env, _info: Term) -> bool {
    let _ = rustler::resource!(StoreResource, env);
    true
}

/// Open (or create) a Bitcask store at the given path.
/// Returns `{:ok, resource}` or `{:error, reason}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn new(env: Env, path: String) -> NifResult<Term> {
    match Store::open(std::path::Path::new(&path)) {
        Ok(store) => {
            let resource = ResourceArc::new(StoreResource(Mutex::new(store)));
            Ok((atoms::ok(), resource).encode(env))
        }
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Get a value by key. Returns `{:ok, value}`, `{:ok, :nil}`, or `{:error, reason}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn get<'a>(env: Env<'a>, resource: ResourceArc<StoreResource>, key: Binary<'a>) -> NifResult<Term<'a>> {
    let mut store = resource.0.lock().map_err(|_| rustler::Error::BadArg)?;
    match store.get(key.as_slice()) {
        Ok(Some(value)) => {
            let mut bin = OwnedBinary::new(value.len()).ok_or(rustler::Error::BadArg)?;
            bin.as_mut_slice().copy_from_slice(&value);
            Ok((atoms::ok(), Binary::from_owned(bin, env)).encode(env))
        }
        Ok(None) => Ok((atoms::ok(), atoms::nil()).encode(env)),
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Put a key-value pair. `expire_at_ms` = 0 means no expiry.
/// Returns `:ok` or `{:error, reason}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn put<'a>(
    env: Env<'a>,
    resource: ResourceArc<StoreResource>,
    key: Binary<'a>,
    value: Binary<'a>,
    expire_at_ms: u64,
) -> NifResult<Term<'a>> {
    let mut store = resource.0.lock().map_err(|_| rustler::Error::BadArg)?;
    match store.put(key.as_slice(), value.as_slice(), expire_at_ms) {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Write multiple key-value pairs in a single fsync (group commit).
///
/// `batch` is a list of `{key, value, expire_at_ms}` tuples.
/// Returns `:ok` or `{:error, reason}`.
///
/// ## BEAM scheduler contract
///
/// This NIF occupies **one** dirty scheduler thread for the duration of the
/// entire batch (all writes + one `fsync`). The Elixir shard `GenServer` should
/// collect writes during a batch window, then call `put_batch` once per window.
/// This keeps dirty scheduler occupancy at O(1) per batch regardless of how
/// many writes the batch contains.
///
/// Contrast with calling `put` N times: each call occupies a dirty thread for
/// its own `fsync`, so N concurrent puts compete for the fixed-size dirty pool
/// (default 10 threads) and cause queuing under load.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn put_batch<'a>(
    env: Env<'a>,
    resource: ResourceArc<StoreResource>,
    batch: Vec<(Binary<'a>, Binary<'a>, u64)>,
) -> NifResult<Term<'a>> {
    let mut store = resource.0.lock().map_err(|_| rustler::Error::BadArg)?;
    let entries: Vec<(&[u8], &[u8], u64)> = batch
        .iter()
        .map(|(k, v, exp)| (k.as_slice(), v.as_slice(), *exp))
        .collect();
    match store.put_batch(&entries) {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Delete a key. Returns `{:ok, true}`, `{:ok, false}` (not found), or `{:error, reason}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn delete<'a>(env: Env<'a>, resource: ResourceArc<StoreResource>, key: Binary<'a>) -> NifResult<Term<'a>> {
    let mut store = resource.0.lock().map_err(|_| rustler::Error::BadArg)?;
    match store.delete(key.as_slice()) {
        Ok(true) => Ok((atoms::ok(), atoms::true_()).encode(env)),
        Ok(false) => Ok((atoms::ok(), atoms::false_()).encode(env)),
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Return all live keys as a list of binaries.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn keys(env: Env, resource: ResourceArc<StoreResource>) -> NifResult<Term> {
    let store = resource.0.lock().map_err(|_| rustler::Error::BadArg)?;
    let all_keys: Vec<Term> = store
        .keys()
        .into_iter()
        .map(|k| {
            let mut bin = OwnedBinary::new(k.len()).unwrap();
            bin.as_mut_slice().copy_from_slice(&k);
            Binary::from_owned(bin, env).encode(env)
        })
        .collect();
    Ok(all_keys.encode(env))
}

/// Write a hint file for the active data file.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn write_hint(env: Env, resource: ResourceArc<StoreResource>) -> NifResult<Term> {
    let store = resource.0.lock().map_err(|_| rustler::Error::BadArg)?;
    match store.write_hint_file() {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

rustler::init!("Elixir.Ferricstore.Bitcask.NIF", load = load);
