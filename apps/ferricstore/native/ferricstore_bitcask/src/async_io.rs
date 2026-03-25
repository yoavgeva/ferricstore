//! Tokio runtime for async IO NIFs.
//!
//! Provides a global Tokio multi-threaded runtime that is initialised once
//! (on first access) and lives for the entire BEAM VM lifetime. Async NIF
//! functions spawn tasks onto this runtime to perform blocking disk IO
//! without occupying BEAM scheduler threads.
//!
//! ## Design rationale
//!
//! When a cold GET happens (hot_cache miss), the BEAM Normal scheduler
//! thread would block during `pread()` for ~50-200us. With many concurrent
//! cold reads, all BEAM schedulers can be blocked simultaneously, stalling
//! ALL Elixir processes. By submitting IO work to Tokio worker threads,
//! the BEAM scheduler returns immediately and is free to run other processes.
//!
//! ## Safety
//!
//! The `OnceLock` ensures the runtime is created exactly once. The runtime
//! is never shut down — it lives until the BEAM process exits.

use std::sync::OnceLock;
use tokio::runtime::Runtime;

static TOKIO_RT: OnceLock<Runtime> = OnceLock::new();

/// Returns a reference to the global Tokio runtime, creating it on first call.
///
/// H-8 fix: limits worker threads to `min(4, num_cpus)` instead of the default
/// (one per CPU core). The Tokio runtime is only used for disk IO operations
/// (pread, fsync), which are limited by NVMe parallelism (4-8 outstanding IOs
/// is optimal). On a 64-core server, the default would create 64 Tokio threads
/// in addition to the BEAM's ~64 scheduler threads — 128 threads competing
/// for CPU. With 4 workers the IO throughput is unchanged but context switching
/// overhead drops significantly.
///
/// # Panics
///
/// Panics if the Tokio runtime cannot be created (e.g. OS thread limit
/// reached). This is a fatal error since the NIF library cannot function
/// without the runtime.
pub fn runtime() -> &'static Runtime {
    TOKIO_RT.get_or_init(|| {
        let num_cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let workers = num_cpus.min(4).max(1);
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(workers)
            .thread_name("ferric-tokio")
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_creates_successfully() {
        let rt = runtime();
        // Verify we can spawn a task and get a result
        let handle = rt.spawn(async { 42 });
        let result = rt.block_on(handle).unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn runtime_is_singleton() {
        let rt1 = runtime() as *const Runtime;
        let rt2 = runtime() as *const Runtime;
        assert_eq!(rt1, rt2, "runtime() must return the same instance");
    }

    #[test]
    fn multiple_concurrent_spawns() {
        let rt = runtime();
        let mut handles = Vec::new();
        for i in 0..100 {
            handles.push(rt.spawn(async move { i * 2 }));
        }
        for (i, handle) in handles.into_iter().enumerate() {
            let result = rt.block_on(handle).unwrap();
            assert_eq!(result, i * 2);
        }
    }

    // -----------------------------------------------------------------------
    // Edge-case tests
    // -----------------------------------------------------------------------

    #[test]
    fn runtime_survives_panic_in_spawned_task() {
        let rt = runtime();
        let panic_handle = rt.spawn(async {
            panic!("intentional panic in async task");
        });
        // The panic should be caught by tokio; the JoinHandle returns Err
        let result = rt.block_on(panic_handle);
        assert!(result.is_err(), "panicking task should return JoinError");

        // Runtime should still be functional
        let ok_handle = rt.spawn(async { 42 });
        let ok_result = rt.block_on(ok_handle).unwrap();
        assert_eq!(ok_result, 42);
    }

    #[test]
    fn multiple_concurrent_spawns_no_deadlock() {
        let rt = runtime();
        // Spawn tasks from multiple threads concurrently
        let handles: Vec<_> = (0..4)
            .map(|t| {
                std::thread::spawn(move || {
                    let rt = runtime();
                    let mut tasks = Vec::new();
                    for i in 0..25 {
                        tasks.push(rt.spawn(async move { t * 100 + i }));
                    }
                    let mut results = Vec::new();
                    for task in tasks {
                        results.push(rt.block_on(task).unwrap());
                    }
                    results
                })
            })
            .collect();

        for h in handles {
            let results = h.join().unwrap();
            assert_eq!(results.len(), 25);
        }
    }

    #[test]
    fn spawn_after_runtime_creation() {
        // First call creates the runtime
        let _rt1 = runtime();
        // Second call reuses it and spawns more
        let rt2 = runtime();
        let h = rt2.spawn(async { 99 });
        let result = rt2.block_on(h).unwrap();
        assert_eq!(result, 99);
    }

    #[test]
    fn heavy_load_10k_concurrent_spawns() {
        let rt = runtime();
        let mut handles = Vec::with_capacity(10_000);
        for i in 0..10_000u64 {
            handles.push(rt.spawn(async move { i }));
        }
        let mut sum = 0u64;
        for h in handles {
            sum += rt.block_on(h).unwrap();
        }
        // Sum of 0..9999 = 9999*10000/2 = 49_995_000
        assert_eq!(sum, 49_995_000);
    }

    // ==================================================================
    // Deep NIF edge cases — targeting async runtime / FFI pitfalls
    // ==================================================================

    #[test]
    fn panic_in_tokio_task_does_not_crash_runtime() {
        let rt = runtime();
        // Spawn multiple panicking tasks
        let mut panic_handles = Vec::new();
        for i in 0..10 {
            panic_handles.push(rt.spawn(async move {
                if i % 2 == 0 {
                    panic!("deliberate panic in task {i}");
                }
                i
            }));
        }

        // All panicking tasks should return JoinError
        for h in panic_handles {
            let _ = rt.block_on(h); // Ok or Err, but no crash
        }

        // Runtime must still be functional
        let ok_handle = rt.spawn(async { 999 });
        assert_eq!(rt.block_on(ok_handle).unwrap(), 999);
    }

    #[test]
    fn task_with_large_closure_1mb() {
        let rt = runtime();
        let large_data = vec![0xABu8; 1_024 * 1_024]; // 1 MB
        let handle = rt.spawn(async move { large_data.len() });
        let result = rt.block_on(handle).unwrap();
        assert_eq!(result, 1_024 * 1_024);
    }

    #[test]
    fn spawn_from_multiple_os_threads_concurrently() {
        let handles: Vec<_> = (0..20)
            .map(|t| {
                std::thread::spawn(move || {
                    let rt = runtime();
                    let mut tasks = Vec::new();
                    for i in 0..50 {
                        tasks.push(rt.spawn(async move { t * 1000 + i }));
                    }
                    let mut results = Vec::new();
                    for task in tasks {
                        results.push(rt.block_on(task).unwrap());
                    }
                    results
                })
            })
            .collect();

        let mut total = 0;
        for h in handles {
            let results = h.join().unwrap();
            assert_eq!(results.len(), 50);
            total += results.len();
        }
        assert_eq!(total, 1000);
    }

    #[test]
    fn nested_spawn_works() {
        let rt = runtime();
        let outer = rt.spawn(async {
            let inner = tokio::spawn(async { 42 });
            inner.await.unwrap()
        });
        let result = rt.block_on(outer).unwrap();
        assert_eq!(result, 42);
    }

    // ------------------------------------------------------------------
    // H-8: Tokio runtime limits worker threads to min(4, num_cpus)
    // ------------------------------------------------------------------

    #[test]
    fn h8_runtime_functional_with_limited_threads() {
        let rt = runtime();
        // Verify that spawning more tasks than worker threads still works
        // (tasks are multiplexed onto the worker pool).
        let mut handles = Vec::with_capacity(100);
        for i in 0..100u64 {
            handles.push(rt.spawn(async move { i * 2 }));
        }
        let mut sum = 0u64;
        for h in handles {
            sum += rt.block_on(h).unwrap();
        }
        assert_eq!(sum, 9900); // sum of 0*2 + 1*2 + ... + 99*2 = 2 * 4950 = 9900
    }

    #[test]
    fn h8_concurrent_io_tasks_complete() {
        let rt = runtime();
        // Spawn many tasks that do a small amount of compute work.
        let mut handles = Vec::new();
        for i in 0u64..20 {
            handles.push(rt.spawn(async move {
                // Simulate a small amount of work
                let mut sum = 0u64;
                for j in 0..100 {
                    sum += i + j;
                }
                sum
            }));
        }
        for h in handles {
            let result = rt.block_on(h).unwrap();
            assert!(result > 0);
        }
    }
}
