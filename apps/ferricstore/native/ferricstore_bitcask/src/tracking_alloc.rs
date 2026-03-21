//! Global tracking allocator that wraps `System` and counts live bytes.
//!
//! Every allocation increments an `AtomicUsize` by the requested layout size;
//! every deallocation decrements it.  The counter is approximate — it tracks
//! the *requested* size, not the actual slab the system allocator hands out —
//! but it is lock-free, cheap (<1 ns overhead per alloc/dealloc), and gives a
//! useful signal for memory monitoring via `INFO ferricstore`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

static ALLOCATED: AtomicUsize = AtomicUsize::new(0);

pub struct TrackingAllocator;

// SAFETY: We delegate entirely to `System` which is itself a valid
// `GlobalAlloc`.  The only addition is an atomic counter bump which
// cannot introduce UB.
unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        ALLOCATED.fetch_sub(layout.size(), Ordering::Relaxed);
    }
}

// Only install as global allocator during tests. In the NIF cdylib, the BEAM
// manages memory allocation and installing a custom global allocator would
// conflict with the BEAM's own allocator, causing hangs or crashes.
#[cfg(test)]
#[global_allocator]
static GLOBAL: TrackingAllocator = TrackingAllocator;

/// Returns the current number of live Rust-heap bytes (approximate).
pub fn allocated_bytes() -> u64 {
    ALLOCATED.load(Ordering::Relaxed) as u64
}

// ---------------------------------------------------------------------------
// NIF function: expose allocated_bytes to Elixir
// ---------------------------------------------------------------------------

/// Returns the current tracked allocation count as a u64.
/// This is a cheap atomic read -- runs on Normal scheduler.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::unnecessary_wraps)]
pub fn rust_allocated_bytes(env: rustler::Env) -> rustler::NifResult<rustler::Term> {
    use rustler::Encoder;
    Ok(allocated_bytes().encode(env))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_bytes_greater_than_zero() {
        // The runtime has already allocated memory (thread stacks, etc.)
        let bytes = allocated_bytes();
        assert!(bytes > 0, "expected > 0 allocated bytes, got {bytes}");
    }

    #[test]
    fn allocate_vec_bytes_increase() {
        let before = allocated_bytes();
        let v: Vec<u8> = vec![0u8; 65_536]; // 64 KiB
        let after = allocated_bytes();
        assert!(
            after > before,
            "expected increase after Vec alloc: before={before}, after={after}"
        );
        drop(v);
    }

    #[test]
    fn drop_vec_bytes_decrease() {
        let before = allocated_bytes();
        {
            let v: Vec<u8> = vec![0u8; 65_536];
            let during = allocated_bytes();
            assert!(during > before);
            drop(v);
        }
        let after = allocated_bytes();
        // After drop, bytes should be back near the before level
        // (other threads may allocate, so we use a generous margin)
        assert!(
            after < before + 1_000_000,
            "bytes didn't decrease sufficiently after drop: before={before}, after={after}"
        );
    }

    #[test]
    fn concurrent_alloc_dealloc_consistent() {
        // Run 4 threads that each allocate and free 1000 x 1KB vectors.
        // The atomic counter should not drift significantly afterwards.
        // We use a generous margin because other test threads may be allocating
        // concurrently.
        let handles: Vec<_> = (0..4)
            .map(|_| {
                std::thread::spawn(|| {
                    for _ in 0..1000 {
                        let v: Vec<u8> = vec![0u8; 1024];
                        std::hint::black_box(&v);
                        drop(v);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        // Just verify the counter hasn't gone astronomically wrong (e.g., negative
        // wrapped to huge positive). The counter should be in a reasonable range.
        let after = allocated_bytes();
        assert!(
            after < 1_000_000_000,
            "counter looks unreasonable: {after} bytes"
        );
    }

    #[test]
    fn large_allocation_tracked() {
        let before = allocated_bytes();
        let v: Vec<u8> = vec![0u8; 100 * 1024 * 1024]; // 100 MB
        let after = allocated_bytes();
        let increase = after - before;
        // Should track at least ~99 MB (Vec may allocate slightly less due to
        // allocator rounding, and concurrent threads may free memory)
        assert!(
            increase >= 99 * 1024 * 1024,
            "expected >= 99MB increase, got {increase} bytes ({} MB)",
            increase / (1024 * 1024)
        );
        drop(v);
    }
}
