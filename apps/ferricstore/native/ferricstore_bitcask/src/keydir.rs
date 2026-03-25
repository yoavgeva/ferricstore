use std::collections::HashMap;

/// Metadata stored in RAM for every key. Points into a data file on disk.
///
/// H-5 fix: reordered fields to minimize padding. The largest-aligned fields
/// (u64) come first, followed by u32, then bool. This packs into 32 bytes
/// instead of 40 bytes, saving 8 bytes per key (80 MB for 10M keys).
#[derive(Debug, Clone, PartialEq, Eq)]
#[repr(C)]
pub struct KeyEntry {
    /// Which data file holds the value (monotonically increasing ID).
    pub file_id: u64,
    /// Byte offset of the record start inside that file.
    pub offset: u64,
    /// Absolute Unix timestamp in milliseconds; 0 = no expiry.
    pub expire_at_ms: u64,
    /// Size of the full on-disk record in bytes (used to seek past it).
    pub value_size: u32,
    /// Reference bit for clock-hand eviction sweep.
    pub ref_bit: bool,
    // 3 bytes padding to align to 8-byte boundary -> total 32 bytes
}

/// In-memory index: key -> location on disk.
///
/// M-8 fix: uses `Box<[u8]>` instead of `Vec<u8>` for keys. `Box<[u8]>` is
/// 16 bytes (ptr + len) vs `Vec<u8>`'s 24 bytes (ptr + len + cap). For 1M
/// keys this saves ~8 MB of RAM and reduces allocator pressure.
///
/// `Box<[u8]>` implements `Borrow<[u8]>`, so `HashMap::get(&[u8])` lookups
/// work without allocating.
pub struct KeyDir {
    map: HashMap<Box<[u8]>, KeyEntry>,
}

impl KeyDir {
    #[must_use]
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    /// Insert or overwrite an entry. Converts the `Vec<u8>` key to `Box<[u8]>`
    /// to save 8 bytes per entry (no capacity field).
    pub fn put(&mut self, key: Vec<u8>, entry: KeyEntry) {
        self.map.insert(key.into_boxed_slice(), entry);
    }

    /// Look up a key. Returns `None` if not found or already logically deleted.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<&KeyEntry> {
        self.map.get(key)
    }

    /// Remove a key from the index (logical delete; the value stays on disk until compaction).
    pub fn delete(&mut self, key: &[u8]) -> bool {
        self.map.remove(key).is_some()
    }

    /// Number of live keys.
    #[must_use]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Iterator over all (key, entry) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], &KeyEntry)> {
        self.map.iter().map(|(k, v)| (k.as_ref(), v))
    }

    /// Set the reference bit for a key (called on cache promotion / access).
    pub fn set_ref_bit(&mut self, key: &[u8]) {
        if let Some(entry) = self.map.get_mut(key) {
            entry.ref_bit = true;
        }
    }

    /// Clear the reference bit and return whether the key is now eviction-eligible.
    /// Returns `None` if key not found.
    pub fn tick_ref_bit(&mut self, key: &[u8]) -> Option<bool> {
        let entry = self.map.get_mut(key)?;
        if entry.ref_bit {
            entry.ref_bit = false;
            Some(false) // given a second chance, not eligible yet
        } else {
            Some(true) // bit was already clear — eligible for eviction
        }
    }

    /// Return keys whose `expire_at_ms` is non-zero and <= `now_ms`.
    #[must_use]
    pub fn expired_keys(&self, now_ms: u64) -> Vec<Vec<u8>> {
        self.map
            .iter()
            .filter(|(_, e)| e.expire_at_ms != 0 && e.expire_at_ms <= now_ms)
            .map(|(k, _)| k.to_vec())
            .collect()
    }
}

impl Default for KeyDir {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(file_id: u64, offset: u64) -> KeyEntry {
        KeyEntry {
            file_id,
            offset,
            value_size: 10,
            expire_at_ms: 0,
            ref_bit: false,
        }
    }

    fn expiring_entry(file_id: u64, expire_at_ms: u64) -> KeyEntry {
        KeyEntry {
            file_id,
            offset: 0,
            value_size: 10,
            expire_at_ms,
            ref_bit: false,
        }
    }

    #[test]
    fn starts_empty() {
        let kd = KeyDir::new();
        assert!(kd.is_empty());
        assert_eq!(kd.len(), 0);
    }

    #[test]
    fn put_and_get() {
        let mut kd = KeyDir::new();
        kd.put(b"hello".to_vec(), entry(1, 0));
        let e = kd.get(b"hello").unwrap();
        assert_eq!(e.file_id, 1);
        assert_eq!(e.offset, 0);
    }

    #[test]
    fn get_missing_key_returns_none() {
        let kd = KeyDir::new();
        assert!(kd.get(b"nope").is_none());
    }

    #[test]
    fn put_overwrites_existing() {
        let mut kd = KeyDir::new();
        kd.put(b"k".to_vec(), entry(1, 0));
        kd.put(b"k".to_vec(), entry(2, 100));
        let e = kd.get(b"k").unwrap();
        assert_eq!(e.file_id, 2);
        assert_eq!(e.offset, 100);
    }

    #[test]
    fn delete_removes_key() {
        let mut kd = KeyDir::new();
        kd.put(b"k".to_vec(), entry(1, 0));
        assert!(kd.delete(b"k"));
        assert!(kd.get(b"k").is_none());
        assert_eq!(kd.len(), 0);
    }

    #[test]
    fn delete_missing_key_returns_false() {
        let mut kd = KeyDir::new();
        assert!(!kd.delete(b"ghost"));
    }

    #[test]
    fn len_tracks_insertions_and_deletions() {
        let mut kd = KeyDir::new();
        kd.put(b"a".to_vec(), entry(1, 0));
        kd.put(b"b".to_vec(), entry(1, 10));
        assert_eq!(kd.len(), 2);
        kd.delete(b"a");
        assert_eq!(kd.len(), 1);
    }

    #[test]
    fn ref_bit_set_and_tick() {
        let mut kd = KeyDir::new();
        kd.put(b"k".to_vec(), entry(1, 0));

        // bit starts clear — should be eligible on first tick
        assert_eq!(kd.tick_ref_bit(b"k"), Some(true));

        // set the bit, then tick — not eligible (gets second chance, bit cleared)
        kd.set_ref_bit(b"k");
        assert_eq!(kd.tick_ref_bit(b"k"), Some(false));

        // now bit is clear again — eligible on next tick
        assert_eq!(kd.tick_ref_bit(b"k"), Some(true));
    }

    #[test]
    fn tick_ref_bit_missing_key_returns_none() {
        let mut kd = KeyDir::new();
        assert_eq!(kd.tick_ref_bit(b"ghost"), None);
    }

    #[test]
    fn expired_keys_empty_when_no_expiry() {
        let mut kd = KeyDir::new();
        kd.put(b"k".to_vec(), entry(1, 0)); // expire_at_ms = 0 means never
        assert!(kd.expired_keys(9_999_999).is_empty());
    }

    #[test]
    fn expired_keys_returns_past_due() {
        let mut kd = KeyDir::new();
        kd.put(b"old".to_vec(), expiring_entry(1, 1000));
        kd.put(b"fresh".to_vec(), expiring_entry(1, 9000));
        kd.put(b"permanent".to_vec(), entry(1, 0));

        let expired = kd.expired_keys(5000);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0], b"old");
    }

    #[test]
    fn expired_keys_at_exact_boundary() {
        let mut kd = KeyDir::new();
        kd.put(b"edge".to_vec(), expiring_entry(1, 5000));
        // expire_at_ms <= now_ms, so 5000 <= 5000 is expired
        assert_eq!(kd.expired_keys(5000).len(), 1);
        // one ms before: not expired
        assert!(kd.expired_keys(4999).is_empty());
    }

    #[test]
    fn iter_yields_all_entries() {
        let mut kd = KeyDir::new();
        kd.put(b"a".to_vec(), entry(1, 0));
        kd.put(b"b".to_vec(), entry(2, 100));
        let keys: Vec<_> = kd.iter().map(|(k, _)| k.to_vec()).collect();
        assert_eq!(keys.len(), 2);
    }

    // ------------------------------------------------------------------
    // Edge cases
    // ------------------------------------------------------------------

    #[test]
    fn zero_length_key_is_valid() {
        let mut kd = KeyDir::new();
        kd.put(vec![], entry(1, 0));
        assert!(kd.get(b"").is_some());
        assert_eq!(kd.len(), 1);
    }

    #[test]
    fn non_utf8_binary_key() {
        let key: Vec<u8> = vec![0xFF, 0xFE, 0x00, 0x01];
        let mut kd = KeyDir::new();
        kd.put(key.clone(), entry(1, 42));
        assert_eq!(kd.get(&key).unwrap().offset, 42);
    }

    #[test]
    fn large_key_256_bytes() {
        let key = vec![b'x'; 256];
        let mut kd = KeyDir::new();
        kd.put(key.clone(), entry(7, 999));
        assert_eq!(kd.get(&key).unwrap().file_id, 7);
    }

    #[test]
    fn overwrite_changes_file_id_and_offset() {
        let mut kd = KeyDir::new();
        kd.put(b"k".to_vec(), entry(1, 0));
        kd.put(b"k".to_vec(), entry(99, 65536));
        let e = kd.get(b"k").unwrap();
        assert_eq!(e.file_id, 99);
        assert_eq!(e.offset, 65536);
    }

    #[test]
    fn delete_then_reinsert_works() {
        let mut kd = KeyDir::new();
        kd.put(b"k".to_vec(), entry(1, 0));
        kd.delete(b"k");
        kd.put(b"k".to_vec(), entry(2, 50));
        assert_eq!(kd.get(b"k").unwrap().file_id, 2);
    }

    #[test]
    fn many_keys_len_is_accurate() {
        let mut kd = KeyDir::new();
        for i in 0u64..1000 {
            kd.put(i.to_le_bytes().to_vec(), entry(1, i * 100));
        }
        assert_eq!(kd.len(), 1000);
        for i in 0u64..500 {
            kd.delete(&i.to_le_bytes());
        }
        assert_eq!(kd.len(), 500);
    }

    #[test]
    fn ref_bit_full_clock_cycle() {
        // Simulate the clock hand sweeping the same key three times
        let mut kd = KeyDir::new();
        kd.put(b"k".to_vec(), entry(1, 0));

        // 1st tick: bit=0 → eligible
        assert_eq!(kd.tick_ref_bit(b"k"), Some(true));
        // touch the key (set bit)
        kd.set_ref_bit(b"k");
        // 2nd tick: bit=1 → given second chance, bit cleared
        assert_eq!(kd.tick_ref_bit(b"k"), Some(false));
        // 3rd tick: bit=0 again → eligible
        assert_eq!(kd.tick_ref_bit(b"k"), Some(true));
    }

    #[test]
    fn expired_keys_all_expired() {
        let mut kd = KeyDir::new();
        for i in 1u64..=5 {
            kd.put(vec![i as u8], expiring_entry(1, i * 100));
        }
        let expired = kd.expired_keys(9999);
        assert_eq!(expired.len(), 5);
    }

    #[test]
    fn expired_keys_none_expired() {
        let mut kd = KeyDir::new();
        for i in 1u64..=5 {
            kd.put(vec![i as u8], expiring_entry(1, i * 1_000_000));
        }
        assert!(kd.expired_keys(1).is_empty());
    }

    #[test]
    fn set_ref_bit_on_missing_key_is_noop() {
        let mut kd = KeyDir::new();
        // Must not panic
        kd.set_ref_bit(b"ghost");
    }

    // ------------------------------------------------------------------
    // len() with expired entries in the raw KeyDir
    // ------------------------------------------------------------------

    /// `KeyDir::len()` counts ALL entries including expired ones (expiry filtering
    /// is done at the Store level). Verify the count includes expired entries.
    #[test]
    fn len_includes_expired_entries_in_raw_keydir() {
        let mut kd = KeyDir::new();
        // Two expired entries + three "live" (expire_at_ms=0 means no expiry)
        kd.put(b"exp1".to_vec(), expiring_entry(1, 1_000)); // expired at ms=1000
        kd.put(b"exp2".to_vec(), expiring_entry(1, 2_000)); // expired at ms=2000
        kd.put(b"live1".to_vec(), entry(1, 0));
        kd.put(b"live2".to_vec(), entry(1, 10));
        kd.put(b"live3".to_vec(), entry(1, 20));
        // KeyDir::len() counts all 5 — expiry is a Store-level concern.
        assert_eq!(
            kd.len(),
            5,
            "raw KeyDir::len() must count all entries including expired"
        );
    }

    /// `is_empty()` returns false even when all entries are logically expired,
    /// because `KeyDir` has no concept of expiry at this layer.
    #[test]
    fn is_empty_false_even_when_all_entries_are_expired() {
        let mut kd = KeyDir::new();
        kd.put(b"e1".to_vec(), expiring_entry(1, 100));
        kd.put(b"e2".to_vec(), expiring_entry(1, 200));
        // KeyDir::is_empty() checks raw map, not logical expiry.
        assert!(
            !kd.is_empty(),
            "KeyDir::is_empty must return false — it has 2 raw entries"
        );
        assert_eq!(kd.len(), 2);
    }

    // ------------------------------------------------------------------
    // expired_keys() read-only semantics
    // ------------------------------------------------------------------

    /// `expired_keys()` returns empty vec when no entries have a non-zero past expiry.
    #[test]
    fn expired_keys_returns_empty_for_all_live() {
        let mut kd = KeyDir::new();
        kd.put(b"no_ttl".to_vec(), entry(1, 0));
        kd.put(b"far_future".to_vec(), expiring_entry(1, u64::MAX));
        let expired = kd.expired_keys(9_999_999_999);
        assert!(expired.is_empty(), "no keys should be expired");
    }

    /// `expired_keys()` returns all 5 expired keys when all have past expiry.
    #[test]
    fn expired_keys_returns_all_expired() {
        let mut kd = KeyDir::new();
        for i in 1u64..=5 {
            kd.put(vec![i as u8], expiring_entry(1, i * 10));
        }
        // Two live keys that must not appear.
        kd.put(b"live_a".to_vec(), entry(2, 0));
        kd.put(b"live_b".to_vec(), expiring_entry(2, 9_999_999));

        // now_ms=10_000 — all 5 expire_at values (10,20,30,40,50) are ≤ 10_000
        let expired = kd.expired_keys(10_000);
        assert_eq!(expired.len(), 5, "exactly 5 keys must be expired");
        // None of the live keys must appear.
        assert!(!expired.contains(&b"live_a".to_vec()));
        assert!(!expired.contains(&b"live_b".to_vec()));
    }

    /// `expired_keys()` is read-only: calling it does NOT remove entries from the keydir.
    #[test]
    fn expired_keys_does_not_remove_from_keydir() {
        let mut kd = KeyDir::new();
        kd.put(b"soon".to_vec(), expiring_entry(1, 500));
        kd.put(b"later".to_vec(), expiring_entry(1, 5_000));
        kd.put(b"never".to_vec(), entry(1, 0));

        // All three expire by now_ms=9_999_999 (except "never").
        let len_before = kd.len();
        let expired = kd.expired_keys(9_999_999);
        assert_eq!(expired.len(), 2, "2 entries should be expired");

        // len() must be unchanged — expired_keys() is read-only.
        assert_eq!(
            kd.len(),
            len_before,
            "expired_keys() must not remove entries from keydir"
        );
        // The entries are still accessible.
        assert!(
            kd.get(b"soon").is_some(),
            "entry must still exist after expired_keys()"
        );
        assert!(
            kd.get(b"later").is_some(),
            "entry must still exist after expired_keys()"
        );
    }

    // ------------------------------------------------------------------
    // ref_bit clock-hand detailed tests
    // ------------------------------------------------------------------

    /// After `set_ref_bit` + `tick_ref_bit`, the bit is false (given second chance).
    #[test]
    fn ref_bit_set_then_tick_clears_it() {
        let mut kd = KeyDir::new();
        kd.put(b"k".to_vec(), entry(1, 0));

        kd.set_ref_bit(b"k");
        // Verify the bit was set by checking entry directly.
        assert!(
            kd.get(b"k").unwrap().ref_bit,
            "ref_bit must be true after set_ref_bit"
        );

        // Tick — bit was set, so entry gets a second chance (returns false = not eligible).
        let eligible = kd.tick_ref_bit(b"k").unwrap();
        assert!(
            !eligible,
            "entry with ref_bit=true must not be eviction-eligible after tick"
        );
        // Bit must now be cleared.
        assert!(
            !kd.get(b"k").unwrap().ref_bit,
            "ref_bit must be false after tick"
        );
    }

    /// `tick_ref_bit` on an entry with `ref_bit=false` marks it as eviction-eligible.
    #[test]
    fn ref_bit_not_set_then_tick_returns_true_for_eviction() {
        let mut kd = KeyDir::new();
        kd.put(b"evictable".to_vec(), entry(1, 0));
        // ref_bit starts as false (no set_ref_bit call).
        assert!(
            !kd.get(b"evictable").unwrap().ref_bit,
            "ref_bit must start false"
        );

        let eligible = kd.tick_ref_bit(b"evictable").unwrap();
        assert!(
            eligible,
            "entry with ref_bit=false must be eviction-eligible after tick"
        );
    }

    // ------------------------------------------------------------------
    // delete detailed tests
    // ------------------------------------------------------------------

    /// Deleting a nonexistent key is a noop — `len()` must not change.
    #[test]
    fn delete_nonexistent_key_is_noop() {
        let mut kd = KeyDir::new();
        kd.put(b"real".to_vec(), entry(1, 0));
        let result = kd.delete(b"phantom");
        assert!(!result, "delete of nonexistent key must return false");
        assert_eq!(kd.len(), 1, "len must remain unchanged after no-op delete");
    }

    /// get after delete returns None.
    #[test]
    fn get_after_delete_returns_none() {
        let mut kd = KeyDir::new();
        kd.put(b"target".to_vec(), entry(1, 42));
        kd.delete(b"target");
        assert!(
            kd.get(b"target").is_none(),
            "get after delete must return None"
        );
    }

    // ------------------------------------------------------------------
    // H-5: KeyEntry struct size reduced from 40 to 32 bytes
    // ------------------------------------------------------------------

    #[test]
    fn h5_key_entry_size_is_32_bytes() {
        let size = std::mem::size_of::<KeyEntry>();
        assert_eq!(
            size, 32,
            "KeyEntry should be 32 bytes (was 40 before H-5 fix), got {size}"
        );
    }

    #[test]
    fn h5_key_entry_alignment_is_8() {
        let align = std::mem::align_of::<KeyEntry>();
        assert_eq!(
            align, 8,
            "KeyEntry alignment should be 8 bytes, got {align}"
        );
    }

    #[test]
    fn h5_field_access_after_reorder() {
        let e = KeyEntry {
            file_id: 42,
            offset: 1234,
            expire_at_ms: 999_000,
            value_size: 512,
            ref_bit: true,
        };
        assert_eq!(e.file_id, 42);
        assert_eq!(e.offset, 1234);
        assert_eq!(e.expire_at_ms, 999_000);
        assert_eq!(e.value_size, 512);
        assert!(e.ref_bit);
    }

    // ------------------------------------------------------------------
    // M-8: Box<[u8]> keys save 8 bytes per entry vs Vec<u8>
    // ------------------------------------------------------------------

    #[test]
    fn m8_box_slice_smaller_than_vec() {
        // Verify that Box<[u8]> is indeed smaller than Vec<u8>
        let box_size = std::mem::size_of::<Box<[u8]>>();
        let vec_size = std::mem::size_of::<Vec<u8>>();
        assert!(
            box_size < vec_size,
            "Box<[u8]> ({box_size} bytes) must be smaller than Vec<u8> ({vec_size} bytes)"
        );
        assert_eq!(box_size, 16, "Box<[u8]> should be 16 bytes (ptr + len)");
        assert_eq!(vec_size, 24, "Vec<u8> should be 24 bytes (ptr + len + cap)");
    }

    #[test]
    fn m8_put_get_with_boxed_keys() {
        let mut kd = KeyDir::new();
        kd.put(b"boxed_key".to_vec(), entry(1, 42));
        assert_eq!(kd.get(b"boxed_key").unwrap().offset, 42);
    }

    #[test]
    fn m8_iter_returns_slice_refs() {
        let mut kd = KeyDir::new();
        kd.put(b"a".to_vec(), entry(1, 0));
        kd.put(b"b".to_vec(), entry(2, 100));

        // iter() should return (&[u8], &KeyEntry) — verify via to_vec
        let mut keys: Vec<Vec<u8>> = kd.iter().map(|(k, _)| k.to_vec()).collect();
        keys.sort();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);
    }

    #[test]
    fn m8_delete_with_slice_lookup() {
        let mut kd = KeyDir::new();
        kd.put(b"delete_me".to_vec(), entry(1, 0));
        assert!(kd.delete(b"delete_me"));
        assert!(kd.get(b"delete_me").is_none());
    }

    #[test]
    fn m8_expired_keys_returns_vec_u8() {
        let mut kd = KeyDir::new();
        kd.put(b"exp".to_vec(), expiring_entry(1, 100));
        let expired = kd.expired_keys(200);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0], b"exp".to_vec());
    }
}
