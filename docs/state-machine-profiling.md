# StateMachine Profiling Results

## Instrumented: do_put and Router.put

### StateMachine do_put breakdown (per write):

| Step | Typical | Spikes | Notes |
|------|---------|--------|-------|
| NIF v2_append_batch_nosync | 30-60us | 1-4ms | File open+write+close, no fsync |
| value_for_ets | <1us | — | byte_size check |
| :ets.insert | 0-5us | 33us | 7-tuple insert |
| sm_prefix_track | 4-30us | 100us | ETS delete_object + insert |
| **Total do_put** | **36-90us** | **1-4ms** | NIF spikes dominate |

### Router round-trip (per write):

| Step | Typical | Range | Notes |
|------|---------|-------|-------|
| check_keydir_full | 0-1us | — | persistent_term check |
| quorum_write (full ra round-trip) | 2-6ms | 2-18ms | pipeline_command → ra_event |
| **Total Router.put** | **2-6ms** | **2-18ms** | ra internal pipeline dominates |

### Where the time goes:

```
Router.put total:    ~5ms typical
  ├── check_keydir:  0us
  └── ra round-trip: ~5ms
       ├── pipeline_command send:     ~0.01ms
       ├── ra_server receives + processes: ~0.1ms
       ├── WAL write:                 ~0.1ms
       ├── async fdatasync wait:      ~3-5ms (macOS APFS)
       ├── ra_server notification:    ~0.1ms
       ├── StateMachine apply:        ~0.05ms (do_put = 50us)
       └── ra_event back to caller:   ~0.1ms
```

### Key finding:
StateMachine (do_put) is fast at 50us. The bottleneck is the ra internal
round-trip (~5ms), dominated by the async fdatasync wait (~3-5ms).

The nosync NIF change worked — v2_append_batch_nosync is 30-60us vs
v2_append_batch which was 5-20ms. But the ra WAL fdatasync (even async)
still gates the overall throughput.

### Projected improvement on Linux NVMe:
- macOS APFS fdatasync: ~5ms → ~200 fsyncs/sec → ~10,000 writes/sec
- Linux NVMe fdatasync: ~200us → ~5,000 fsyncs/sec → ~250,000 writes/sec
