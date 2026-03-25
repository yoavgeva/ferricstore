import Config

config :ferricstore, Ferricstore.Bitcask.NIF,
  skip_compilation?: true,
  load_from: {:ferricstore, "priv/native/ferricstore_bitcask"}

# TCP server port (default: 6379, matches Redis)
config :ferricstore, :port, 6379

# Data directory for Bitcask shards
config :ferricstore, :data_dir, "data"

# LFU decay: minutes per decay step (0 = no decay). Matches Redis lfu-decay-time.
config :ferricstore, :lfu_decay_time, 1
# LFU log factor: controls probabilistic increment curve. Matches Redis lfu-log-factor.
config :ferricstore, :lfu_log_factor, 10

# L1 per-connection cache defaults.
# Each connection process caches GET results in a Map to avoid repeated ETS
# (L2) lookups for hot keys. Invalidation is handled by CLIENT TRACKING:
# when another connection writes a tracked key, L1 clears the stale entry.
config :ferricstore, :l1_cache_enabled, true
config :ferricstore, :l1_cache_max_entries, 64
config :ferricstore, :l1_cache_max_bytes, 1_048_576

# Sendfile zero-copy threshold for GET responses in standalone TCP mode.
# Values >= this size are served via :file.sendfile/5 (kernel zero-copy)
# instead of reading into BEAM memory. Only applies to cold (on-disk) keys
# over plain TCP (:ranch_tcp); TLS and hot keys always use the normal path.
config :ferricstore_server, :sendfile_threshold, 65_536

# Node discovery via libcluster.
# Default: Gossip strategy for local/dev multi-node clusters.
# Override in prod.exs or runtime.exs for Kubernetes DNS or other strategies.
config :libcluster,
  topologies: [
    ferricstore: [
      strategy: Cluster.Strategy.Gossip,
      config: [
        port: 45892,
        if_addr: "0.0.0.0",
        multicast_if: "0.0.0.0",
        multicast_addr: "230.1.1.251",
        multicast_ttl: 1
      ]
    ]
  ]

import_config "#{config_env()}.exs"
