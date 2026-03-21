import Config

config :ferricstore, Ferricstore.Bitcask.NIF,
  skip_compilation?: true,
  load_from: {:ferricstore, "priv/native/ferricstore_bitcask"}

# TCP server port (default: 6379, matches Redis)
config :ferricstore, :port, 6379

# Data directory for Bitcask shards
config :ferricstore, :data_dir, "data"

# L1 per-connection cache defaults.
# Each connection process caches GET results in a Map to avoid repeated ETS
# (L2) lookups for hot keys. Invalidation is handled by CLIENT TRACKING:
# when another connection writes a tracked key, L1 clears the stale entry.
config :ferricstore, :l1_cache_enabled, true
config :ferricstore, :l1_cache_max_entries, 64
config :ferricstore, :l1_cache_max_bytes, 1_048_576

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
