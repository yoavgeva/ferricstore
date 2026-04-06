import Config

if config_env() == :prod do
  # ---------------------------------------------------------------------------
  # Core
  # ---------------------------------------------------------------------------
  config :ferricstore,
    protected_mode: true,
    port: String.to_integer(System.get_env("FERRICSTORE_PORT", "6379")),
    health_port: String.to_integer(System.get_env("FERRICSTORE_HEALTH_PORT", "6380")),
    data_dir: System.get_env("FERRICSTORE_DATA_DIR", "/data"),
    shard_count: (
      count = System.get_env("FERRICSTORE_SHARD_COUNT", "0")
      count = String.to_integer(count)
      if count == 0, do: System.schedulers_online(), else: count
    )

  # ---------------------------------------------------------------------------
  # Durability
  # ---------------------------------------------------------------------------
  config :ferricstore,
    default_durability: String.to_existing_atom(System.get_env("FERRICSTORE_DURABILITY", "quorum"))

  # ---------------------------------------------------------------------------
  # Memory & Eviction
  # ---------------------------------------------------------------------------
  config :ferricstore,
    max_memory_bytes: String.to_integer(System.get_env("FERRICSTORE_MAX_MEMORY", "0")),
    eviction_policy: System.get_env("FERRICSTORE_EVICTION_POLICY", "volatile_lru"),
    max_value_size: String.to_integer(System.get_env("FERRICSTORE_MAX_VALUE_SIZE", "1048576")),
    hot_cache_max_value_size: String.to_integer(System.get_env("FERRICSTORE_HOT_CACHE_MAX_VALUE_SIZE", "65536")),
    memory_guard_interval_ms: String.to_integer(System.get_env("FERRICSTORE_MEMORY_GUARD_INTERVAL_MS", "5000"))

  # ---------------------------------------------------------------------------
  # LFU Scoring
  # ---------------------------------------------------------------------------
  config :ferricstore,
    lfu_decay_time: String.to_integer(System.get_env("FERRICSTORE_LFU_DECAY_TIME", "1")),
    lfu_log_factor: String.to_integer(System.get_env("FERRICSTORE_LFU_LOG_FACTOR", "10")),
    read_sample_rate: String.to_integer(System.get_env("FERRICSTORE_READ_SAMPLE_RATE", "100"))

  # ---------------------------------------------------------------------------
  # Expiry
  # ---------------------------------------------------------------------------
  config :ferricstore,
    expiry_sweep_interval_ms: String.to_integer(System.get_env("FERRICSTORE_EXPIRY_SWEEP_INTERVAL_MS", "100")),
    expiry_max_keys_per_sweep: String.to_integer(System.get_env("FERRICSTORE_EXPIRY_MAX_KEYS_PER_SWEEP", "20"))

  # ---------------------------------------------------------------------------
  # Slow Log
  # ---------------------------------------------------------------------------
  config :ferricstore,
    slowlog_log_slower_than_us: String.to_integer(System.get_env("FERRICSTORE_SLOWLOG_SLOWER_THAN_US", "10000")),
    slowlog_max_len: String.to_integer(System.get_env("FERRICSTORE_SLOWLOG_MAX_LEN", "128"))

  # ---------------------------------------------------------------------------
  # Security
  # ---------------------------------------------------------------------------
  config :ferricstore,
    protected_mode: System.get_env("FERRICSTORE_PROTECTED_MODE", "true") == "true",
    maxclients: String.to_integer(System.get_env("FERRICSTORE_MAXCLIENTS", "10000")),
    audit_log_enabled: System.get_env("FERRICSTORE_AUDIT_LOG", "false") == "true",
    acl_auto_save: System.get_env("FERRICSTORE_ACL_AUTO_SAVE", "false") == "true"

  # ---------------------------------------------------------------------------
  # TLS (optional)
  # ---------------------------------------------------------------------------
  tls_cert = System.get_env("FERRICSTORE_TLS_CERT_FILE")

  if tls_cert do
    config :ferricstore,
      tls_port: String.to_integer(System.get_env("FERRICSTORE_TLS_PORT", "6380")),
      tls_cert_file: tls_cert,
      tls_key_file: System.get_env("FERRICSTORE_TLS_KEY_FILE"),
      tls_ca_cert_file: System.get_env("FERRICSTORE_TLS_CA_CERT_FILE"),
      require_tls: System.get_env("FERRICSTORE_REQUIRE_TLS", "false") == "true"
  end

  # ---------------------------------------------------------------------------
  # Connection
  # ---------------------------------------------------------------------------
  socket_mode = System.get_env("FERRICSTORE_SOCKET_ACTIVE_MODE", "true")

  config :ferricstore,
    socket_active_mode: (
      case socket_mode do
        "true" -> true
        "once" -> :once
        n -> String.to_integer(n)
      end
    )

  # ---------------------------------------------------------------------------
  # Raft / Internals
  # ---------------------------------------------------------------------------
  config :ferricstore,
    release_cursor_interval: String.to_integer(System.get_env("FERRICSTORE_RELEASE_CURSOR_INTERVAL", "10000")),
    promotion_threshold: String.to_integer(System.get_env("FERRICSTORE_PROMOTION_THRESHOLD", "100"))

  # ---------------------------------------------------------------------------
  # Supervisor (test-only tuning, production defaults are fine)
  # ---------------------------------------------------------------------------
  config :ferricstore,
    supervisor_max_restarts: {
      String.to_integer(System.get_env("FERRICSTORE_MAX_RESTARTS", "20")),
      String.to_integer(System.get_env("FERRICSTORE_MAX_RESTARTS_SECONDS", "10"))
    }

  # ---------------------------------------------------------------------------
  # Clustering
  # ---------------------------------------------------------------------------
  node_name = System.get_env("FERRICSTORE_NODE_NAME")
  _cookie = System.get_env("FERRICSTORE_COOKIE", "ferricstore")

  if node_name do
    config :ferricstore, :cluster_nodes,
      String.split(System.get_env("FERRICSTORE_CLUSTER_NODES", ""), ",")
  end
end
