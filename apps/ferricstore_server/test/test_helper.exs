# Server app test helper.
# Tags are inherited from the umbrella root run and from the core app.
# NOTE: Do NOT rm_rf data_dir in after_suite — it destroys Bitcask files
# while shards are still running, causing cascading failures in subsequent apps.
ExUnit.start(
  exclude: [:bench, :linux_io_uring, :large_alloc, :cluster, :jepsen, :shard_kill, :compaction, :conn_lifecycle, :concurrency],
  formatters: [ExUnit.CLIFormatter, Ferricstore.Test.AuditFormatter]
)
