import Config

if config_env() == :prod do
  config :ferricstore,
    port: String.to_integer(System.get_env("FERRICSTORE_PORT", "6379")),
    health_port: String.to_integer(System.get_env("FERRICSTORE_HEALTH_PORT", "6380")),
    data_dir: System.get_env("FERRICSTORE_DATA_DIR", "/data"),
    shard_count: (
      count = System.get_env("FERRICSTORE_SHARD_COUNT", "0")
      count = String.to_integer(count)
      if count == 0, do: System.schedulers_online(), else: count
    ),
    default_durability: String.to_existing_atom(System.get_env("FERRICSTORE_DURABILITY", "quorum")),
    sandbox_enabled: false

  # Clustering
  node_name = System.get_env("FERRICSTORE_NODE_NAME")
  cookie = System.get_env("FERRICSTORE_COOKIE", "ferricstore")

  if node_name do
    config :ferricstore, :cluster_nodes,
      String.split(System.get_env("FERRICSTORE_CLUSTER_NODES", ""), ",")
  end
end
