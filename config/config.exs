import Config

config :ferricstore, Ferricstore.Bitcask.NIF,
  skip_compilation?: true,
  load_from: {:ferricstore, "priv/native/ferricstore_bitcask"}

# TCP server port (default: 6379, matches Redis)
config :ferricstore, :port, 6379

# Data directory for Bitcask shards
config :ferricstore, :data_dir, "data"

import_config "#{config_env()}.exs"
