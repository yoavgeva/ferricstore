import Config

config :ferricstore, Ferricstore.Bitcask.NIF,
  skip_compilation?: true,
  load_from: {:ferricstore, "priv/native/ferricstore_bitcask"}

config :ferricstore, :port, 0
config :ferricstore, :data_dir, System.tmp_dir!() <> "/ferricstore_bench"
