# Re-create FerricStore data directories if they were cleaned up by a
# previous app's after_suite (e.g., ferricstore_server). In umbrella
# `mix test` runs, each app gets its own ExUnit cycle, so the server
# app's after_suite may have deleted the data dir before we start.
data_dir = Application.fetch_env!(:ferricstore, :data_dir)
shard_count = Application.get_env(:ferricstore, :shard_count, 4)

for i <- 0..(shard_count - 1) do
  File.mkdir_p!(Path.join(data_dir, "shard_#{i}"))
end

# Start the test repo
{:ok, _} = FerricstoreEcto.TestRepo.start_link()

# Run migrations
Ecto.Migrator.up(FerricstoreEcto.TestRepo, 1, FerricstoreEcto.Test.Migrations, log: false)

# Set sandbox mode
Ecto.Adapters.SQL.Sandbox.mode(FerricstoreEcto.TestRepo, :manual)

# Nuclear flush: clear ALL ETS keydir entries
shard_count = Application.get_env(:ferricstore, :shard_count, 4)

for i <- 0..(shard_count - 1) do
  try do :ets.delete_all_objects(:"keydir_#{i}") catch :error, :badarg -> :ok end
  try do :ets.delete_all_objects(:"prefix_keys_#{i}") catch :error, :badarg -> :ok end
end

ExUnit.start()
