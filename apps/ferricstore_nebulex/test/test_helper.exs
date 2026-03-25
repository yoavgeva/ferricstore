# Nuclear flush: clear ALL ETS keydir entries to ensure clean state
# when running after other app tests in umbrella. Also clears prefix index.
shard_count = Application.get_env(:ferricstore, :shard_count, 4)

for i <- 0..(shard_count - 1) do
  try do :ets.delete_all_objects(:"keydir_#{i}") catch :error, :badarg -> :ok end
  try do :ets.delete_all_objects(:"prefix_keys_#{i}") catch :error, :badarg -> :ok end
end

ExUnit.start()
