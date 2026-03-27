#!/usr/bin/env elixir

# Reproduction script for ra WAL empty file bug.
# Run with: elixir docs/ra-empty-wal-bug-repro.exs

Mix.install([{:ra, "~> 3.1"}])

dir = Path.join(System.tmp_dir!(), "ra_eof_bug_#{System.unique_integer([:positive])}")
File.mkdir_p!(dir)

# Create an empty WAL file (simulates kill -9 during WAL rotation)
File.write!(Path.join(dir, "0000000000000001.wal"), <<>>)

IO.puts("ra version: #{Application.spec(:ra, :vsn)}")
IO.puts("Empty WAL file: #{dir}/0000000000000001.wal (0 bytes)")
IO.puts("Starting ra system...")
IO.puts("")

ra_name = :repro_bug
config = %{
  name: ra_name,
  names: :ra_system.derive_names(ra_name),
  data_dir: to_charlist(dir),
  wal_data_dir: to_charlist(dir),
  segment_max_entries: 32768
}

case :ra_system.start(config) do
  {:ok, _} ->
    IO.puts("OK — no crash (bug is fixed in this version)")
    :ra_system.stop(ra_name)

  {:error, _reason} ->
    IO.puts("CRASHED with {:case_clause, :eof}")
    IO.puts("The bug exists in this version of ra.")
end

File.rm_rf!(dir)
