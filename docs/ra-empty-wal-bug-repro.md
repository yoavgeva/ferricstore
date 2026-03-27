# ra: empty WAL file crashes recovery with `{:case_clause, :eof}`

## Bug Summary

When a WAL file exists but is empty (0 bytes) — e.g., after a kill -9 during WAL rotation — `ra_log_wal` crashes on startup with `{:case_clause, :eof}`, preventing the ra system from starting.

## Affected Code

`ra_log_wal.erl`, function `open_at_first_record/1`:

```erlang
open_at_first_record(File) ->
    {ok, Fd} = file:open(File, [read, binary, raw]),
    case file:read(Fd, 5) of
        {ok, <<?MAGIC, ?CURRENT_VERSION:8/unsigned>>} ->
            Fd;
        {ok, <<Magic:4/binary, UnknownVersion:8/unsigned>>} ->
            exit({unknown_wal_file_format, Magic, UnknownVersion})
    end.
```

`file:read/2` returns `eof` for an empty file, but neither clause matches `eof`.

## Minimal Reproduction (pure ra, no dependencies)

```elixir
# 1. Add ra to a fresh mix project:
#    {:ra, "~> 2.15"}

# 2. Run this script:
Mix.install([{:ra, "~> 2.15"}])

dir = Path.join(System.tmp_dir!(), "ra_eof_bug_#{System.unique_integer([:positive])}")
File.mkdir_p!(dir)

# Create an empty WAL file (simulates kill -9 during WAL rotation)
File.write!(Path.join(dir, "0000000000000001.wal"), <<>>)

IO.puts("Empty WAL file created at: #{dir}/0000000000000001.wal")
IO.puts("File size: 0 bytes")
IO.puts("Starting ra system...")

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
    IO.puts("OK (no crash)")
    :ra_system.stop(ra_name)

  {:error, reason} ->
    IO.puts("CRASHED: #{inspect(reason)}")
end

File.rm_rf!(dir)
```

## Expected Output (current behavior)

```
Empty WAL file created at: /tmp/ra_eof_bug_12345/0000000000000001.wal
File size: 0 bytes
Starting ra system...
CRASHED: {{:shutdown, {:failed_to_start_child, :ra_log_sup, {:shutdown,
  {:failed_to_start_child, :ra_log_wal_sup, {:shutdown,
    {:failed_to_start_child, :ra_log_wal, {:case_clause, :eof}}}}}}}, ...}
```

## How This Happens in Production

1. ra rotates the WAL: creates a new `.wal` file
2. Before the 5-byte header is written, the process is killed (`kill -9`, OOM, hardware failure)
3. An empty 0-byte `.wal` file remains on disk
4. On restart, `open_at_first_record/1` opens it, `file:read(Fd, 5)` returns `eof`
5. `{:case_clause, :eof}` — ra system won't start, application crashes

Also reproducible with a truncated header (< 5 bytes written before crash):

```elixir
# Write 3 bytes instead of <<>> — same crash, different clause
File.write!(Path.join(dir, "0000000000000001.wal"), <<0, 1, 2>>)
```

## Suggested Fix

Add `eof` and partial-header clauses:

```erlang
open_at_first_record(File) ->
    {ok, Fd} = file:open(File, [read, binary, raw]),
    case file:read(Fd, 5) of
        {ok, <<?MAGIC, ?CURRENT_VERSION:8/unsigned>>} ->
            Fd;
        {ok, <<Magic:4/binary, UnknownVersion:8/unsigned>>} ->
            exit({unknown_wal_file_format, Magic, UnknownVersion});
        eof ->
            %% Empty WAL file — treat as empty, recovery loop will see eof
            Fd;
        {ok, _PartialHeader} ->
            %% Truncated header (< 5 bytes) — unrecoverable, treat as empty
            {ok, 0} = file:position(Fd, 0),
            Fd
    end.
```

## Verified Affected Versions

Tested and confirmed on both:

- **ra 2.17.2** — `{:case_clause, :eof}` at `ra_log_wal.erl` line 920
- **ra 3.1.2** (latest) — `{:case_clause, :eof}` at `ra_log_wal.erl` line 825

The bug has existed since `open_at_first_record/1` was introduced. Both v2.x and v3.x have the identical code pattern with no `eof` clause.

```
13:09:45.113 [error] WAL in repro_bug failed to initialise with {:case_clause, :eof}, stack [
  {:ra_log_wal, :open_at_first_record, 1, [file: ..., line: 825]},
  {:ra_log_wal, :"-recover_wal/2-anonymous-5-", 7, [file: ..., line: 419]},
  ...
]
```

## Environment

- ra version: **2.17.2 and 3.1.2** (both confirmed)
- Erlang/OTP: 28
- OS: Linux / macOS
- Discovered in FerricStore (distributed cache using ra for Raft consensus)

## Impact

This affects any system using ra (including RabbitMQ quorum queues). If a node is kill -9'd or OOM-killed during WAL file rotation, the ra system will not start on the next boot.
