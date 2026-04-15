defmodule Ferricstore.WritePathProfiler do
  @moduledoc """
  Lightweight profiler for the write path. Uses :atomics for zero-contention
  counters. Enable during investigation, disable in production.

  Tracks timing at each stage of a quorum write:
  1. Connection → Router.put (RESP parse + dispatch)
  2. Router.put → Shard GenServer.call
  3. Shard handle_call → Batcher.write_async (cast)
  4. Batcher enqueue → flush timer fires
  5. Batcher flush → ra:pipeline_command
  6. ra pipeline → WAL write + sync → apply → reply

  Call `start/0` to begin profiling, `report/0` to print results,
  `stop/0` to disable.
  """

  @counters_size 20

  # Counter indices
  @c_writes 1           # total writes observed
  @c_batcher_enqueue 2  # Batcher.write_async calls
  @c_batcher_flush 3    # Batcher flush_slot calls
  @c_batcher_flush_size 4  # total commands per flush (for avg)
  @c_ra_pipeline 5      # ra:pipeline_command calls
  @c_ra_applied 6       # ra_event applied callbacks
  @c_sync_requested 7   # wal_nif:sync calls
  @c_sync_completed 8   # wal_sync_complete received
  @c_time_batcher_enqueue_us 9  # cumulative time in enqueue_write (μs)
  @c_time_batcher_flush_us 10   # cumulative time in do_flush_slot (μs)
  @c_time_ra_pipeline_us 11     # cumulative time in pipeline_submit (μs)

  def start do
    ref = :atomics.new(@counters_size, signed: false)
    :persistent_term.put(:write_path_profiler, ref)
    :ok
  end

  def stop do
    :persistent_term.erase(:write_path_profiler)
    :ok
  end

  def enabled? do
    try do
      :persistent_term.get(:write_path_profiler)
      true
    catch
      _, _ -> false
    end
  end

  def incr(index, delta \\ 1) do
    try do
      ref = :persistent_term.get(:write_path_profiler)
      :atomics.add(ref, index, delta)
    catch
      _, _ -> :ok
    end
  end

  def get(index) do
    try do
      ref = :persistent_term.get(:write_path_profiler)
      :atomics.get(ref, index)
    catch
      _, _ -> 0
    end
  end

  # Convenience — measure a block and add to a timing counter
  defmacro timed(counter_index, do: block) do
    quote do
      if Ferricstore.WritePathProfiler.enabled?() do
        t1 = System.monotonic_time(:microsecond)
        result = unquote(block)
        t2 = System.monotonic_time(:microsecond)
        Ferricstore.WritePathProfiler.incr(unquote(counter_index), t2 - t1)
        result
      else
        unquote(block)
      end
    end
  end

  def report do
    writes = get(@c_writes)
    enqueues = get(@c_batcher_enqueue)
    flushes = get(@c_batcher_flush)
    flush_size = get(@c_batcher_flush_size)
    pipelines = get(@c_ra_pipeline)
    applied = get(@c_ra_applied)
    sync_req = get(@c_sync_requested)
    sync_done = get(@c_sync_completed)
    t_enqueue = get(@c_time_batcher_enqueue_us)
    t_flush = get(@c_time_batcher_flush_us)
    t_pipeline = get(@c_time_ra_pipeline_us)

    avg_flush_size = if flushes > 0, do: div(flush_size, flushes), else: 0
    avg_enqueue_us = if enqueues > 0, do: div(t_enqueue, enqueues), else: 0
    avg_flush_us = if flushes > 0, do: div(t_flush, flushes), else: 0
    avg_pipeline_us = if pipelines > 0, do: div(t_pipeline, pipelines), else: 0

    IO.puts("""

    === Write Path Profile ===
    Writes total:           #{writes}
    Batcher enqueues:       #{enqueues}
    Batcher flushes:        #{flushes}
    Avg commands per flush: #{avg_flush_size}
    Ra pipeline_commands:   #{pipelines}
    Ra applied (events):    #{applied}
    WAL sync requested:     #{sync_req}
    WAL sync completed:     #{sync_done}
    Avg enqueue time:       #{avg_enqueue_us}μs
    Avg flush time:         #{avg_flush_us}μs
    Avg pipeline time:      #{avg_pipeline_us}μs
    """)
  end

  # Counter index accessors for use in other modules
  def c_writes, do: @c_writes
  def c_batcher_enqueue, do: @c_batcher_enqueue
  def c_batcher_flush, do: @c_batcher_flush
  def c_batcher_flush_size, do: @c_batcher_flush_size
  def c_ra_pipeline, do: @c_ra_pipeline
  def c_ra_applied, do: @c_ra_applied
  def c_sync_requested, do: @c_sync_requested
  def c_sync_completed, do: @c_sync_completed
  def c_time_batcher_enqueue_us, do: @c_time_batcher_enqueue_us
  def c_time_batcher_flush_us, do: @c_time_batcher_flush_us
  def c_time_ra_pipeline_us, do: @c_time_ra_pipeline_us
end
