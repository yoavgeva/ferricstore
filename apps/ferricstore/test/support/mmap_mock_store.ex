defmodule Ferricstore.Test.MmapMockStore do
  @moduledoc """
  Creates a mock store with mmap-backed registry callbacks for probabilistic
  data structures (Bloom, Cuckoo, CMS).

  Each call to `make_cuckoo/0` or `make_cms/0` returns a store map with a
  `cuckoo_registry` or `cms_registry` key containing callback functions
  that manage NIF resources in an Agent.
  """

  alias Ferricstore.Bitcask.NIF

  @doc """
  Creates a mock store with a cuckoo_registry backed by an Agent and temp dir.
  """
  @spec make_cuckoo() :: map()
  def make_cuckoo do
    tmp_dir = make_tmp_dir("cuckoo")
    {:ok, pid} = Agent.start_link(fn -> %{} end)

    registry = %{
      get: fn key ->
        Agent.get(pid, fn state -> Map.get(state, key) end)
      end,
      put: fn key, resource, meta ->
        Agent.update(pid, fn state -> Map.put(state, key, {resource, meta}) end)
        :ok
      end,
      delete: fn key ->
        Agent.update(pid, fn state -> Map.delete(state, key) end)
        :ok
      end,
      path: fn key ->
        safe = String.replace(key, ~r/[^a-zA-Z0-9_.\-]/, "_")
        Path.join(tmp_dir, "#{safe}.cuckoo")
      end
    }

    exists_fn = fn key ->
      Agent.get(pid, fn state -> Map.has_key?(state, key) end)
    end

    %{
      cuckoo_registry: registry,
      exists?: exists_fn
    }
  end

  @doc """
  Creates a mock store with a cms_registry backed by an Agent and temp dir.
  """
  @spec make_cms() :: map()
  def make_cms do
    tmp_dir = make_tmp_dir("cms")
    {:ok, pid} = Agent.start_link(fn -> %{} end)

    registry = %{
      get: fn key ->
        Agent.get(pid, fn state -> Map.get(state, key) end)
      end,
      put: fn key, resource, meta ->
        Agent.update(pid, fn state -> Map.put(state, key, {resource, meta}) end)
        :ok
      end,
      delete: fn key ->
        Agent.update(pid, fn state -> Map.delete(state, key) end)
        :ok
      end,
      path: fn key ->
        safe = String.replace(key, ~r/[^a-zA-Z0-9_.\-]/, "_")
        Path.join(tmp_dir, "#{safe}.cms")
      end
    }

    exists_fn = fn key ->
      Agent.get(pid, fn state -> Map.has_key?(state, key) end)
    end

    %{
      cms_registry: registry,
      exists?: exists_fn
    }
  end

  @doc """
  Creates a mock store with a bloom_registry backed by an Agent and temp dir.
  """
  @spec make_bloom() :: map()
  def make_bloom do
    tmp_dir = make_tmp_dir("bloom")
    {:ok, pid} = Agent.start_link(fn -> %{} end)

    registry = %{
      get: fn key ->
        Agent.get(pid, fn state -> Map.get(state, key) end)
      end,
      put: fn key, resource, meta ->
        Agent.update(pid, fn state -> Map.put(state, key, {resource, meta}) end)
        :ok
      end,
      delete: fn key ->
        Agent.update(pid, fn state -> Map.delete(state, key) end)
        :ok
      end,
      path: fn key ->
        safe = String.replace(key, ~r/[^a-zA-Z0-9_.\-]/, "_")
        Path.join(tmp_dir, "#{safe}.bloom")
      end
    }

    exists_fn = fn key ->
      Agent.get(pid, fn state -> Map.has_key?(state, key) end)
    end

    %{
      bloom_registry: registry,
      exists?: exists_fn
    }
  end

  defp make_tmp_dir(prefix) do
    dir = Path.join(System.tmp_dir!(), "ferricstore_test_#{prefix}_#{:erlang.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    dir
  end
end
