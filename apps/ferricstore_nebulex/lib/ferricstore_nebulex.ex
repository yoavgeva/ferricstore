defmodule FerricstoreNebulex do
  @moduledoc """
  Nebulex 3.x adapter for FerricStore.

  This package provides `FerricstoreNebulex.Adapter`, a drop-in replacement for
  `Nebulex.Adapters.Local` backed by FerricStore's persistent, crash-recoverable
  storage engine.

  ## Quick Start

      # 1. Define your cache
      defmodule MyApp.Cache do
        use Nebulex.Cache,
          otp_app: :my_app,
          adapter: FerricstoreNebulex.Adapter
      end

      # 2. Configure (config/config.exs)
      config :my_app, MyApp.Cache,
        prefix: "cache",
        serializer: :erlang_term

      # 3. Add to supervision tree (application.ex)
      children = [MyApp.Cache]

      # 4. Use it
      MyApp.Cache.put("session:42", %{user_id: 42}, ttl: :timer.hours(1))
      MyApp.Cache.get("session:42")
      #=> %{user_id: 42}

  All standard Nebulex operations (`get`, `put`, `delete`, `get_all`,
  `count_all`, `delete_all`, `incr`, `decr`, `ttl`, `expire`, `take`,
  `has_key?`, `stream`, `transaction`) work out of the box.

  ## Why FerricStore Over Other Adapters?

  | Feature | Nebulex.Adapters.Local | FerricstoreNebulex.Adapter |
  |---------|----------------------|---------------------------|
  | Storage | ETS (in-memory) | Bitcask (persistent) |
  | Survives restart | No | Yes |
  | Eviction | Generation-based | LFU with time decay |
  | Multi-node | No | Raft consensus |
  | Data structures | Key-value only | Hash, List, Set, ZSet |

  ## Features

  - **Persistent storage** that survives BEAM restarts and crashes
  - **Millisecond-precision TTL** mapped directly to FerricStore's expiry system
  - **LFU eviction** with time decay for intelligent memory management
  - **Transparent serialization** of arbitrary Elixir terms (structs, tuples, maps)
  - **Non-binary key support** -- atoms, integers, and tuples are auto-encoded
  - **Async-safe test isolation** via `FerricStore.Sandbox`
  - **Two serializers** -- `:erlang_term` (any Elixir term) or `:jason` (JSON-compatible)

  ## Configuration

  | Option | Default | Description |
  |--------|---------|-------------|
  | `:prefix` | `"nbx"` | Key prefix in FerricStore. Enables namespacing multiple caches. |
  | `:serializer` | `:erlang_term` | `:erlang_term` for any Elixir term, `:jason` for JSON. |

  See `FerricstoreNebulex.Adapter` for the full adapter implementation and
  per-operation documentation.
  """
end
