defmodule FerricstoreEcto do
  @moduledoc """
  Transparent L2 cache for Ecto, powered by FerricStore.

  Provides automatic caching for Ecto Repo operations with zero boilerplate.
  Schemas opt in with `use FerricStore.Ecto.Cacheable`, and Repos gain
  caching by replacing `use Ecto.Repo` with `use FerricStore.Ecto.CachedRepo`.

  ## Quick Start

      # 1. Switch your Repo (one-line change)
      defmodule MyApp.Repo do
        use FerricStore.Ecto.CachedRepo,  # was: use Ecto.Repo
          otp_app: :my_app,
          adapter: Ecto.Adapters.Postgres
      end

      # 2. Mark schemas as cacheable
      defmodule MyApp.User do
        use Ecto.Schema
        use FerricStore.Ecto.Cacheable, ttl: :timer.minutes(10)

        schema "users" do
          field :name, :string
          field :email, :string
          timestamps()
        end
      end

      # 3. Existing code works unchanged
      Repo.get(User, 42)                  # DB hit, cached
      Repo.get(User, 42)                  # cache hit, no DB
      Repo.update(changeset)              # DB write, cache invalidated
      Repo.get(User, 42, cache: false)    # bypass cache

  Schemas without `Cacheable` (audit logs, event streams) always hit the
  database -- you choose what gets cached.

  ## Architecture

  Two cache tiers work together:

    * **Entity cache** -- FerricStore hashes keyed by `"ecto:{table}:{pk}"`.
      Each schema field is stored as an individual hash field via
      `FerricstoreEcto.FieldCodec`. Type-aware serialization provides
      deterministic round-trips for all standard Ecto types.

    * **Query cache** -- PK lists stored as LISTs (ordered queries) or SETs
      (unordered queries). A per-table generation counter
      (`"ecto:gen:{table}"`) is embedded in the cache key. Table mutations
      increment the counter, making all old-generation entries unreachable
      without expensive bulk DELs.

  ## Invalidation

  All mutations (`insert`, `update`, `delete`) trigger:

  1. `DEL ecto:{table}:{pk}` -- remove entity cache entry
  2. `INCR ecto:gen:{table}` -- bump generation counter

  The cache is never surgically updated. The next read repopulates from the
  database, eliminating stale-data bugs.

  ## Per-Query Control

      Repo.get(User, 42)                     # normal: read cache, populate on miss
      Repo.get(User, 42, cache: false)       # bypass: skip cache entirely
      Repo.get(User, 42, cache: :refresh)    # refresh: skip read, repopulate
      Repo.get(User, 42, cache: :cache_only) # cache only: no DB fallback
      Repo.get(User, 42, cache: [ttl: ms])   # custom TTL for this read

  ## Modules

    * `FerricStore.Ecto.CachedRepo` -- drop-in replacement for `use Ecto.Repo`
    * `FerricStore.Ecto.Cacheable` -- schema-level cache configuration
    * `FerricstoreEcto.Cache` -- core cache logic (entity + query + invalidation)
    * `FerricstoreEcto.FieldCodec` -- type-aware field serialization
    * `FerricstoreEcto.QueryHash` -- deterministic query hashing
  """
end
