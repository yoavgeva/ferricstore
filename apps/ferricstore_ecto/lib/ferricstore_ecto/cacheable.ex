defmodule FerricStore.Ecto.Cacheable do
  @moduledoc """
  Marks an Ecto schema as cacheable by the FerricStore L2 cache.

  Schemas that `use FerricStore.Ecto.Cacheable` opt in to automatic
  caching. Schemas without this module are never cached -- `Repo.get`,
  `Repo.all`, etc. always hit the database. This is intentional: you
  choose exactly which tables benefit from caching.

  ## How It Works

  This module injects a `__ferricstore_cache_config__/0` function into
  your schema module. The cache layer (`FerricstoreEcto.Cache`) calls
  this function to determine:

  - Whether the schema is cacheable at all
  - The TTL for cache entries
  - The cache strategy (read/write behavior)
  - The cache region (for future per-region configuration)

  ## Options

    * `:ttl` - Time-to-live in milliseconds. How long a cached entity
      remains valid before it expires and the next read hits the database.
      Defaults to `300_000` (5 minutes). Use `:timer` helpers for clarity:

          ttl: :timer.minutes(10)   # 600_000 ms
          ttl: :timer.hours(1)      # 3_600_000 ms
          ttl: :timer.seconds(30)   # 30_000 ms

    * `:region` - Cache region name for independent configuration.
      Defaults to `"default"`. Use different regions to group schemas
      with similar access patterns:

          region: "hot"       # frequently accessed, short TTL
          region: "reference" # rarely changes, long TTL

    * `:strategy` - Cache strategy that controls read/write behavior.
      Defaults to `:read_write`. Available strategies:

      - `:read_write` -- Cache reads, invalidate on writes. Best for
        most schemas. This is the safe default.

      - `:read_only` -- Cache reads. Writes still invalidate the cache
        but also log a warning, since writes to read-only data are
        unexpected. Use for reference tables (countries, currencies,
        feature flags) that are populated by seeds/migrations.

      - `:nonstrict_read_write` -- Same as `:read_write`. Reserved for
        future optimization that may allow slightly stale reads in
        exchange for reduced invalidation overhead.

    * `:enabled` - Boolean to enable or disable caching for this schema.
      Defaults to `true`. Set to `false` to temporarily disable caching
      without removing the `use` line. Useful during schema migrations
      or when debugging cache-related issues:

          use FerricStore.Ecto.Cacheable,
            ttl: :timer.minutes(5),
            enabled: false  # disabled until migration completes

  ## Examples

  ### Basic Usage

      defmodule MyApp.User do
        use Ecto.Schema
        use FerricStore.Ecto.Cacheable,
          ttl: :timer.minutes(10),
          strategy: :read_write

        schema "users" do
          field :name, :string
          field :email, :string
          timestamps()
        end
      end

      MyApp.User.__ferricstore_cache_config__()
      #=> %{ttl: 600_000, region: "default", strategy: :read_write, enabled: true}

  ### Reference Data (Long TTL, Read-Only)

      defmodule MyApp.Country do
        use Ecto.Schema
        use FerricStore.Ecto.Cacheable,
          ttl: :timer.hours(24),
          strategy: :read_only,
          region: "reference"

        schema "countries" do
          field :name, :string
          field :code, :string
        end
      end

  ### Write-Heavy Table (Not Cached)

      defmodule MyApp.AuditLog do
        use Ecto.Schema
        # No `use FerricStore.Ecto.Cacheable`
        # Every Repo.all/get always hits the database

        schema "audit_logs" do
          field :action, :string
          field :details, :map
          timestamps()
        end
      end
  """

  @doc false
  defmacro __using__(opts) do
    quote do
      @ferricstore_cache_opts unquote(opts)

      @doc false
      def __ferricstore_cache_config__ do
        %{
          ttl: unquote(Keyword.get(opts, :ttl, :timer.minutes(5))),
          region: unquote(Keyword.get(opts, :region, "default")),
          strategy: unquote(Keyword.get(opts, :strategy, :read_write)),
          enabled: unquote(Keyword.get(opts, :enabled, true))
        }
      end
    end
  end
end
