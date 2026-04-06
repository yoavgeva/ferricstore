defmodule FerricStore.Ecto.CachedRepo do
  @moduledoc """
  Drop-in replacement for `use Ecto.Repo` that adds transparent L2
  caching backed by FerricStore.

  Replace `use Ecto.Repo` with `use FerricStore.Ecto.CachedRepo` in
  your Repo module. All existing code (`Repo.get`, `Repo.all`, etc.)
  works unchanged -- cache lookups and invalidation happen automatically.

  ## Usage

      defmodule MyApp.Repo do
        use FerricStore.Ecto.CachedRepo,
          otp_app: :my_app,
          adapter: Ecto.Adapters.Postgres
      end

  All options (`:otp_app`, `:adapter`, `:pool_size`, etc.) are forwarded
  to `Ecto.Repo` unchanged.

  ## Cache Modes (Per-Query)

  Every Repo operation accepts an optional `:cache` keyword:

    * `cache: true` (default) -- read from cache, populate on miss
    * `cache: false` -- bypass cache entirely, no read or write
    * `cache: :refresh` -- skip cache read, force DB fetch, repopulate cache
    * `cache: :cache_only` -- read from cache only, return nil on miss (no DB)
    * `cache: :put_only` -- skip cache read, populate cache from DB result
    * `cache: [ttl: ms]` -- normal mode with a per-query TTL override

  The `:cache` option is stripped before passing to the underlying Ecto
  functions, so it never reaches the database adapter.

  ## How It Works

  This module provides a `__using__/1` macro that:

  1. Calls `use Ecto.Repo` with your options (generating all standard Repo
     functions)
  2. Uses `defoverridable` to make the generated functions overridable
  3. Redefines each function with a cache-aware wrapper

  ### Read Path (`get`, `get!`, `one`, `one!`, `all`)

  1. Check if the schema is cacheable (`FerricStore.Ecto.Cacheable`)
  2. Check cache mode from opts
  3. On cache hit: return deserialized struct(s) directly
  4. On cache miss: call `super/N` (original Ecto implementation),
     then populate cache from the DB result

  ### Write Path (`insert`, `update`, `delete`)

  1. Call `super/N` (original Ecto implementation) first
  2. Only on `{:ok, struct}` result: invalidate entity cache and bump
     the generation counter via `FerricstoreEcto.Cache.invalidate_entity/1`
  3. Return the original result unchanged

  ### Bulk Operations (`insert_all`, `update_all`, `delete_all`)

  1. Call `super/N` first
  2. Bump the generation counter for the table
  3. For `update_all`/`delete_all`: also delete all entity cache entries
     for the table (prefix scan)

  ### Special Behaviors

  * `get_by` / `get_by!` -- Cannot use entity cache (keyed by PK, not by
    arbitrary clauses). Always hits DB, but populates entity cache on result.
  * `get!` -- If the entity cache returns `nil`, falls through to DB
    (because `get!` should raise `Ecto.NoResultsError`, not return nil).
  * `:read_only` strategy -- Updates on `:read_only` schemas log a warning
    but still invalidate the cache.

  ## Examples

      # Normal usage -- caching is transparent
      user = Repo.get(User, 42)

      # Bypass cache for admin/debug operations
      user = Repo.get(User, 42, cache: false)

      # Force refresh after out-of-band DB update
      user = Repo.get(User, 42, cache: :refresh)

      # Custom TTL for this specific read
      user = Repo.get(User, 42, cache: [ttl: :timer.hours(1)])

      # Mutations automatically invalidate
      {:ok, updated} = Repo.update(User.changeset(user, %{name: "Bob"}))
      # Cache entry for user 42 is now deleted; next read repopulates
  """

  @doc false
  defmacro __using__(opts) do
    # credo:disable-for-next-line Credo.Check.Refactor.LongQuoteBlocks
    quote do
      use Ecto.Repo, unquote(opts)

      # Override read and write operations
      defoverridable [
        get: 2, get: 3,
        get!: 2, get!: 3,
        get_by: 2, get_by: 3,
        get_by!: 2, get_by!: 3,
        one: 1, one: 2,
        one!: 1, one!: 2,
        all: 1, all: 2,
        insert: 1, insert: 2,
        insert!: 1, insert!: 2,
        update: 1, update: 2,
        update!: 1, update!: 2,
        delete: 1, delete: 2,
        delete!: 1, delete!: 2,
        insert_all: 2, insert_all: 3,
        update_all: 2, update_all: 3,
        delete_all: 1, delete_all: 2
      ]

      alias FerricstoreEcto.Cache

      # -----------------------------------------------------------------------
      # get / get!
      # -----------------------------------------------------------------------

      def get(queryable, id, opts \\ []) do
        schema = FerricStore.Ecto.CachedRepo.__resolve_schema__(queryable)

        case Cache.cached_get(__MODULE__, schema, id, opts) do
          {:hit, struct} ->
            struct

          :miss ->
            result = super(queryable, id, Cache.strip_cache_opts(opts))
            Cache.populate_entity(schema, result, opts)
            result
        end
      end

      def get!(queryable, id, opts \\ []) do
        schema = FerricStore.Ecto.CachedRepo.__resolve_schema__(queryable)

        case Cache.cached_get(__MODULE__, schema, id, opts) do
          {:hit, nil} ->
            # Cache says nil but get! should raise -- fall through to DB
            super(queryable, id, Cache.strip_cache_opts(opts))

          {:hit, struct} ->
            struct

          :miss ->
            result = super(queryable, id, Cache.strip_cache_opts(opts))
            Cache.populate_entity(schema, result, opts)
            result
        end
      end

      # -----------------------------------------------------------------------
      # get_by / get_by!
      # -----------------------------------------------------------------------

      # get_by cannot use entity cache (keyed by PK, not by arbitrary clauses).
      # Pass through to DB, but populate entity cache on result.
      def get_by(queryable, clauses, opts \\ []) do
        result = super(queryable, clauses, Cache.strip_cache_opts(opts))
        schema = FerricStore.Ecto.CachedRepo.__resolve_schema__(queryable)
        Cache.populate_entity(schema, result, opts)
        result
      end

      def get_by!(queryable, clauses, opts \\ []) do
        result = super(queryable, clauses, Cache.strip_cache_opts(opts))
        schema = FerricStore.Ecto.CachedRepo.__resolve_schema__(queryable)
        Cache.populate_entity(schema, result, opts)
        result
      end

      # -----------------------------------------------------------------------
      # one / one!
      # -----------------------------------------------------------------------

      def one(queryable, opts \\ []) do
        query = Ecto.Queryable.to_query(queryable)

        case Cache.cached_all(__MODULE__, query, opts) do
          {:hit, [pk]} ->
            schema = FerricstoreEcto.QueryHash.primary_schema(query)
            source = schema.__schema__(:source)
            cache_key = Cache.entity_key(source, pk)

            case FerricStore.hgetall(cache_key) do
              {:ok, %{} = fields} when map_size(fields) > 0 ->
                # Entity cache hit -- reconstruct
                pk_type = schema.__schema__(:type, hd(schema.__schema__(:primary_key)))
                pk_val = FerricstoreEcto.FieldCodec.decode(pk_type, pk)
                deserialized = FerricstoreEcto.FieldCodec.deserialize_fields(schema, fields)
                pk_field = hd(schema.__schema__(:primary_key))
                merged = Map.put(deserialized, pk_field, pk_val)
                Ecto.put_meta(struct(schema, merged), state: :loaded)

              _ ->
                # Entity cache miss -- fall through
                result = super(queryable, Cache.strip_cache_opts(opts))
                schema = FerricstoreEcto.QueryHash.primary_schema(query)
                if schema, do: Cache.populate_entity(schema, result, opts)
                result
            end

          {:hit, []} ->
            nil

          {:hit, _pks} ->
            # Multiple results -- let Ecto handle (it may raise)
            super(queryable, Cache.strip_cache_opts(opts))

          :miss ->
            result = super(queryable, Cache.strip_cache_opts(opts))
            query = Ecto.Queryable.to_query(queryable)

            if result do
              Cache.populate_query(__MODULE__, query, [result], opts)
            else
              Cache.populate_query(__MODULE__, query, [], opts)
            end

            result
        end
      end

      def one!(queryable, opts \\ []) do
        result = one(queryable, opts)

        case result do
          nil -> raise Ecto.NoResultsError, queryable: queryable
          _ -> result
        end
      end

      # -----------------------------------------------------------------------
      # all
      # -----------------------------------------------------------------------

      def all(queryable, opts \\ []) do
        query = Ecto.Queryable.to_query(queryable)

        case Cache.cached_all(__MODULE__, query, opts) do
          {:hit, pks} ->
            schema = FerricstoreEcto.QueryHash.primary_schema(query)

            case Cache.resolve_pks(__MODULE__, schema, pks, opts) do
              {:ok, results} ->
                results

              :partial_miss ->
                # Some entities missing from cache -- fall back to DB
                results = super(queryable, Cache.strip_cache_opts(opts))
                Cache.populate_query(__MODULE__, query, results, opts)
                results
            end

          :miss ->
            results = super(queryable, Cache.strip_cache_opts(opts))
            Cache.populate_query(__MODULE__, query, results, opts)
            results
        end
      end

      # -----------------------------------------------------------------------
      # insert / insert!
      # -----------------------------------------------------------------------

      def insert(struct_or_changeset, opts \\ []) do
        case super(struct_or_changeset, Cache.strip_cache_opts(opts)) do
          {:ok, struct} = result ->
            Cache.invalidate_entity(struct)
            result

          error ->
            error
        end
      end

      def insert!(struct_or_changeset, opts \\ []) do
        case insert(struct_or_changeset, opts) do
          {:ok, struct} ->
            struct

          {:error, %Ecto.Changeset{} = changeset} ->
            raise Ecto.InvalidChangesetError, action: :insert, changeset: changeset
        end
      end

      # -----------------------------------------------------------------------
      # update / update!
      # -----------------------------------------------------------------------

      def update(changeset, opts \\ []) do
        case super(changeset, Cache.strip_cache_opts(opts)) do
          {:ok, struct} = result ->
            schema = struct.__struct__

            if Cache.cacheable?(schema) do
              config = schema.__ferricstore_cache_config__()

              if config.strategy == :read_only do
                require Logger
                Logger.warning("FerricStore.Ecto.Cache: update on :read_only entity #{inspect(schema)}")
              end
            end

            Cache.invalidate_entity(struct)
            result

          error ->
            error
        end
      end

      def update!(changeset, opts \\ []) do
        case update(changeset, opts) do
          {:ok, struct} ->
            struct

          {:error, %Ecto.Changeset{} = changeset} ->
            raise Ecto.InvalidChangesetError, action: :update, changeset: changeset
        end
      end

      # -----------------------------------------------------------------------
      # delete / delete!
      # -----------------------------------------------------------------------

      def delete(struct_or_changeset, opts \\ []) do
        case super(struct_or_changeset, Cache.strip_cache_opts(opts)) do
          {:ok, struct} = result ->
            Cache.invalidate_entity(struct)
            result

          error ->
            error
        end
      end

      def delete!(struct_or_changeset, opts \\ []) do
        case delete(struct_or_changeset, opts) do
          {:ok, struct} ->
            struct

          {:error, %Ecto.Changeset{} = changeset} ->
            raise Ecto.InvalidChangesetError, action: :delete, changeset: changeset
        end
      end

      # -----------------------------------------------------------------------
      # insert_all
      # -----------------------------------------------------------------------

      def insert_all(schema_or_source, entries, opts \\ []) do
        result = super(schema_or_source, entries, Cache.strip_cache_opts(opts))

        source = FerricStore.Ecto.CachedRepo.__extract_source__(schema_or_source)
        if source, do: Cache.invalidate_query_caches(source)

        result
      end

      # -----------------------------------------------------------------------
      # update_all
      # -----------------------------------------------------------------------

      def update_all(queryable, updates, opts \\ []) do
        result = super(queryable, updates, Cache.strip_cache_opts(opts))

        query = Ecto.Queryable.to_query(queryable)
        source = FerricstoreEcto.QueryHash.source_table(query)

        if source do
          # Bulk invalidation: delete all entity caches for this table
          FerricStore.Ecto.CachedRepo.__invalidate_all_entities__(source)
          Cache.invalidate_query_caches(source)
        end

        result
      end

      # -----------------------------------------------------------------------
      # delete_all
      # -----------------------------------------------------------------------

      def delete_all(queryable, opts \\ []) do
        result = super(queryable, Cache.strip_cache_opts(opts))

        query = Ecto.Queryable.to_query(queryable)
        source = FerricstoreEcto.QueryHash.source_table(query)

        if source do
          FerricStore.Ecto.CachedRepo.__invalidate_all_entities__(source)
          Cache.invalidate_query_caches(source)
        end

        result
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Compile-time helpers (called from the macro-generated code)
  # ---------------------------------------------------------------------------

  @doc false
  def __resolve_schema__(queryable) when is_atom(queryable), do: queryable

  def __resolve_schema__(%Ecto.Query{from: %Ecto.Query.FromExpr{source: {_table, schema}}})
      when is_atom(schema) and not is_nil(schema) do
    schema
  end

  def __resolve_schema__(_), do: nil

  @doc false
  def __extract_source__(schema) when is_atom(schema) do
    if function_exported?(schema, :__schema__, 1) do
      schema.__schema__(:source)
    else
      nil
    end
  end

  def __extract_source__({source, _schema}) when is_binary(source), do: source
  def __extract_source__(source) when is_binary(source), do: source
  def __extract_source__(_), do: nil

  @doc false
  def __invalidate_all_entities__(source) do
    prefix = "ecto:#{source}:"

    case FerricStore.keys("#{prefix}*") do
      {:ok, keys} ->
        Enum.each(keys, &FerricStore.del/1)

      _ ->
        :ok
    end
  end
end
