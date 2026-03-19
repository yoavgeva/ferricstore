defmodule Ferricstore.Commands.Vector do
  @moduledoc """
  Handles vector search commands for similarity search over dense float vectors.

  Implements 8 commands for creating and managing vector collections, adding
  and retrieving vectors, performing nearest-neighbor search, and collection
  lifecycle management.

  ## Supported Commands

    * `VCREATE collection dims metric [M m] [EF ef]` - create a vector collection
    * `VADD collection key vector...` - add/overwrite a vector
    * `VGET collection key` - retrieve a vector
    * `VDEL collection key` - delete a vector
    * `VSEARCH collection vector... TOP k [EF ef]` - brute-force nearest neighbor search
    * `VINFO collection` - collection metadata
    * `VLIST [MATCH pattern] [COUNT n]` - list collections
    * `VEVICT collection` - evict from page cache (no-op in v1)

  ## Storage Layout

  Collection metadata and vector data are stored in the shared Bitcask:

    * Collection metadata: `VM:collection` -> serialized `{dims, metric, m, ef}`
      (stored as a regular key via `store.put`)
    * Vector data: `V:collection\\0key` -> serialized list of floats
      (stored via compound key callbacks routed by collection name)

  Both `VM:` and `V:` prefixes are registered as internal keys in
  `CompoundKey.internal_key?/1` so they are excluded from user-facing
  KEYS and DBSIZE results.

  ## Distance Metrics

    * `:cosine` - `1 - cos(a, b)` (0 = identical, 2 = opposite)
    * `:l2` - squared Euclidean distance (sum of squared differences)
    * `:inner_product` - `1 - dot(a, b)` (lower = more similar)

  ## VSEARCH Strategy (v1)

  Brute-force scan over all vectors in the collection. Adequate for collections
  with fewer than ~100K vectors. A future version will add HNSW indexing via
  a Rust NIF for larger collections.
  """

  @default_m 16
  @default_ef 128

  @valid_metrics ~w(cosine l2 inner_product)

  @doc """
  Handles a vector command.

  ## Parameters

    - `cmd` - Uppercased command name (e.g. `"VCREATE"`, `"VSEARCH"`)
    - `args` - List of string arguments
    - `store` - Injected store map with `get`, `put`, `exists?`, `keys`,
      `compound_get`, `compound_put`, `compound_scan`, `compound_count`,
      `compound_delete` callbacks

  ## Returns

  Plain Elixir term: `:ok`, `nil`, integer, list, or `{:error, message}`.
  """
  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  # ---------------------------------------------------------------------------
  # VCREATE collection dims metric [M m] [EF ef]
  # ---------------------------------------------------------------------------

  def handle("VCREATE", [collection, dims_str, metric_str | opts], store) do
    with {:ok, dims} <- parse_pos_integer(dims_str, "dimension"),
         {:ok, metric} <- parse_metric(metric_str),
         {:ok, m, ef} <- parse_hnsw_opts(opts) do
      mk = meta_key(collection)

      if store.exists?.(mk) do
        {:error, "ERR collection '#{collection}' already exists"}
      else
        meta = :erlang.term_to_binary({dims, metric, m, ef})
        store.put.(mk, meta, 0)
        :ok
      end
    end
  end

  def handle("VCREATE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'vcreate' command"}
  end

  # ---------------------------------------------------------------------------
  # VADD collection key f32 f32 ...
  # ---------------------------------------------------------------------------

  def handle("VADD", [collection, key | components], store) when components != [] do
    with {:ok, {dims, _metric, _m, _ef}} <- get_collection_meta(collection, store),
         {:ok, floats} <- parse_float_vector(components) do
      if length(floats) != dims do
        {:error, "ERR vector dimension mismatch: expected #{dims}, got #{length(floats)}"}
      else
        vec_key = vector_key(collection, key)
        encoded = :erlang.term_to_binary(floats)
        store.compound_put.(collection, vec_key, encoded, 0)
        :ok
      end
    end
  end

  def handle("VADD", _args, _store) do
    {:error, "ERR wrong number of arguments for 'vadd' command"}
  end

  # ---------------------------------------------------------------------------
  # VGET collection key
  # ---------------------------------------------------------------------------

  def handle("VGET", [collection, key], store) do
    with {:ok, _meta} <- get_collection_meta(collection, store) do
      vec_key = vector_key(collection, key)

      case store.compound_get.(collection, vec_key) do
        nil -> nil
        encoded -> encoded |> :erlang.binary_to_term() |> format_vector()
      end
    end
  end

  def handle("VGET", args, _store) when length(args) != 2 do
    {:error, "ERR wrong number of arguments for 'vget' command"}
  end

  # ---------------------------------------------------------------------------
  # VDEL collection key
  # ---------------------------------------------------------------------------

  def handle("VDEL", [collection, key], store) do
    with {:ok, _meta} <- get_collection_meta(collection, store) do
      vec_key = vector_key(collection, key)

      case store.compound_get.(collection, vec_key) do
        nil ->
          0

        _value ->
          store.compound_delete.(collection, vec_key)
          1
      end
    end
  end

  def handle("VDEL", args, _store) when length(args) != 2 do
    {:error, "ERR wrong number of arguments for 'vdel' command"}
  end

  # ---------------------------------------------------------------------------
  # VSEARCH collection f32... TOP k [EF ef]
  # ---------------------------------------------------------------------------

  def handle("VSEARCH", args, store) when is_list(args) do
    with {:ok, collection, query_floats, k, _ef_search} <- parse_vsearch_args(args),
         {:ok, {dims, metric, _m, _ef}} <- get_collection_meta(collection, store) do
      if length(query_floats) != dims do
        {:error, "ERR vector dimension mismatch: expected #{dims}, got #{length(query_floats)}"}
      else
        prefix = vector_prefix(collection)
        entries = store.compound_scan.(collection, prefix)

        entries
        |> Enum.map(fn {sub_key, encoded} ->
          vec = :erlang.binary_to_term(encoded)
          dist = compute_distance(metric, query_floats, vec)
          {sub_key, dist}
        end)
        |> Enum.sort_by(fn {_key, dist} -> dist end)
        |> Enum.take(k)
        |> Enum.flat_map(fn {key, dist} ->
          [key, format_float(dist)]
        end)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # VINFO collection
  # ---------------------------------------------------------------------------

  def handle("VINFO", [collection], store) do
    with {:ok, {dims, metric, m, ef}} <- get_collection_meta(collection, store) do
      prefix = vector_prefix(collection)
      count = store.compound_count.(collection, prefix)

      [
        "collection", collection,
        "dims", dims,
        "metric", Atom.to_string(metric),
        "m", m,
        "ef", ef,
        "vector_count", count
      ]
    end
  end

  def handle("VINFO", args, _store) when length(args) != 1 do
    {:error, "ERR wrong number of arguments for 'vinfo' command"}
  end

  # ---------------------------------------------------------------------------
  # VLIST [MATCH pattern] [COUNT n]
  # ---------------------------------------------------------------------------

  def handle("VLIST", args, store) do
    with {:ok, match_pattern, count} <- parse_vlist_opts(args) do
      # Scan all keys and filter for vector collection metadata keys.
      # VM: keys are stored as regular keys, so store.keys.() returns them.
      prefix = "VM:"

      collection_names =
        store.keys.()
        |> Enum.filter(&String.starts_with?(&1, prefix))
        |> Enum.map(&String.replace_leading(&1, prefix, ""))
        |> Enum.sort()

      # Apply MATCH filter
      filtered =
        case match_pattern do
          nil -> collection_names
          pattern -> Enum.filter(collection_names, &glob_match?(&1, pattern))
        end

      # Apply COUNT limit
      case count do
        nil -> filtered
        n -> Enum.take(filtered, n)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # VEVICT collection
  # ---------------------------------------------------------------------------

  def handle("VEVICT", [collection], store) do
    with {:ok, _meta} <- get_collection_meta(collection, store) do
      # No-op in v1 (no mmap to evict)
      :ok
    end
  end

  def handle("VEVICT", args, _store) when length(args) != 1 do
    {:error, "ERR wrong number of arguments for 'vevict' command"}
  end

  # ===========================================================================
  # Private -- Key construction
  # ===========================================================================

  @separator <<0>>

  # Collection metadata key: VM:collection (stored as a regular key)
  defp meta_key(collection), do: "VM:" <> collection

  # Vector data key: V:collection\0key (stored as a compound key)
  defp vector_key(collection, key), do: "V:" <> collection <> @separator <> key

  # Scan prefix for all vectors in a collection: V:collection\0
  defp vector_prefix(collection), do: "V:" <> collection <> @separator

  # ===========================================================================
  # Private -- Collection metadata
  # ===========================================================================

  defp get_collection_meta(collection, store) do
    mk = meta_key(collection)

    case store.get.(mk) do
      nil ->
        {:error, "ERR collection '#{collection}' not found"}

      encoded ->
        {:ok, :erlang.binary_to_term(encoded)}
    end
  end

  # ===========================================================================
  # Private -- Distance computation
  # ===========================================================================

  defp compute_distance(:cosine, a, b) do
    {dot, norm_a, norm_b} = dot_and_norms(a, b)
    denom = :math.sqrt(norm_a) * :math.sqrt(norm_b)

    if denom == 0.0 do
      # If either vector is zero, cosine similarity is undefined.
      # Return 1.0 (maximally dissimilar) as a safe fallback.
      1.0
    else
      # Clamp to [-1, 1] to handle floating point rounding
      similarity = max(-1.0, min(1.0, dot / denom))
      1.0 - similarity
    end
  end

  defp compute_distance(:l2, a, b) do
    Enum.zip(a, b)
    |> Enum.reduce(0.0, fn {ai, bi}, acc ->
      diff = ai - bi
      acc + diff * diff
    end)
  end

  defp compute_distance(:inner_product, a, b) do
    dot =
      Enum.zip(a, b)
      |> Enum.reduce(0.0, fn {ai, bi}, acc -> acc + ai * bi end)

    1.0 - dot
  end

  defp dot_and_norms(a, b) do
    Enum.zip(a, b)
    |> Enum.reduce({0.0, 0.0, 0.0}, fn {ai, bi}, {dot, na, nb} ->
      {dot + ai * bi, na + ai * ai, nb + bi * bi}
    end)
  end

  # ===========================================================================
  # Private -- Argument parsing
  # ===========================================================================

  defp parse_pos_integer(str, name) do
    case Integer.parse(str) do
      {n, ""} when n > 0 -> {:ok, n}
      {_n, ""} -> {:error, "ERR #{name} must be a positive integer"}
      _ -> {:error, "ERR #{name} must be a positive integer"}
    end
  end

  defp parse_metric(str) do
    metric = String.downcase(str)

    if metric in @valid_metrics do
      {:ok, String.to_existing_atom(metric)}
    else
      {:error, "ERR invalid metric '#{str}', must be one of: cosine, l2, inner_product"}
    end
  end

  defp parse_hnsw_opts(opts), do: parse_hnsw_opts(opts, @default_m, @default_ef)

  defp parse_hnsw_opts([], m, ef), do: {:ok, m, ef}

  defp parse_hnsw_opts(["M", m_str | rest], _m, ef) do
    case parse_pos_integer(m_str, "M") do
      {:ok, m} -> parse_hnsw_opts(rest, m, ef)
      error -> error
    end
  end

  defp parse_hnsw_opts(["EF", ef_str | rest], m, _ef) do
    case parse_pos_integer(ef_str, "EF") do
      {:ok, ef} -> parse_hnsw_opts(rest, m, ef)
      error -> error
    end
  end

  defp parse_hnsw_opts([unknown | _rest], _m, _ef) do
    {:error, "ERR syntax error, unknown option '#{unknown}'"}
  end

  defp parse_float_vector(components) do
    Enum.reduce_while(components, {:ok, []}, fn str, {:ok, acc} ->
      case parse_float_component(str) do
        {:ok, f} -> {:cont, {:ok, [f | acc]}}
        :error -> {:halt, {:error, "ERR value '#{str}' is not a valid float"}}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      error -> error
    end
  end

  defp parse_float_component(str) when is_binary(str) do
    # Try integer first (e.g. "3"), then float (e.g. "3.14")
    case Integer.parse(str) do
      {i, ""} ->
        {:ok, i * 1.0}

      _ ->
        case Float.parse(str) do
          {f, ""} -> {:ok, f}
          _ -> :error
        end
    end
  end

  defp parse_float_component(_), do: :error

  defp parse_vsearch_args(args) when length(args) < 4 do
    {:error, "ERR wrong number of arguments for 'vsearch' command"}
  end

  defp parse_vsearch_args(args) do
    # Find the position of "TOP" to split vector components from k and options.
    # Format: VSEARCH collection f32... TOP k [EF ef]
    top_idx = Enum.find_index(args, fn a -> String.upcase(a) == "TOP" end)

    if top_idx == nil or top_idx < 2 do
      {:error, "ERR wrong number of arguments for 'vsearch' command"}
    else
      [collection | rest] = args
      # Vector components are from index 0 to (top_idx - 2) in `rest`
      vec_strs = Enum.slice(rest, 0, top_idx - 1)
      after_top = Enum.drop(rest, top_idx)

      case after_top do
        [k_str | ef_opts] ->
          with {:ok, floats} <- parse_float_vector(vec_strs),
               {:ok, k} <- parse_k(k_str),
               {:ok, ef_search} <- parse_vsearch_ef_opts(ef_opts) do
            {:ok, collection, floats, k, ef_search}
          end

        [] ->
          {:error, "ERR wrong number of arguments for 'vsearch' command"}
      end
    end
  end

  defp parse_k(str) do
    case Integer.parse(str) do
      {k, ""} when k > 0 -> {:ok, k}
      {_k, ""} -> {:error, "ERR k must be a positive integer"}
      _ -> {:error, "ERR k must be a positive integer"}
    end
  end

  defp parse_vsearch_ef_opts([]), do: {:ok, nil}

  defp parse_vsearch_ef_opts(["EF", ef_str]) do
    case parse_pos_integer(ef_str, "EF") do
      {:ok, ef} -> {:ok, ef}
      error -> error
    end
  end

  defp parse_vsearch_ef_opts(_), do: {:error, "ERR syntax error in VSEARCH options"}

  defp parse_vlist_opts(args), do: parse_vlist_opts(args, nil, nil)

  defp parse_vlist_opts([], match, count), do: {:ok, match, count}

  defp parse_vlist_opts(["MATCH", pattern | rest], _match, count) do
    parse_vlist_opts(rest, pattern, count)
  end

  defp parse_vlist_opts(["COUNT", count_str | rest], match, _count) do
    case Integer.parse(count_str) do
      {n, ""} when n > 0 -> parse_vlist_opts(rest, match, n)
      {_n, ""} -> {:error, "ERR COUNT must be a positive integer"}
      _ -> {:error, "ERR COUNT must be a positive integer"}
    end
  end

  defp parse_vlist_opts([unknown | _rest], _match, _count) do
    {:error, "ERR syntax error, unknown option '#{unknown}'"}
  end

  # ===========================================================================
  # Private -- Output formatting
  # ===========================================================================

  defp format_vector(floats) do
    Enum.map(floats, &format_float/1)
  end

  defp format_float(f) when is_float(f) do
    # Use :short option to produce the shortest string that round-trips back
    # to the same float value. This gives "3.14" instead of "3.14000000000000012".
    :erlang.float_to_binary(f, [:short])
  end

  # ===========================================================================
  # Private -- Glob pattern matching
  # ===========================================================================

  defp glob_match?(string, pattern) do
    regex_str =
      pattern
      |> Regex.escape()
      |> String.replace("\\*", ".*")
      |> String.replace("\\?", ".")
      |> then(&("^" <> &1 <> "$"))

    case Regex.compile(regex_str) do
      {:ok, regex} -> Regex.match?(regex, string)
      _ -> false
    end
  end
end
