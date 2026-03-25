defmodule FerricstoreEcto.QueryHash do
  @moduledoc """
  Computes a deterministic hash from an `Ecto.Query` AST and its bound
  parameters.

  The hash is used as part of the query cache key. Two structurally
  identical queries with the same parameters produce the same hash.
  Different parameters or different query shapes produce different hashes.

  ## How Hashing Works

  The hash considers the following query components:

  * FROM source (table name + schema module)
  * WHERE clauses (expressions + bound parameters)
  * ORDER BY clauses
  * LIMIT and OFFSET
  * DISTINCT
  * GROUP BY and HAVING
  * SELECT expression

  All components are normalized into a plain map, then hashed with
  `:erlang.phash2/1` (32-bit hash space). This is fast (~1 microsecond)
  and sufficient for practical query volumes -- collisions are theoretically
  possible but extremely unlikely.

  ## Helper Functions

  * `source_table/1` -- Extracts the primary table name from a query
  * `has_order_by?/1` -- Checks if the query has ORDER BY (determines
    whether query cache uses LIST or SET storage)
  * `primary_schema/1` -- Extracts the schema module from the FROM source
  """

  @doc """
  Returns a deterministic integer hash for the given `Ecto.Query`.

  The hash considers: source tables, WHERE clauses, ORDER BY, LIMIT,
  OFFSET, DISTINCT, GROUP BY, HAVING, SELECT, and all bound parameters.

  ## Examples

      iex> query = from(u in "users", where: u.active == true)
      iex> is_integer(FerricstoreEcto.QueryHash.hash(query))
      true

  """
  @spec hash(Ecto.Query.t()) :: non_neg_integer()
  def hash(%Ecto.Query{} = query) do
    normalized = %{
      from: normalize_from(query.from),
      wheres: Enum.map(query.wheres || [], &normalize_expr/1),
      order_bys: Enum.map(query.order_bys || [], &normalize_expr/1),
      limit: normalize_expr(query.limit),
      offset: normalize_expr(query.offset),
      distinct: normalize_expr(query.distinct),
      group_bys: Enum.map(query.group_bys || [], &normalize_expr/1),
      havings: Enum.map(query.havings || [], &normalize_expr/1),
      select: normalize_select(query.select)
    }

    :erlang.phash2(normalized)
  end

  @doc """
  Extracts the primary source table name from an `Ecto.Query`.

  Returns the table name as a string, or `nil` if the source cannot
  be resolved (e.g., subquery in FROM position).
  """
  @spec source_table(Ecto.Query.t()) :: String.t() | nil
  def source_table(%Ecto.Query{from: %Ecto.Query.FromExpr{source: {table, _schema}}})
      when is_binary(table) do
    table
  end

  def source_table(_query), do: nil

  @doc """
  Returns `true` if the query has an ORDER BY clause.
  """
  @spec has_order_by?(Ecto.Query.t()) :: boolean()
  def has_order_by?(%Ecto.Query{order_bys: [_ | _]}), do: true
  def has_order_by?(_query), do: false

  @doc """
  Extracts the primary schema module from the query's FROM source.

  Returns `nil` if the query uses a bare table string (no schema).
  """
  @spec primary_schema(Ecto.Query.t()) :: module() | nil
  def primary_schema(%Ecto.Query{from: %Ecto.Query.FromExpr{source: {_table, schema}}})
      when is_atom(schema) and not is_nil(schema) do
    schema
  end

  def primary_schema(_query), do: nil

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp normalize_from(nil), do: nil

  defp normalize_from(%Ecto.Query.FromExpr{source: {table, schema}}) do
    {table, schema}
  end

  defp normalize_from(%Ecto.Query.FromExpr{} = from) do
    # Subquery or other complex source -- hash the whole struct
    :erlang.phash2(from)
  end

  defp normalize_expr(nil), do: nil

  defp normalize_expr(%{expr: expr, params: params}) do
    {expr, normalize_params(params)}
  end

  defp normalize_expr(other), do: :erlang.phash2(other)

  defp normalize_params(nil), do: []

  defp normalize_params(params) when is_list(params) do
    Enum.map(params, fn
      {val, type} -> {val, type}
      val -> val
    end)
  end

  defp normalize_select(nil), do: nil

  defp normalize_select(%Ecto.Query.SelectExpr{expr: expr, params: params}) do
    {expr, normalize_params(params)}
  end

  defp normalize_select(other), do: :erlang.phash2(other)
end
