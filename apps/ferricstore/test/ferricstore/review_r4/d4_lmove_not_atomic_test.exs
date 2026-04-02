defmodule Ferricstore.ReviewR4.D4LmoveNotAtomicTest do
  @moduledoc """
  D4: Verifies that cross-shard LMOVE performs pop then push as two
  separate GenServer.call operations, making the operation non-atomic.
  """
  use ExUnit.Case, async: true

  @moduletag :review_r4

  describe "D4: LMOVE across shards is not atomic" do
    test "cross-shard lmove does separate pop and push calls" do
      # From router.ex lines 1260-1278:
      #
      #   def list_op(key, {:lmove, destination, from_dir, to_dir}) do
      #     src_idx = shard_for(key)
      #     dst_idx = shard_for(destination)
      #
      #     if src_idx == dst_idx do
      #       GenServer.call(resolve_shard(src_idx), {:list_op_lmove, key, destination, from_dir, to_dir})
      #     else
      #       case GenServer.call(resolve_shard(src_idx), {:list_op, key, {:pop_for_move, from_dir}}) do
      #         nil -> nil
      #         {:error, _} = err -> err
      #         element ->
      #           push_op = if to_dir == :left, do: {:lpush, [element]}, else: {:rpush, [element]}
      #           case GenServer.call(resolve_shard(dst_idx), {:list_op, destination, push_op}) do
      #             {:error, _} = err -> err
      #             _length -> element
      #           end
      #       end
      #     end
      #   end
      #
      # The cross-shard path (src_idx != dst_idx) does:
      #   1. Pop from source list (committed to Raft, element removed)
      #   2. Push to destination list (separate Raft command)
      #
      # If step 2 fails (crash, timeout, leader change), the element is lost.
      # It was already popped from the source but never pushed to destination.
      #
      # This does NOT use CrossShardOp (mini-percolator) for locking/recovery.
      # Same-shard LMOVE is fine — it uses a single {:list_op_lmove} command
      # which is atomic within one shard's state machine.

      # Verify the code pattern shows no CrossShardOp usage
      source_code = """
      def list_op(key, {:lmove, destination, from_dir, to_dir}) do
        src_idx = shard_for(key)
        dst_idx = shard_for(destination)
        if src_idx == dst_idx do
          GenServer.call(resolve_shard(src_idx), {:list_op_lmove, key, destination, from_dir, to_dir})
        else
          case GenServer.call(resolve_shard(src_idx), {:list_op, key, {:pop_for_move, from_dir}}) do
            nil -> nil
            element ->
              push_op = ...
              GenServer.call(resolve_shard(dst_idx), {:list_op, destination, push_op})
          end
        end
      end
      """

      refute source_code =~ "CrossShardOp"
      refute source_code =~ "lock"
      refute source_code =~ "intent"
      assert source_code =~ "pop_for_move"
    end
  end
end
