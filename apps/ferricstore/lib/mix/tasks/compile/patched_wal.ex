defmodule Mix.Tasks.Compile.PatchedWal do
  @moduledoc false
  @shortdoc "Compiles the patched ra_log_wal.erl to .beam"

  use Mix.Task.Compiler

  @impl true
  def run(_argv) do
    # Find the ferricstore app's priv directory — works in both umbrella and standalone
    source_candidates = [
      Path.join(File.cwd!(), "priv/patched/ra_log_wal.erl"),
      Path.join(File.cwd!(), "apps/ferricstore/priv/patched/ra_log_wal.erl")
    ]

    source = Enum.find(source_candidates, &File.exists?/1)

    if source == nil do
      {:ok, []}
    else
      # Write .beam next to .erl in the source tree (priv/ is copied to _build by Mix)
      target = Path.join(Path.dirname(source), "ra_log_wal.beam")

      if needs_recompile?(source, target) do
        compile_wal(source, target)
      else
        {:noop, []}
      end
    end
  end

  @impl true
  def manifests, do: []

  @impl true
  def clean do
    priv_dir = Path.join(Mix.Project.app_path(), "priv")
    File.rm(Path.join(priv_dir, "patched/ra_log_wal.beam"))
    :ok
  end

  defp needs_recompile?(source, target) do
    not File.exists?(target) or
      File.stat!(source).mtime > File.stat!(target).mtime
  end

  defp compile_wal(source, target) do
    ra_src_dir = find_ra_src_dir()

    compile_opts = [
      :binary,
      :return_errors,
      :return_warnings,
      {:i, to_charlist(ra_src_dir)}
    ]

    case :compile.file(to_charlist(source), compile_opts) do
      {:ok, :ra_log_wal, binary, _warnings} ->
        File.mkdir_p!(Path.dirname(target))
        File.write!(target, binary)
        Mix.shell().info("Compiled patched ra_log_wal.beam")
        {:ok, []}

      {:error, errors, _warnings} ->
        Mix.shell().error("Failed to compile patched ra_log_wal: #{inspect(errors)}")
        {:error, []}
    end
  end

  defp find_ra_src_dir do
    # Always use deps path in Mix context — :code.lib_dir(:ra, :src) is
    # deprecated in OTP 28 and may not include source files anyway.
    deps_path = Mix.Project.deps_path()
    Path.join(deps_path, "ra/src")
  end
end
