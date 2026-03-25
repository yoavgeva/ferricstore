# Precompiled NIF Distribution Design

FerricStore ships a single Rust NIF crate (`ferricstore_bitcask`) containing ~22k lines of Rust across the Bitcask storage engine, probabilistic data structures (Bloom, Cuckoo, CMS, TopK, TDigest), HNSW vector index, and io_uring async I/O. Users currently need a full Rust toolchain + 2-5 minutes of compilation. This document designs the precompiled binary distribution so `mix deps.get` just works.

---

## 1. Platform Matrix

| Target triple                    | OS       | Arch    | libc  | Notes                              |
|----------------------------------|----------|---------|-------|------------------------------------|
| `aarch64-apple-darwin`           | macOS    | ARM64   | —     | Apple Silicon M1-M4                |
| `x86_64-apple-darwin`            | macOS    | x86_64  | —     | Intel Macs (still common in CI)    |
| `aarch64-unknown-linux-gnu`      | Linux    | ARM64   | glibc | AWS Graviton, Ampere, Docker ARM  |
| `aarch64-unknown-linux-musl`     | Linux    | ARM64   | musl  | Alpine ARM64 containers            |
| `x86_64-unknown-linux-gnu`       | Linux    | x86_64  | glibc | Most common production target      |
| `x86_64-unknown-linux-musl`      | Linux    | x86_64  | musl  | Alpine, distroless Docker images   |

**Excluded targets (revisit later):**
- `x86_64-pc-windows-msvc` / `x86_64-pc-windows-gnu` — FerricStore uses Unix-specific APIs (mmap, fsync semantics). Windows support is not a priority.
- `arm-unknown-linux-gnueabihf` — 32-bit ARM; negligible Elixir deployment footprint.
- `riscv64gc-unknown-linux-gnu` — emerging; add when demand exists.

**io_uring handling:** The `io-uring` crate dependency is gated behind `[target.'cfg(target_os = "linux")'.dependencies]` in Cargo.toml. macOS builds exclude it entirely at the Cargo dependency resolution level — no conditional compilation flags needed. Linux builds include io_uring automatically.

**NIF version:** Build for NIF `2.16` (OTP 24+). FerricStore requires Elixir 1.19 which mandates OTP 27+, so 2.16 covers all supported runtimes. NIF version 2.15 could be added later if OTP 22/23 support becomes needed.

---

## 2. Rustler Precompiled Integration

### 2.1 Dependency changes in `apps/ferricstore/mix.exs`

```elixir
defmodule Ferricstore.MixProject do
  use Mix.Project

  @version "0.1.0"

  def project do
    [
      app: :ferricstore,
      version: @version,
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      compilers: Mix.compilers(),
      deps: deps(),
      package: package()
    ]
  end

  defp package do
    [
      files: [
        "lib",
        "native/ferricstore_bitcask/.cargo",
        "native/ferricstore_bitcask/src",
        "native/ferricstore_bitcask/Cargo*",
        "checksum-*.exs",
        "mix.exs"
      ],
      licenses: ["MIT"],
      links: %{
        "GitHub" => "https://github.com/YoavGivati/ferricstore"
      }
    ]
  end

  defp deps do
    [
      {:rustler_precompiled, "~> 0.8"},
      {:rustler, "~> 0.37", optional: true},  # only for local development
      {:ra, "~> 2.14"},
      {:libcluster, "~> 3.3"},
      {:telemetry, "~> 1.4"},
      {:jason, "~> 1.4"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:arch_test, "~> 0.1.2", only: [:dev, :test], runtime: false}
    ]
  end
end
```

Key changes:
- `rustler` becomes `optional: true` — Hex won't require users to install it
- `rustler_precompiled` added as a runtime dependency
- `package/0` added with `checksum-*.exs` included in Hex package
- `@version` module attribute for single source of truth

### 2.2 NIF module changes in `lib/ferricstore/bitcask/nif.ex`

```elixir
defmodule Ferricstore.Bitcask.NIF do
  @moduledoc false

  version = Mix.Project.config()[:version]

  use RustlerPrecompiled,
    otp_app: :ferricstore,
    crate: "ferricstore_bitcask",
    base_url:
      "https://github.com/YoavGivati/ferricstore/releases/download/v#{version}",
    force_build: System.get_env("FERRICSTORE_BUILD_NIF") in ["1", "true"],
    targets: ~w(
      aarch64-apple-darwin
      x86_64-apple-darwin
      aarch64-unknown-linux-gnu
      aarch64-unknown-linux-musl
      x86_64-unknown-linux-gnu
      x86_64-unknown-linux-musl
    ),
    nif_versions: ["2.16"],
    version: version

  # All function stubs remain identical — RustlerPrecompiled
  # generates the same :erlang.nif_error/1 fallbacks.
  def new(_path), do: :erlang.nif_error(:nif_not_loaded)
  def get(_store, _key), do: :erlang.nif_error(:nif_not_loaded)
  # ... (all existing stubs unchanged)
end
```

The `use RustlerPrecompiled` macro replaces `use Rustler`. At compile time it:
1. Detects the host OS/arch/NIF version
2. Downloads the matching `.tar.gz` from GitHub Releases
3. Verifies the SHA-256 checksum against `checksum-Elixir.Ferricstore.Bitcask.NIF.exs`
4. Extracts the `.so`/`.dylib` into `priv/native/`
5. Loads it via `:erlang.load_nif/2`

### 2.3 Cargo feature flags for NIF version

Add to `native/ferricstore_bitcask/Cargo.toml`:

```toml
[dependencies]
rustler = { version = "0.37", default-features = false, features = ["derive"] }

[features]
default = ["nif_version_2_16"]
nif_version_2_15 = ["rustler/nif_version_2_15"]
nif_version_2_16 = ["rustler/nif_version_2_16"]
nif_version_2_17 = ["rustler/nif_version_2_17"]
```

The `rustler-precompiled-action` activates the correct feature flag automatically based on the NIF version in the build matrix.

### 2.4 Cargo cross-compilation config

Create `native/ferricstore_bitcask/.cargo/config.toml`:

```toml
[target.'cfg(target_os = "macos")']
rustflags = ["-C", "link-arg=-undefined", "-C", "link-arg=dynamic_lookup"]

[target.x86_64-unknown-linux-musl]
rustflags = ["-C", "target-feature=-crt-static"]

[target.aarch64-unknown-linux-musl]
rustflags = ["-C", "target-feature=-crt-static"]
```

macOS requires `dynamic_lookup` because the NIF links against symbols provided by the BEAM at runtime. musl targets need `-crt-static` disabled to produce a proper shared library.

---

## 3. CI/CD Pipeline

### 3.1 Release workflow: `.github/workflows/release.yml`

```yaml
name: Build precompiled NIFs

on:
  push:
    tags: ["v*"]
  workflow_dispatch:

env:
  NIF_DIRECTORY: "apps/ferricstore/native/ferricstore_bitcask"
  PROJECT_NAME: "ferricstore_bitcask"

permissions:
  contents: write
  id-token: write
  attestations: write

jobs:
  build_release:
    name: NIF ${{ matrix.nif }} - ${{ matrix.target }} (${{ matrix.os }})
    runs-on: ${{ matrix.os }}
    strategy:
      fail-fast: false
      matrix:
        nif: ["2.16"]
        job:
          # macOS
          - { target: aarch64-apple-darwin,           os: macos-14    }
          - { target: x86_64-apple-darwin,            os: macos-13    }
          # Linux glibc
          - { target: aarch64-unknown-linux-gnu,      os: ubuntu-22.04, use-cross: true }
          - { target: x86_64-unknown-linux-gnu,       os: ubuntu-22.04, use-cross: true }
          # Linux musl
          - { target: aarch64-unknown-linux-musl,     os: ubuntu-22.04, use-cross: true }
          - { target: x86_64-unknown-linux-musl,      os: ubuntu-22.04, use-cross: true }

    steps:
      - uses: actions/checkout@v4

      - name: Extract project version
        shell: bash
        run: |
          VERSION=$(sed -n 's/.*@version "\(.*\)"/\1/p' apps/ferricstore/mix.exs)
          echo "PROJECT_VERSION=${VERSION}" >> $GITHUB_ENV

      - name: Install Rust toolchain
        uses: dtolnay/rust-toolchain@stable
        with:
          targets: ${{ matrix.job.target }}

      - name: Cache Rust dependencies
        uses: Swatinem/rust-cache@v2
        with:
          prefix-key: v0-precomp
          shared-key: ${{ matrix.job.target }}-${{ matrix.nif }}
          workspaces: ${{ env.NIF_DIRECTORY }}

      - name: Build precompiled NIF
        uses: philss/rustler-precompiled-action@v1.1.4
        id: build
        with:
          project-name: ${{ env.PROJECT_NAME }}
          project-version: ${{ env.PROJECT_VERSION }}
          target: ${{ matrix.job.target }}
          nif-version: ${{ matrix.nif }}
          use-cross: ${{ matrix.job.use-cross || false }}
          project-dir: ${{ env.NIF_DIRECTORY }}

      - name: Artifact attestation
        uses: actions/attest-build-provenance@v1
        with:
          subject-path: ${{ steps.build.outputs.file-path }}

      - name: Upload artifact
        uses: actions/upload-artifact@v4
        with:
          name: ${{ steps.build.outputs.file-name }}
          path: ${{ steps.build.outputs.file-path }}

      - name: Publish to GitHub Release
        uses: softprops/action-gh-release@v2
        if: startsWith(github.ref, 'refs/tags/')
        with:
          files: ${{ steps.build.outputs.file-path }}
```

### 3.2 How it works

**Trigger:** Pushing a git tag like `v0.1.0` triggers the workflow.

**macOS builds:** Run natively on GitHub's macOS runners — `macos-14` for Apple Silicon (ARM64), `macos-13` for Intel (x86_64). No cross-compilation needed.

**Linux builds:** Use `cross-rs` via Docker containers that include the correct glibc/musl toolchains. The `philss/rustler-precompiled-action` handles installing and invoking `cross` when `use-cross: true`.

**Artifact naming:** The action produces files named:
```
ferricstore_bitcask-nif-2.16-aarch64-apple-darwin.tar.gz
ferricstore_bitcask-nif-2.16-x86_64-unknown-linux-gnu.tar.gz
ferricstore_bitcask-nif-2.16-x86_64-unknown-linux-musl.tar.gz
...
```

Each `.tar.gz` contains the single `.so` or `.dylib` file. The action also outputs the SHA-256 hash of each archive.

**io_uring in cross builds:** The `cross-rs` Docker containers run Linux, so `cfg(target_os = "linux")` resolves correctly and includes the `io-uring` crate. macOS builds exclude it at the Cargo level — no special handling needed.

---

## 4. Cross-Compilation Details

### 4.1 macOS targets

| Target | Runner | Method |
|--------|--------|--------|
| `aarch64-apple-darwin` | `macos-14` (M1) | Native compilation |
| `x86_64-apple-darwin` | `macos-13` (Intel) | Native compilation |

macOS cross-compilation (ARM on Intel or vice versa) is possible but unnecessary — GitHub provides both runner types. Native builds avoid linker issues with the Apple SDK.

### 4.2 Linux targets

| Target | Runner | Method | Container |
|--------|--------|--------|-----------|
| `x86_64-unknown-linux-gnu` | `ubuntu-22.04` | `cross` | `ghcr.io/cross-rs/x86_64-unknown-linux-gnu` |
| `x86_64-unknown-linux-musl` | `ubuntu-22.04` | `cross` | `ghcr.io/cross-rs/x86_64-unknown-linux-musl` |
| `aarch64-unknown-linux-gnu` | `ubuntu-22.04` | `cross` | `ghcr.io/cross-rs/aarch64-unknown-linux-gnu` |
| `aarch64-unknown-linux-musl` | `ubuntu-22.04` | `cross` | `ghcr.io/cross-rs/aarch64-unknown-linux-musl` |

`cross-rs` uses pre-built Docker images with the correct cross-toolchains. The `io-uring` crate compiles inside these containers since the target OS is Linux (even if the build runs x86_64 host targeting aarch64). The `io-uring` crate only uses kernel headers at compile time; the actual io_uring syscalls happen at runtime and require kernel >= 5.1 (with graceful fallback to SyncBackend in FerricStore's Rust code).

### 4.3 glibc compatibility

The `cross-rs` GNU containers use Ubuntu 20.04 base images with glibc 2.31. This covers:
- Ubuntu 20.04+ (glibc 2.31)
- Debian 11+ (glibc 2.31)
- RHEL/CentOS 9+ (glibc 2.34)
- Amazon Linux 2023 (glibc 2.34)

For broader compatibility (glibc 2.17, CentOS 7), a custom Cross.toml could pin an older base image, but CentOS 7 is EOL and OTP 27 itself requires glibc >= 2.17.

---

## 5. Version Management and Release Process

### 5.1 Single source of truth

The version lives in `apps/ferricstore/mix.exs` as `@version "0.1.0"`. The GitHub Actions workflow extracts it with sed. The `base_url` in `nif.ex` interpolates it as `v#{version}`.

### 5.2 Release checklist

```
1. Bump @version in apps/ferricstore/mix.exs
2. Commit: "release: v0.2.0"
3. Tag:    git tag v0.2.0
4. Push:   git push origin main --tags
5. Wait:   CI builds all 6 targets, uploads to GitHub Release "v0.2.0"
6. Download checksums locally:
     mix rustler_precompiled.download Ferricstore.Bitcask.NIF \
       --all --print
7. Commit the generated checksum-Elixir.Ferricstore.Bitcask.NIF.exs
8. Publish: mix hex.publish
```

Step 6 downloads every artifact from the GitHub Release, computes SHA-256, and writes them into the checksum file. This file ships inside the Hex package so end users can verify integrity without network access after the initial download.

### 5.3 Pre-release versions

If `@version` contains a pre-release suffix (e.g., `"0.2.0-dev"`), `RustlerPrecompiled` automatically sets `force_build: true` and compiles from source. This is the correct behavior for development — precompiled binaries only exist for tagged releases.

---

## 6. Fallback to Source Compilation

When precompiled binaries are unavailable (unsupported platform, network error after retries, or explicit opt-in), compilation falls back to Rustler:

```elixir
# User sets env var to force source build:
FERRICSTORE_BUILD_NIF=1 mix deps.compile

# Or uses a pre-release version (auto-detected):
{:ferricstore, "~> 0.2.0-dev"}
```

Requirements for fallback:
- Rust toolchain (rustup + cargo)
- The `rustler` dependency (declared as `optional: true`, so the user must add `{:rustler, "~> 0.37"}` to their own deps if they want source builds)

The `force_build` option in `use RustlerPrecompiled` passes through to Rustler's compilation pipeline, so the existing `Cargo.toml`, `lib.rs`, and all Rust source files must be included in the Hex package (handled by `package[:files]`).

---

## 7. Binary Size Estimates

Current macOS ARM64 release build (with LTO + codegen-units=1): **1.3 MB** uncompressed.

| Target | Estimated .so/.dylib | Estimated .tar.gz |
|--------|---------------------|-------------------|
| `aarch64-apple-darwin` | ~1.3 MB | ~500 KB |
| `x86_64-apple-darwin` | ~1.4 MB | ~550 KB |
| `x86_64-unknown-linux-gnu` | ~1.5 MB | ~600 KB |
| `x86_64-unknown-linux-musl` | ~1.8 MB | ~700 KB |
| `aarch64-unknown-linux-gnu` | ~1.4 MB | ~550 KB |
| `aarch64-unknown-linux-musl` | ~1.7 MB | ~650 KB |

musl builds are slightly larger because musl statically links more of libc. Linux builds include io_uring code, adding ~50-100 KB. These are estimates based on the current 1.3 MB macOS binary; actual sizes will be confirmed after the first CI run.

**Optimization flags (already in Cargo.toml):**
- `opt-level = 3` — maximize runtime performance (not size)
- `lto = "thin"` — cross-crate inlining, moderate size reduction
- `codegen-units = 1` — maximum optimization, slower compile but smaller binary

**Strip debug symbols:** The `rustler-precompiled-action` builds in release mode and the resulting binaries have debug symbols stripped by default in the tar.gz output. If not, add `strip = true` to `[profile.release]`.

---

## 8. Testing Strategy

### 8.1 CI matrix for precompiled binaries

The existing `test.yml` workflow continues to build from source on `ubuntu-latest` (the development/CI path). Add a separate job that validates precompiled binaries work correctly:

```yaml
# In .github/workflows/test.yml, add:
  test_precompiled:
    name: "Test precompiled NIF (${{ matrix.target }})"
    needs: [build_release]  # or run after release is published
    runs-on: ${{ matrix.os }}
    strategy:
      matrix:
        include:
          - { os: macos-14,      target: aarch64-apple-darwin }
          - { os: macos-13,      target: x86_64-apple-darwin }
          - { os: ubuntu-22.04,  target: x86_64-unknown-linux-gnu }
    steps:
      - uses: actions/checkout@v4
      - uses: erlef/setup-beam@v1
        with:
          elixir-version: "1.19"
          otp-version: "28"
      # NO Rust toolchain installed — this validates precompiled path
      - run: mix deps.get
      - run: mix test --trace
```

The key validation: **no Rust toolchain is installed**. If tests pass, the precompiled binary was downloaded and loaded successfully.

### 8.2 What to test per platform

- **Basic NIF loading:** `Ferricstore.Bitcask.NIF.new/1` returns a resource without crash
- **Bitcask operations:** put/get/delete round-trip
- **Probabilistic structures:** Bloom, Cuckoo, CMS, TopK, TDigest create/query
- **HNSW:** Vector insert + search
- **io_uring (Linux only):** The existing `linux_io_uring` tagged tests verify io_uring works when the kernel supports it; `SyncBackend` fallback is tested on macOS
- **Zero-copy binaries:** `get_zero_copy` returns valid BEAM binaries backed by Rust memory

### 8.3 Checksum integrity

The `mix rustler_precompiled.download --all --print` command verifies that every target's artifact exists in the GitHub Release and that the checksums match. Run this as part of the release process before `mix hex.publish`.

### 8.4 Smoke test for users

After publishing to Hex, test the full user experience in a fresh project:

```bash
mix new test_ferric --no-ecto
cd test_ferric
# Add {:ferricstore, "~> 0.1"} to mix.exs deps
mix deps.get    # should download precompiled NIF
mix compile     # should NOT invoke cargo
iex -S mix -e 'Ferricstore.Bitcask.NIF.new("/tmp/test")'
```

---

## 9. Umbrella Project Considerations

FerricStore is an umbrella (`apps_path: "apps"`) with the NIF in `apps/ferricstore/`. This affects several paths:

- **NIF source:** `apps/ferricstore/native/ferricstore_bitcask/`
- **Cargo config:** `apps/ferricstore/native/ferricstore_bitcask/.cargo/config.toml`
- **priv/native:** `apps/ferricstore/priv/native/ferricstore_bitcask.so`
- **CI workflow `NIF_DIRECTORY`:** `apps/ferricstore/native/ferricstore_bitcask`
- **Hex package:** Only the `ferricstore` app is published, not the umbrella root

When publishing to Hex, `mix hex.publish` is run from `apps/ferricstore/`, which sees its own `mix.exs` and `package[:files]`. The umbrella root and `ferricstore_server` app are not included.

---

## 10. Migration Plan

### Phase 1: Infrastructure (no user-facing changes)
1. Create `apps/ferricstore/native/ferricstore_bitcask/.cargo/config.toml` with cross-compilation flags
2. Add NIF version feature flags to `Cargo.toml`
3. Add `strip = true` to `[profile.release]` in `Cargo.toml`
4. Create `.github/workflows/release.yml`
5. Test the CI pipeline by pushing a test tag (e.g., `v0.0.1-rc.1`)
6. Verify all 6 artifacts appear in the GitHub Release

### Phase 2: Integration
7. Add `rustler_precompiled` dependency to `mix.exs`
8. Change `rustler` to `optional: true`
9. Convert `nif.ex` from `use Rustler` to `use RustlerPrecompiled`
10. Add `package/0` config to `mix.exs`
11. Run `mix rustler_precompiled.download Ferricstore.Bitcask.NIF --all --print`
12. Commit `checksum-Elixir.Ferricstore.Bitcask.NIF.exs`

### Phase 3: Publish
13. Tag `v0.1.0`, push, wait for CI
14. Re-run checksum download (in case binaries changed)
15. `mix hex.publish` from `apps/ferricstore/`
16. Smoke test from a fresh project on macOS and Linux

### Phase 4: Validate
17. Add precompiled NIF test job to `test.yml` (no Rust toolchain)
18. Monitor Hex download stats and GitHub issues for platform gaps
19. Consider adding Windows targets if demand arises
