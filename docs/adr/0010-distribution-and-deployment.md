# ADR 0010: Distribution and Deployment

## Status
Accepted

## Context
Stophammer must be straightforward to deploy on any Linux server without requiring operators to install a language runtime, Docker, or a package manager. The target operator audience ranges from self-hosters on a Raspberry Pi to cloud VMs running arbitrary Linux distributions. The binary already compiles to a fully static musl executable (see ADR-0002); this ADR records the decisions around how those binaries are built, distributed, and run as a system service.

## Decisions

### 1. musl static binaries as the distribution unit

Rust's `x86_64-unknown-linux-musl` and `aarch64-unknown-linux-musl` targets produce fully static executables that carry zero shared-library dependencies. The resulting binary runs on any Linux system regardless of glibc version, distribution, or whether a C runtime is present at all. This satisfies the zero-dependency deployment requirement without needing a container runtime or a distribution-specific package.

The `rusqlite` dependency is compiled with the `bundled` feature, which statically links SQLite into the binary rather than linking against a system `libsqlite3`. This is the only dependency that might otherwise introduce a system requirement.

### 2. GitHub Actions + `cross` for aarch64 cross-compilation

GitHub-hosted `ubuntu-latest` runners are x86_64. Rather than provisioning a dedicated ARM builder or maintaining a QEMU binfmt layer, aarch64 binaries are produced using the [`cross`](https://github.com/cross-rs/cross) crate, which wraps `cargo build` with a pre-configured Docker image that includes an aarch64 musl cross-toolchain. This keeps CI simple: no self-hosted runners, no custom Docker registries, and no per-arch matrix complexity beyond the two jobs.

x86_64 binaries are built natively on the runner with `musl-tools` installed, which is faster and avoids the Docker pull overhead for the common case.

Releases are triggered by pushing a `v*` tag. Both binaries are uploaded as workflow artifacts, then a final `release` job downloads them and creates a GitHub Release with `softprops/action-gh-release`, attaching the binaries as `stophammer-linux-x86_64` and `stophammer-linux-aarch64`.

### 3. curl-pipe install script

The install script follows the BOINC / Homebrew drop-in pattern:

```
curl -fsSL https://raw.githubusercontent.com/stophammer/stophammer/main/install.sh | sh
```

It uses only POSIX tools available on every Linux distribution (`curl`, `uname`, `chmod`, `mv`, `sed`, `tr`) — no `jq`, no Python, no package manager. Architecture detection maps `uname -m` output to the correct release asset name. The latest release tag is fetched from the GitHub Releases API using plain text parsing. A `--dry-run` flag prints what would happen without making any changes, which is useful for auditing before piping to a shell.

### 4. systemd unit for production deployments

`stophammer.service` runs the binary as a dedicated `stophammer` user with:

- `EnvironmentFile=/etc/stophammer/env` — secrets stay out of the unit file and process list
- `NoNewPrivileges=true` — the process cannot escalate privileges
- `ProtectSystem=strict` — the filesystem is read-only except for paths explicitly listed
- `ReadWritePaths=/var/lib/stophammer` — the database and signing key directory

This is standard Linux service hardening with no unusual tooling required. Any distribution shipping systemd (which covers the vast majority of server Linux deployments) can manage the service with `systemctl`.

An `stophammer.env.example` file documents all environment variables with their defaults and is intended to be copied to `/etc/stophammer/env` and filled in before enabling the unit.

## Consequences

- Operators need only `curl` to install stophammer on a Linux machine. No Docker, no runtime, no package manager.
- Binary sizes are larger than dynamically-linked equivalents because libc and SQLite are bundled, but this is an acceptable trade-off for zero deployment dependencies.
- The GitHub Actions pipeline handles both architectures without requiring ARM hardware or a self-hosted runner.
- The `cross`-based aarch64 build pulls a Docker image on each CI run; build times are longer than native compilation but acceptable for a release pipeline triggered only on tags.
- The systemd unit enforces a narrow filesystem footprint; operators who want to store data in non-default paths must update `ReadWritePaths` in the unit or use an override file.
- Operators running non-systemd init systems (OpenRC, runit, s6) must write their own service wrapper; this is out of scope for this ADR.
