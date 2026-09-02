#!/usr/bin/env bash
# Prepares a clean Ubuntu machine to build and run the streaming gateway,
# then builds it and verifies the result.
#
# Idempotent: safe to re-run. Only installs what's actually missing, never
# reinstalls something already present, and never touches Node or Python
# (this project needs neither).
#
# Usage:
#   ./setup.sh            # install deps (asks for sudo if needed) + build + verify
#   ./setup.sh --no-build  # only install system/Rust deps, skip compiling

set -euo pipefail

BUILD=1
for arg in "$@"; do
  case "$arg" in
    --no-build) BUILD=0 ;;
    *)
      echo "unknown argument: $arg" >&2
      exit 1
      ;;
  esac
done

log()  { printf '\033[1;32m==>\033[0m %s\n' "$1"; }
warn() { printf '\033[1;33m!!\033[0m %s\n' "$1" >&2; }
die()  { printf '\033[1;31mXX\033[0m %s\n' "$1" >&2; exit 1; }

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
cd "$SCRIPT_DIR"

# ---------------------------------------------------------------------------
# 1. Detect the OS. This script is written for Ubuntu (and Debian-derivatives
#    close enough to share apt package names); it still tries to proceed
#    elsewhere, but says so.
# ---------------------------------------------------------------------------
if [ -r /etc/os-release ]; then
  . /etc/os-release
  log "Detected: ${PRETTY_NAME:-unknown Linux} (ID=${ID:-unknown})"
  if [ "${ID:-}" != "ubuntu" ] && [ "${ID_LIKE:-}" != *"ubuntu"* ] && [ "${ID_LIKE:-}" != *"debian"* ]; then
    warn "This isn't Ubuntu/Debian -- apt-based dependency install below may not apply."
    warn "Rust itself and the build will still work on any Linux the toolchain supports."
  fi
else
  warn "Cannot read /etc/os-release; assuming a Debian/Ubuntu-like system and continuing."
fi

# ---------------------------------------------------------------------------
# 2. System packages.
#
#    Notably absent: libssl-dev/pkg-config for OpenSSL. This project builds
#    librqbit with its `rust-tls` feature (pure-Rust rustls + ring instead of
#    OpenSSL) specifically so it needs no system TLS library at all. cmake +
#    build-essential + perl are still needed because rustls's crypto backend
#    (aws-lc-rs/ring) compiles a small amount of C/assembly.
# ---------------------------------------------------------------------------
REQUIRED_APT_PACKAGES=(build-essential cmake pkg-config perl git ca-certificates curl)

missing=()
for pkg in "${REQUIRED_APT_PACKAGES[@]}"; do
  dpkg -s "$pkg" >/dev/null 2>&1 || missing+=("$pkg")
done

if [ "${#missing[@]}" -gt 0 ]; then
  if ! command -v apt-get >/dev/null 2>&1; then
    die "Missing packages (${missing[*]}) and no apt-get on this system -- install them manually."
  fi
  log "Installing missing system packages: ${missing[*]}"
  sudo apt-get update -y
  sudo apt-get install -y "${missing[@]}"
else
  log "All required system packages already present: ${REQUIRED_APT_PACKAGES[*]}"
fi

# ---------------------------------------------------------------------------
# 3. Rust toolchain (rustup + stable). Skips install if cargo/rustc already work.
# ---------------------------------------------------------------------------
if command -v cargo >/dev/null 2>&1 && command -v rustc >/dev/null 2>&1; then
  log "Rust already installed: $(rustc --version)"
else
  log "Installing Rust via rustup (stable channel)..."
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
  # shellcheck disable=SC1091
  source "$HOME/.cargo/env"
fi

# Make sure this shell can see cargo even if rustup was already installed in
# a previous, non-interactive session that never sourced its env file.
if ! command -v cargo >/dev/null 2>&1 && [ -f "$HOME/.cargo/env" ]; then
  # shellcheck disable=SC1091
  source "$HOME/.cargo/env"
fi

command -v cargo >/dev/null 2>&1 || die "cargo still not on PATH after install -- open a new shell and re-run this script."

rustup default stable >/dev/null 2>&1 || true
log "Using: $(rustc --version) / $(cargo --version)"

# ---------------------------------------------------------------------------
# 4. Build.
# ---------------------------------------------------------------------------
if [ "$BUILD" -eq 1 ]; then
  log "Building in release mode (this compiles ~250 crates the first time; later builds are incremental)..."
  cargo build --release

  BIN="$SCRIPT_DIR/target/release/streaming-gateway"
  [ -x "$BIN" ] || die "build finished but $BIN is missing -- something went wrong."

  # ---------------------------------------------------------------------------
  # 5. Verify: --help exits via clap before any networking/torrent code runs,
  #    so this proves the binary links and starts without needing a network,
  #    a free port, or root.
  # ---------------------------------------------------------------------------
  log "Verifying the binary starts..."
  "$BIN" --help >/dev/null
  log "OK: $BIN runs."

  # Optional convenience: open the default port on ufw if it's active, so
  # phones/TVs on the LAN can actually reach the gateway. Skipped silently if
  # ufw isn't installed or isn't enabled -- this is a nice-to-have, not a
  # requirement (plenty of desktops run without any local firewall at all).
  if command -v ufw >/dev/null 2>&1 && sudo ufw status 2>/dev/null | head -1 | grep -qi "active"; then
    log "ufw is active -- allowing inbound TCP on 11470 and 8080 (gateway ports) from the LAN."
    sudo ufw allow 11470/tcp >/dev/null || true
    sudo ufw allow 8080/tcp >/dev/null || true
  fi

  echo
  log "Setup complete. Start the gateway with:"
  echo "    $BIN"
  echo "  or, for development iteration:"
  echo "    cargo run --release"
else
  log "Setup complete (--no-build passed, skipped compiling)."
fi
