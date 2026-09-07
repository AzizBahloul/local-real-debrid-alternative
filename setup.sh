#!/usr/bin/env bash
# Prepares a clean Ubuntu machine to build and run the streaming gateway,
# then builds it and verifies the result.
#
# Idempotent: safe to re-run. Only installs what's actually missing, never
# reinstalls something already present, and never touches Node or Python
# (this project needs neither).
#
# Usage:
#   ./setup.sh              # install server deps (asks for sudo if needed) + build + verify
#   ./setup.sh --gui        # also build the desktop GUI launcher and package a .deb
#   ./setup.sh --gui --service  # ...and run the gateway as an always-on user service
#   ./setup.sh --no-build   # only install system/Rust deps, skip compiling
#
# The server itself has no GUI dependencies at all (it's a headless HTTP
# service) -- --gui is separate and optional specifically so this script
# stays usable on a headless box (NAS, home server, container host).
#
# --service installs a *user* systemd unit (see crates/gateway-gui/src/service.rs),
# so it needs no root and survives a reboot via linger. It is driven through
# the GUI binary's own `--enable-always-on` flag rather than a unit file
# written here, so there is exactly one definition of that unit in the repo.

set -euo pipefail

BUILD=1
GUI=0
SERVICE=0
for arg in "$@"; do
  case "$arg" in
    --no-build) BUILD=0 ;;
    --gui) GUI=1 ;;
    --service) SERVICE=1 ;;
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
  if [ "${ID:-}" != "ubuntu" ] && [[ "${ID_LIKE:-}" != *ubuntu* ]] && [[ "${ID_LIKE:-}" != *debian* ]]; then
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

# Only needed for the GUI launcher (winit/glow's Linux windowing backend) --
# skipped entirely unless --gui is passed, so a headless install never asks
# for X11/GTK packages it doesn't need.
GUI_APT_PACKAGES=(libgtk-3-dev libx11-dev libxkbcommon-dev libxrandr-dev libxi-dev libxcursor-dev libgl1-mesa-dev libwayland-dev)

missing=()
for pkg in "${REQUIRED_APT_PACKAGES[@]}"; do
  dpkg -s "$pkg" >/dev/null 2>&1 || missing+=("$pkg")
done
if [ "$GUI" -eq 1 ]; then
  for pkg in "${GUI_APT_PACKAGES[@]}"; do
    dpkg -s "$pkg" >/dev/null 2>&1 || missing+=("$pkg")
  done
fi

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
  if [ "$GUI" -eq 1 ]; then
    log "Building server + GUI in release mode (this compiles ~350 crates the first time; later builds are incremental)..."
    cargo build --release
  else
    log "Building the server in release mode (this compiles ~250 crates the first time; later builds are incremental)..."
    cargo build --release -p streaming-gateway
  fi

  BIN="$SCRIPT_DIR/target/release/streaming-gateway"
  [ -x "$BIN" ] || die "build finished but $BIN is missing -- something went wrong."

  # ---------------------------------------------------------------------------
  # 5. Verify: --help exits via clap before any networking/torrent code runs,
  #    so this proves the binary links and starts without needing a network,
  #    a free port, or root.
  # ---------------------------------------------------------------------------
  log "Verifying the server binary starts..."
  "$BIN" --help >/dev/null
  log "OK: $BIN runs."

  if [ "$GUI" -eq 1 ]; then
    GUI_BIN="$SCRIPT_DIR/target/release/streaming-gateway-gui"
    [ -x "$GUI_BIN" ] || die "build finished but $GUI_BIN is missing -- something went wrong."
    log "OK: $GUI_BIN built (a GUI app can't be verified headlessly the same way; launch it to check)."

    if ! command -v cargo-deb >/dev/null 2>&1; then
      log "Installing cargo-deb (one-time; only needed to build the .deb package)..."
      cargo install cargo-deb
    fi

    log "Packaging the .deb (server + GUI + desktop menu entry + icon)..."
    DEB_PATH="$(cargo deb -p streaming-gateway-gui --no-build)"
    log "Built: $DEB_PATH"

    log "Installing $DEB_PATH (asks for sudo)..."
    sudo dpkg -i "$DEB_PATH" || sudo apt-get install -f -y
    log "OK: NovaStream installed to /usr/bin, menu entry + icon in place."

    # Drop a real, clickable icon on the Desktop too, not just the app menu
    # entry dpkg already installed to /usr/share/applications.
    DESKTOP_DIR="$(xdg-user-dir DESKTOP 2>/dev/null || true)"
    DESKTOP_DIR="${DESKTOP_DIR:-$HOME/Desktop}"
    mkdir -p "$DESKTOP_DIR"
    SHORTCUT="$DESKTOP_DIR/novastream.desktop"
    cp "$SCRIPT_DIR/crates/gateway-gui/assets/streaming-gateway-gui.desktop" "$SHORTCUT"
    chmod +x "$SHORTCUT"
    # GNOME/Nautilus refuses to run a Desktop .desktop file until it's marked
    # trusted -- otherwise double-clicking it just opens it as a text file.
    command -v gio >/dev/null 2>&1 && gio set "$SHORTCUT" metadata::trusted true 2>/dev/null || true
    log "OK: Desktop shortcut created at $SHORTCUT"
    echo "  Launch \"NovaStream\" from your applications menu, the new Desktop icon, or run streaming-gateway-gui."

    if [ "$SERVICE" -eq 1 ]; then
      # The installed binary, not the one in target/: the unit records an
      # absolute ExecStart, and pointing it at a build directory would break
      # the service the first time someone runs `cargo clean`.
      log "Enabling always-on mode (systemd user service, starts at boot)..."
      /usr/bin/streaming-gateway-gui --enable-always-on
    fi
  elif [ "$SERVICE" -eq 1 ]; then
    warn "--service needs --gui: the always-on switch lives in the desktop binary."
  fi

  # Optional convenience: open the default port on ufw if it's active, so
  # phones/TVs on the LAN can actually reach the gateway. Skipped silently if
  # ufw isn't installed or isn't enabled -- this is a nice-to-have, not a
  # requirement (plenty of desktops run without any local firewall at all).
  if command -v ufw >/dev/null 2>&1 && sudo ufw status 2>/dev/null | head -1 | grep -qi "active"; then
    log "ufw is active -- allowing inbound TCP on 8080 and 11470 (gateway ports) from the LAN."
    sudo ufw allow 8080/tcp >/dev/null || true
    sudo ufw allow 11470/tcp >/dev/null || true
  fi

  echo
  log "Setup complete. Start the gateway with:"
  echo "    $BIN"
  echo "  or, for development iteration:"
  echo "    cargo run --release -p streaming-gateway"
else
  log "Setup complete (--no-build passed, skipped compiling)."
fi
