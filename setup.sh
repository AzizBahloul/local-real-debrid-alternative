#!/usr/bin/env bash
# Prepares any mainstream Linux machine to build and run the streaming
# gateway, then builds it and verifies the result.
#
# Idempotent: safe to re-run. Only installs what's actually missing, never
# reinstalls something already present, and never touches Node or Python
# (this project needs neither).
#
# Usage:
#   ./setup.sh              # install build deps (asks for sudo if needed) + build + verify
#   ./setup.sh --gui        # also build the desktop GUI launcher and install it
#   ./setup.sh --gui --service  # ...and run the gateway as an always-on user service
#   ./setup.sh --no-build   # only install system/Rust deps, skip compiling
#   ./setup.sh --gui --user # install into ~/.local instead of /usr -- no root at all
#
# --user exists because rebuilding is the common case and typing a password
# every time is not. It installs the same four files to the same freedesktop
# locations, just under $HOME/.local, which needs no privilege whatsoever --
# so an unattended rebuild-and-install is possible without granting anything
# passwordless root. It also skips the distro package step, since installing
# system packages is the one part that genuinely does need root; on a machine
# that has already built once, there is nothing left for it to install.
#
# The server itself has no GUI dependencies at all (it's a headless HTTP
# service) -- --gui is separate and optional specifically so this script
# stays usable on a headless box (NAS, home server, container host).
#
# --service installs a *user* systemd unit (see crates/gateway-gui/src/service.rs),
# so it needs no root and survives a reboot via linger. It is driven through
# the GUI binary's own `--enable-always-on` flag rather than a unit file
# written here, so there is exactly one definition of that unit in the repo.
#
# ---------------------------------------------------------------------------
# Why this script is short despite covering every distro
# ---------------------------------------------------------------------------
# It used to install 15 -dev packages under Debian names, which is what made
# it Ubuntu-only. Measured on 2026-09-07, none of them were needed:
#
#   * `ldd` on both release binaries reports only libc, libm and libgcc_s.
#     winit/glutin do not link X11, Wayland, xkbcommon or GL at build time --
#     they `dlopen` all of them at runtime (the `libloading`, `x11-dl` and
#     `wayland-sys` dlopen features), so there is nothing to compile against.
#   * There is no GTK anywhere in the tree. The tray is `ksni`, which speaks
#     the StatusNotifierItem D-Bus protocol in pure Rust; the usual
#     libappindicator/GTK path was avoided on purpose.
#   * aws-lc-sys 0.45 builds with the `cc` crate, not CMake (its `out/` holds
#     .o files and no CMakeCache.txt). A full rebuild with cmake, perl,
#     pkg-config, nasm and go all stubbed to exit 127 succeeds.
#
# So the real requirement is a C toolchain and Rust. That is the same three
# packages on every distro, which is why the matrix below is small enough to
# be trustworthy.

set -euo pipefail

BUILD=1
GUI=0
SERVICE=0
USER_PREFIX=0
for arg in "$@"; do
  case "$arg" in
    --no-build) BUILD=0 ;;
    --gui) GUI=1 ;;
    --service) SERVICE=1 ;;
    --user) USER_PREFIX=1 ;;
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
# 0. How do we become root?
#
#    Not every distro ships sudo (Debian netinst without it, some Arch and
#    Void installs prefer doas), and inside a container we are usually root
#    already. Resolving this once means every privileged call below is just
#    "$SUDO cmd" and works everywhere.
# ---------------------------------------------------------------------------
if [ "$USER_PREFIX" -eq 1 ]; then
  # Nothing a --user run does needs privilege, so it must never ask for any.
  # Emptying SUDO here rather than at the install site also makes the package
  # step below take its own "cannot install packages" branch, which is the
  # correct outcome: a home-directory install has no business touching /usr.
  SUDO=""
elif [ "$(id -u)" -eq 0 ]; then
  SUDO=""
elif command -v sudo >/dev/null 2>&1; then
  SUDO="sudo"
elif command -v doas >/dev/null 2>&1; then
  SUDO="doas"
else
  SUDO=""
  warn "Neither sudo nor doas found and you are not root -- package installs will be skipped."
fi

# ---------------------------------------------------------------------------
# 1. Detect the distribution and, more importantly, its package manager.
#
#    Detection is on the package manager rather than the ID in os-release:
#    there are far more distro IDs than package managers, and a derivative
#    nobody has heard of (Nobara, CachyOS, Zorin) is correctly handled by
#    whichever of the six below it inherited.
#
#    The one family this does NOT cover is the immutable/atomic desktops
#    (Silverblue, Kinoite, Bazzite, SteamOS): they ship no host package
#    manager at all, so every branch below misses and the script stops at the
#    "no C compiler" check rather than installing into a read-only /usr.
#    Failing there is the correct outcome -- a half-install on an ostree
#    system is worse than no install.
# ---------------------------------------------------------------------------
DISTRO_NAME="unknown Linux"
DISTRO_ID=""
if [ -r /etc/os-release ]; then
  # shellcheck disable=SC1091
  . /etc/os-release
  DISTRO_NAME="${PRETTY_NAME:-${NAME:-unknown Linux}}"
  DISTRO_ID="${ID:-}"
fi

PM=""
PM_INSTALL=""
PM_HAS=""       # returns 0 when the named package is already installed
BUILD_PACKAGES=()
# Runtime-only, and only for --gui: these are dlopen'd, so they are never
# needed to build, but a machine with no desktop installed at all will not
# have them. Listed per family so the GUI can be run on a minimal install.
GUI_RUNTIME_PACKAGES=()

if command -v apt-get >/dev/null 2>&1; then
  PM="apt"
  PM_INSTALL="$SUDO apt-get install -y"
  PM_HAS() { dpkg -s "$1" >/dev/null 2>&1; }
  BUILD_PACKAGES=(build-essential git curl ca-certificates)
  GUI_RUNTIME_PACKAGES=(libgl1 libegl1 libx11-6 libxkbcommon0 libwayland-client0)
elif command -v dnf >/dev/null 2>&1; then
  PM="dnf"
  PM_INSTALL="$SUDO dnf install -y"
  PM_HAS() { rpm -q "$1" >/dev/null 2>&1; }
  BUILD_PACKAGES=(gcc gcc-c++ make git curl ca-certificates)
  GUI_RUNTIME_PACKAGES=(mesa-libGL mesa-libEGL libX11 libxkbcommon wayland-libs-client)
elif command -v pacman >/dev/null 2>&1; then
  PM="pacman"
  PM_INSTALL="$SUDO pacman -S --needed --noconfirm"
  PM_HAS() { pacman -Qi "$1" >/dev/null 2>&1; }
  BUILD_PACKAGES=(base-devel git curl)
  GUI_RUNTIME_PACKAGES=(libglvnd libx11 libxkbcommon wayland)
elif command -v zypper >/dev/null 2>&1; then
  PM="zypper"
  PM_INSTALL="$SUDO zypper --non-interactive install"
  PM_HAS() { rpm -q "$1" >/dev/null 2>&1; }
  BUILD_PACKAGES=(gcc gcc-c++ make git curl ca-certificates)
  GUI_RUNTIME_PACKAGES=(Mesa-libGL1 Mesa-libEGL1 libX11-6 libxkbcommon0 libwayland-client0)
elif command -v apk >/dev/null 2>&1; then
  PM="apk"
  PM_INSTALL="$SUDO apk add"
  PM_HAS() { apk info -e "$1" >/dev/null 2>&1; }
  BUILD_PACKAGES=(build-base git curl ca-certificates)
  GUI_RUNTIME_PACKAGES=(mesa-gl mesa-egl libx11 libxkbcommon wayland-libs-client)
elif command -v xbps-install >/dev/null 2>&1; then
  PM="xbps"
  PM_INSTALL="$SUDO xbps-install -Sy"
  PM_HAS() { xbps-query "$1" >/dev/null 2>&1; }
  BUILD_PACKAGES=(base-devel git curl)
  GUI_RUNTIME_PACKAGES=(libglvnd libX11 libxkbcommon wayland)
else
  PM_HAS() { return 1; }
fi

log "Detected: $DISTRO_NAME (package manager: ${PM:-none recognised})"

# ---------------------------------------------------------------------------
# 2. Build dependencies: a C toolchain, and that is all.
#
#    Gentoo and NixOS are deliberately not in the matrix above -- both build
#    from source by design and already have a toolchain, and shelling out to
#    `emerge`/`nix-env` from a setup script would fight the distro rather
#    than help it. They fall through to the "no package manager" branch,
#    which just checks that `cc` exists and carries on.
# ---------------------------------------------------------------------------
WANTED=("${BUILD_PACKAGES[@]}")
if [ "$GUI" -eq 1 ] && [ "${#GUI_RUNTIME_PACKAGES[@]}" -gt 0 ]; then
  WANTED+=("${GUI_RUNTIME_PACKAGES[@]}")
fi

if [ -z "$PM" ]; then
  warn "No supported package manager found (looked for apt-get, dnf, pacman, zypper, apk, xbps-install)."
  warn "This is expected on Gentoo, NixOS and source-built systems."
  command -v cc >/dev/null 2>&1 || command -v gcc >/dev/null 2>&1 \
    || die "No C compiler on PATH either -- install a C toolchain and re-run."
  log "A C compiler is present, which is the only system requirement. Continuing."
elif [ "$USER_PREFIX" -eq 1 ]; then
  log "--user: skipping distro packages (they need root). Checking the toolchain instead."
  command -v cc >/dev/null 2>&1 || command -v gcc >/dev/null 2>&1 \
    || die "No C compiler on PATH -- run this once without --user, or install a C toolchain."
elif [ -z "$SUDO" ] && [ "$(id -u)" -ne 0 ]; then
  warn "Cannot install packages without root. Assuming a C toolchain is already present."
else
  missing=()
  for pkg in "${WANTED[@]}"; do
    PM_HAS "$pkg" || missing+=("$pkg")
  done
  if [ "${#missing[@]}" -gt 0 ]; then
    log "Installing missing packages: ${missing[*]}"
    # Refresh metadata first where the package manager needs it; pacman and
    # apk resolve against a local database that may be older than the
    # mirrors, and installing against a stale one 404s.
    case "$PM" in
      apt) $SUDO apt-get update -y ;;
      pacman) $SUDO pacman -Sy --noconfirm >/dev/null ;;
      apk) $SUDO apk update >/dev/null ;;
    esac
    # shellcheck disable=SC2086
    $PM_INSTALL "${missing[@]}"
  else
    log "All required packages already present: ${WANTED[*]}"
  fi
fi

# ---------------------------------------------------------------------------
# 3. Rust toolchain (rustup + stable). Skips install if cargo/rustc already work.
#
#    Distro-packaged Rust is accepted when it is new enough; several distros
#    (Fedora, Arch, openSUSE) ship a current rustc, and installing rustup on
#    top of it leaves two toolchains fighting over PATH.
# ---------------------------------------------------------------------------
MIN_RUST_MINOR=75

rust_minor() {
  rustc --version 2>/dev/null | sed -n 's/^rustc 1\.\([0-9]*\).*/\1/p'
}

if command -v cargo >/dev/null 2>&1 && command -v rustc >/dev/null 2>&1; then
  have="$(rust_minor)"
  if [ -n "$have" ] && [ "$have" -lt "$MIN_RUST_MINOR" ]; then
    warn "Found $(rustc --version), which is older than the 1.$MIN_RUST_MINOR this workspace needs."
    warn "Install a newer toolchain (rustup, or your distro's rust package) and re-run."
    die "Rust too old."
  fi
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

# `rustup default` only exists when rustup manages the toolchain; a
# distro-packaged rustc has no such command and must not fail the script.
if command -v rustup >/dev/null 2>&1; then
  rustup default stable >/dev/null 2>&1 || true
fi
log "Using: $(rustc --version) / $(cargo --version)"

# ---------------------------------------------------------------------------
# 4. Build.
# ---------------------------------------------------------------------------
if [ "$BUILD" -eq 0 ]; then
  log "Setup complete (--no-build passed, skipped compiling)."
  exit 0
fi

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

# ---------------------------------------------------------------------------
# 6. Install the desktop app.
#
#    A native package where building one is a single command, and a plain
#    file copy everywhere else. The fallback is not a lesser path: it puts
#    the same four files in the same freedesktop locations the packages use,
#    just under /usr/local, so the menu entry, the icon and the always-on
#    service behave identically. That is what makes "every distro" true
#    rather than "the six distros with a packager".
# ---------------------------------------------------------------------------
#    --user swaps the prefix for $HOME/.local, which needs no privilege. On
#    every mainstream distro ~/.profile *prepends* ~/.local/bin to PATH, so
#    the user copy also shadows any older system one rather than losing to it
#    -- which matters, because `Exec=` in the .desktop entry is a bare program
#    name resolved through PATH.
if [ "$USER_PREFIX" -eq 1 ]; then
  PREFIX="$HOME/.local"
else
  PREFIX="/usr/local"
fi

install_manually() {
  log "Installing to $PREFIX..."
  $SUDO install -Dm755 "$SCRIPT_DIR/target/release/streaming-gateway-gui" "$PREFIX/bin/streaming-gateway-gui"
  $SUDO install -Dm755 "$SCRIPT_DIR/target/release/streaming-gateway"     "$PREFIX/bin/streaming-gateway"
  $SUDO install -Dm644 "$SCRIPT_DIR/crates/gateway-gui/assets/streaming-gateway-gui.desktop" \
    "$PREFIX/share/applications/streaming-gateway-gui.desktop"
  # pixmaps rather than the hicolor theme tree: it is read directly by icon
  # lookup with no cache refresh, so the icon is correct immediately instead
  # of after the next gtk-update-icon-cache.
  $SUDO install -Dm644 "$SCRIPT_DIR/crates/gateway-gui/assets/icon-256.png" \
    "$PREFIX/share/pixmaps/streaming-gateway-gui.png"
  # ...and again into the hicolor theme, but only under a home prefix.
  # `/usr/share/pixmaps` is a legacy location every icon lookup still falls
  # back to; `~/.local/share/pixmaps` is not searched by anything. Without
  # this the menu entry under --user renders a blank square, which reads as a
  # broken icon theme rather than as a packaging choice.
  if [ "$USER_PREFIX" -eq 1 ]; then
    install -Dm644 "$SCRIPT_DIR/crates/gateway-gui/assets/icon-256.png" \
      "$PREFIX/share/icons/hicolor/256x256/apps/streaming-gateway-gui.png"
  fi
  INSTALLED_GUI="$PREFIX/bin/streaming-gateway-gui"
}

# Makes sure the named cargo subcommand (cargo-deb / cargo-generate-rpm) is
# available, installing it if not. Returns non-zero when it cannot be had, so
# the caller can fall back to install_manually rather than abort the whole
# setup over a packaging tool.
ensure_cargo_packager() {
  local tool="$1"
  if ! command -v "$tool" >/dev/null 2>&1; then
    log "Installing $tool (one-time; only needed to build the package)..."
    cargo install "$tool" || return 1
  fi
  return 0
}

if [ "$GUI" -eq 1 ]; then
  GUI_BIN="$SCRIPT_DIR/target/release/streaming-gateway-gui"
  [ -x "$GUI_BIN" ] || die "build finished but $GUI_BIN is missing -- something went wrong."
  log "OK: $GUI_BIN built (a GUI app can't be verified headlessly the same way; launch it to check)."

  INSTALLED_GUI=""
  if [ "$USER_PREFIX" -eq 1 ]; then
    # No packager branch under --user: every native package installs into
    # /usr, which is the one thing this mode exists to avoid.
    install_manually
  else
  case "$PM" in
    apt)
      if ensure_cargo_packager cargo-deb; then
        log "Packaging the .deb (server + GUI + desktop menu entry + icon)..."
        DEB_PATH="$(cargo deb -p streaming-gateway-gui --no-build)"
        log "Built: $DEB_PATH"
        log "Installing $DEB_PATH..."
        $SUDO dpkg -i "$DEB_PATH" || $SUDO apt-get install -f -y
        INSTALLED_GUI="/usr/bin/streaming-gateway-gui"
        log "OK: NovaStream installed to /usr/bin, menu entry + icon in place."
      else
        warn "cargo-deb unavailable; falling back to a plain install."
        install_manually
      fi
      ;;
    dnf|zypper)
      if ensure_cargo_packager cargo-generate-rpm; then
        log "Packaging the .rpm (server + GUI + desktop menu entry + icon)..."
        # -o names the output instead of searching target/ for whatever is
        # newest: `find -newermt` is GNU-find-only syntax and silently errors
        # out under bfs (which some distros now ship as `find`), and -p takes
        # a *path* to the crate despite its --help calling it a crate name.
        RPM_PATH="$SCRIPT_DIR/target/generate-rpm/novastream.rpm"
        cargo generate-rpm -p crates/gateway-gui -o "$RPM_PATH"
        [ -f "$RPM_PATH" ] || die "cargo generate-rpm reported success but produced no .rpm"
        log "Built: $RPM_PATH"
        log "Installing $RPM_PATH..."
        if [ "$PM" = "dnf" ]; then
          $SUDO dnf install -y "$RPM_PATH"
        else
          $SUDO zypper --non-interactive install --allow-unsigned-rpm "$RPM_PATH"
        fi
        INSTALLED_GUI="/usr/bin/streaming-gateway-gui"
        log "OK: NovaStream installed to /usr/bin, menu entry + icon in place."
      else
        warn "cargo-generate-rpm unavailable; falling back to a plain install."
        install_manually
      fi
      ;;
    *)
      # Arch has packaging/arch/PKGBUILD for anyone who wants a tracked
      # package, but makepkg refuses to run as root and needs its own build
      # of the source tree, so the scripted path here stays the file copy.
      install_manually
      ;;
  esac
  fi

  # Drop a real, clickable icon on the Desktop too, not just the app menu
  # entry the install above created.
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
    if [ -z "$INSTALLED_GUI" ] || [ ! -x "$INSTALLED_GUI" ]; then
      warn "--service skipped: the GUI binary was not installed to a stable location."
    elif ! command -v systemctl >/dev/null 2>&1; then
      # Alpine (OpenRC), Void (runit), Artix, Devuan. The app already greys
      # the switch out rather than writing a unit nothing will read; saying
      # so here beats a confusing no-op.
      warn "--service skipped: always-on mode needs systemd, and this system has no systemctl."
      warn "The gateway still runs normally -- start it from the app or from $BIN."
    else
      log "Enabling always-on mode (systemd user service, starts at boot)..."
      "$INSTALLED_GUI" --enable-always-on
    fi
  fi
elif [ "$SERVICE" -eq 1 ]; then
  warn "--service needs --gui: the always-on switch lives in the desktop binary."
fi

# ---------------------------------------------------------------------------
# 7. Optional convenience: open the default ports on whichever local firewall
#    is actually running, so phones/TVs on the LAN can reach the gateway.
#    Skipped silently when no firewall is active -- plenty of desktops run
#    without one, and this is a nice-to-have, not a requirement.
# ---------------------------------------------------------------------------
if command -v ufw >/dev/null 2>&1 && $SUDO ufw status 2>/dev/null | head -1 | grep -qi "active"; then
  log "ufw is active -- allowing inbound TCP on 8080 and 11470 (gateway ports) from the LAN."
  $SUDO ufw allow 8080/tcp >/dev/null || true
  $SUDO ufw allow 11470/tcp >/dev/null || true
elif command -v firewall-cmd >/dev/null 2>&1 && $SUDO firewall-cmd --state >/dev/null 2>&1; then
  # Fedora, RHEL and derivatives ship firewalld enabled by default, which is
  # the single most likely reason a working gateway is unreachable from a
  # phone on those distros.
  log "firewalld is active -- allowing inbound TCP on 8080 and 11470 (gateway ports)."
  $SUDO firewall-cmd --permanent --add-port=8080/tcp >/dev/null || true
  $SUDO firewall-cmd --permanent --add-port=11470/tcp >/dev/null || true
  $SUDO firewall-cmd --reload >/dev/null || true
fi

echo
log "Setup complete. Start the gateway with:"
echo "    $BIN"
echo "  or, for development iteration:"
echo "    cargo run --release -p streaming-gateway"
