//! The install is described in four places, and nothing at runtime notices
//! when they disagree.
//!
//! `cargo deb` and `cargo generate-rpm` read their own tables in
//! `Cargo.toml`, `packaging/arch/PKGBUILD` has its own `install` lines, and
//! `setup.sh` has a hand-rolled copy for every distro without a packager --
//! which, since that fallback is what Arch, Alpine, Void, Gentoo and NixOS
//! actually get, is the *most* used of the four, not the least.
//!
//! Drift between them is silent in the worst way. A package that puts the
//! binary somewhere the `.desktop` entry does not point still builds, still
//! installs, still shows up in the applications menu, and does nothing at all
//! when clicked. An `Icon=` name that no installed file matches renders as a
//! blank square, which looks like a theme problem rather than a packaging
//! one. Both are invisible from every angle except following the path.
//!
//! So these tests read the four manifests as text and compare them. Text and
//! not a TOML parser on purpose: adding a dev-dependency to check the build
//! metadata is the sort of thing that grows a second dependency tree, and the
//! files being matched are all in this repo and all in a format we control.

/// The install, as the packages must all agree on it: the path each file
/// lands at under the package prefix, and the source it comes from.
///
/// `/usr/bin` for the native packages, `/usr/local/bin` for the source
/// install in `setup.sh` -- so the prefix is stripped before comparing and
/// only the tail is pinned. That is the part that has to match, because it is
/// the part the `.desktop` entry and the icon lookup depend on.
const INSTALLED_FILES: [(&str, &str); 4] = [
    ("streaming-gateway-gui", "bin/streaming-gateway-gui"),
    ("streaming-gateway", "bin/streaming-gateway"),
    (
        "streaming-gateway-gui.desktop",
        "share/applications/streaming-gateway-gui.desktop",
    ),
    (
        "icon-256.png",
        "share/pixmaps/streaming-gateway-gui.png",
    ),
];

const CARGO_TOML: &str = include_str!("../Cargo.toml");
const DESKTOP_ENTRY: &str = include_str!("../assets/streaming-gateway-gui.desktop");
const PKGBUILD: &str = include_str!("../../../packaging/arch/PKGBUILD");
const SETUP_SH: &str = include_str!("../../../setup.sh");

/// The body of a named `[package.metadata.*]` table, up to the next table
/// header.
fn metadata_table<'a>(manifest: &'a str, header: &str) -> &'a str {
    let start = manifest
        .find(header)
        .unwrap_or_else(|| panic!("{header} is missing from Cargo.toml"))
        + header.len();
    let rest = &manifest[start..];
    match rest.find("\n[") {
        Some(end) => &rest[..end],
        None => rest,
    }
}

fn value_of(entry: &str, key: &str) -> Option<String> {
    entry
        .lines()
        .map(str::trim)
        .find_map(|line| line.strip_prefix(key))
        .map(|value| value.trim().to_string())
}

#[test]
fn every_packaging_manifest_installs_the_same_four_files() {
    let deb = metadata_table(CARGO_TOML, "[package.metadata.deb]");
    let rpm = metadata_table(CARGO_TOML, "[package.metadata.generate-rpm]");

    for (source, dest) in INSTALLED_FILES {
        // The .deb writes destinations without a leading slash
        // ("usr/share/pixmaps/..."), the .rpm with one. Matching on the tail
        // covers both spellings without pinning either.
        assert!(
            deb.contains(dest),
            "the .deb does not install {dest}; a file the other packages ship \
             would be missing on Debian/Ubuntu only"
        );
        assert!(
            rpm.contains(dest),
            "the .rpm does not install {dest}; a file the other packages ship \
             would be missing on Fedora/openSUSE only"
        );
        assert!(
            PKGBUILD.contains(dest),
            "packaging/arch/PKGBUILD does not install {dest}"
        );
        assert!(
            SETUP_SH.contains(dest),
            "setup.sh's install_manually does not install {dest} -- that is \
             the path every distro without a native packager takes, so this \
             is the widest breakage of the four"
        );

        assert!(
            deb.contains(source) && rpm.contains(source) && PKGBUILD.contains(source),
            "{source} is not named as a source in every packaging manifest"
        );
    }
}

#[test]
fn the_desktop_entry_launches_a_binary_the_packages_actually_install() {
    let exec = value_of(DESKTOP_ENTRY, "Exec=").expect("the .desktop entry has an Exec= line");
    // Exec may carry arguments; the program is the first field.
    let program = exec.split_whitespace().next().expect("Exec= is not empty");

    let installed: Vec<&str> = INSTALLED_FILES
        .iter()
        .filter_map(|(_, dest)| dest.strip_prefix("bin/"))
        .collect();

    assert!(
        installed.contains(&program),
        "the menu entry runs `{program}`, which none of the packages install \
         (they install {installed:?}). Clicking the icon would do nothing, and \
         the package would still install cleanly."
    );
}

#[test]
fn the_desktop_entry_icon_name_matches_the_icon_file_that_gets_installed() {
    let icon = value_of(DESKTOP_ENTRY, "Icon=").expect("the .desktop entry has an Icon= line");
    assert!(
        !icon.contains('/'),
        "Icon= should be a theme name, not a path ({icon}) -- a path breaks \
         the moment the prefix changes from /usr to /usr/local, which is \
         exactly what setup.sh's fallback install does"
    );

    let installed_icon = INSTALLED_FILES
        .iter()
        .find_map(|(_, dest)| dest.strip_prefix("share/pixmaps/"))
        .expect("an icon is installed");
    let stem = installed_icon
        .rsplit_once('.')
        .map(|(stem, _)| stem)
        .unwrap_or(installed_icon);

    assert_eq!(
        icon, stem,
        "the menu entry asks for icon `{icon}` but the packages install \
         `{installed_icon}`. Icon lookup finds nothing and renders a blank \
         square, which reads as a broken icon theme rather than a packaging bug."
    );
}

/// Every package-manager branch in `setup.sh` must name both sets of
/// packages.
///
/// Build deps and GUI runtime deps are separate lists because the runtime
/// ones are `dlopen`ed -- so a branch that sets `BUILD_PACKAGES` and forgets
/// `GUI_RUNTIME_PACKAGES` compiles perfectly on that distro and then fails to
/// open a window on a minimal install, with a dlopen error and no hint that a
/// package is missing. Counting them is enough to catch a branch added in a
/// hurry.
#[test]
fn every_package_manager_branch_names_both_build_and_gui_runtime_packages() {
    // The declarations at the top of the script initialise every one of these
    // to empty before the detection chain assigns them, so the empty forms
    // (`PM=""`, `BUILD_PACKAGES=()`) have to be discounted or the manager
    // count is always one ahead of the lists.
    let count = |prefix: &str| {
        SETUP_SH
            .lines()
            .map(str::trim)
            .filter(|line| {
                line.starts_with(prefix) && !line.ends_with("=()") && !line.ends_with("=\"\"")
            })
            .count()
    };

    let managers = count("PM=\"");
    let build = count("BUILD_PACKAGES=(");
    let runtime = count("GUI_RUNTIME_PACKAGES=(");

    assert!(
        managers >= 6,
        "setup.sh recognises only {managers} package managers; apt, dnf, \
         pacman, zypper, apk and xbps are the mainstream set"
    );
    assert_eq!(
        managers, build,
        "{managers} package-manager branches but {build} BUILD_PACKAGES lists \
         -- one branch would install nothing and fail at the first compile"
    );
    assert_eq!(
        managers, runtime,
        "{managers} package-manager branches but {runtime} GUI_RUNTIME_PACKAGES \
         lists -- the missing one builds fine and then cannot open a window"
    );
}

/// `install_manually` is the path for every distro without a native packager,
/// and it is the only one that chooses its own prefix. If it ever installs
/// under /usr while a native package also owns /usr, the two fight.
#[test]
fn the_universal_install_uses_its_own_prefix_and_not_the_packages() {
    assert!(
        SETUP_SH.contains(r#"PREFIX="/usr/local""#),
        "the fallback install must stay under /usr/local so it never collides \
         with a .deb or .rpm that owns /usr"
    );
}
