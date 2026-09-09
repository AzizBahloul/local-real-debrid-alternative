//! HTTPS for a machine that only has a private LAN address.
//!
//! Stremio's Android app refuses to install an addon from a plain
//! `http://192.168.x.x` URL -- verified repeatedly, it simply spins. That is
//! the *only* reason this project ever needed a tunnel: the video always
//! streamed fine over the LAN, but the few kilobytes of addon JSON had to
//! arrive over https, and a tunnel was the obvious way to get a real
//! certificate. It also dragged in a bandwidth cap, a URL that changes on
//! every restart, and a third party in the path.
//!
//! None of that is necessary. A certificate authority will not issue for
//! `192.168.1.67`, but it will issue for a *domain*, and nothing stops a
//! public domain from resolving to a private address. Services like
//! `local-ip.sh` do exactly that: `192-168-1-67.local-ip.sh` resolves to
//! `192.168.1.67`, and they publish a genuine Let's Encrypt wildcard
//! certificate for `*.local-ip.sh` -- private key and all -- for anyone to
//! use. Serving that certificate makes this machine a real https origin that
//! an unmodified phone trusts, with every byte still on the LAN.
//!
//! ## What the public private key does and does not mean
//!
//! The key is downloadable by anyone, so this is *not* protection against an
//! attacker who can already intercept traffic on your network -- they hold
//! the same key and could impersonate the hostname. It is strictly better
//! than the plain http it replaces (passive snooping on the wifi no longer
//! reveals what you are watching) and it is what unlocks the tunnel-free
//! path, but it should not be mistaken for authentication. Anyone wanting
//! real guarantees should point `--tls-cert-file`/`--tls-key-file` at a
//! certificate for a domain they control; everything else here is unchanged.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use axum_server::tls_rustls::RustlsConfig;
use tracing::{debug, info, warn};

/// How often to re-fetch the certificate. The published one is a normal
/// 90-day Let's Encrypt certificate that is replaced well before expiry, so
/// a daily check is far more often than strictly needed and costs two small
/// downloads.
const REFRESH_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

/// Bounds the fetch so a hung certificate host delays startup briefly rather
/// than indefinitely -- there is a cached copy and an http fallback behind it.
const FETCH_TIMEOUT: Duration = Duration::from_secs(15);

/// A certificate and its key, as PEM bytes.
struct Material {
    cert_pem: Vec<u8>,
    key_pem: Vec<u8>,
    /// True when this came off disk because the provider could not be
    /// reached. The cached copy is a normal ~90-day certificate, so it keeps
    /// working for a while and then simply stops -- and an expired
    /// certificate presents as Stremio refusing to load the addon, with
    /// nothing anywhere saying why. Worth saying out loud every time.
    from_cache: bool,
}

/// Turns a LAN address into the hostname the certificate is valid for.
///
/// The wildcard covers one label, so the address has to be encoded into a
/// single label: `192.168.1.67` becomes `192-168-1-67`, which the provider's
/// DNS resolves straight back to `192.168.1.67`.
pub fn hostname_for(ip: std::net::IpAddr, suffix: &str) -> String {
    let label = ip.to_string().replace(['.', ':'], "-");
    format!("{label}.{}", suffix.trim_matches('.'))
}

/// Where the last known-good certificate is kept, so a restart without
/// internet (or while the provider is down) still comes up on https.
fn cache_paths(cache_dir: &Path) -> (PathBuf, PathBuf) {
    let dir = cache_dir.join("tls");
    (dir.join("server.pem"), dir.join("server.key"))
}

async fn fetch(url: &str) -> Result<Vec<u8>> {
    let client = reqwest::Client::builder()
        .timeout(FETCH_TIMEOUT)
        .build()
        .context("building the certificate fetch client")?;
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("requesting {url}"))?
        .error_for_status()
        .with_context(|| format!("{url} returned an error status"))?;
    Ok(response.bytes().await?.to_vec())
}

/// Fetches the certificate, falling back to the cached copy when the network
/// or the provider is unavailable.
async fn obtain(cert_url: &str, key_url: &str, cache_dir: &Path) -> Result<Material> {
    let (cert_path, key_path) = cache_paths(cache_dir);

    match tokio::try_join!(fetch(cert_url), fetch(key_url)) {
        Ok((cert_pem, key_pem)) => {
            // Only overwrite the cache once *both* halves arrived: a cert
            // written next to a stale key produces a listener that fails
            // every handshake, which is far worse than the previous pair.
            if let Some(parent) = cert_path.parent() {
                let _ = tokio::fs::create_dir_all(parent).await;
            }
            if let Err(e) = tokio::fs::write(&cert_path, &cert_pem).await {
                warn!("could not cache the certificate: {e}");
            }
            if let Err(e) = write_private(&key_path, &key_pem).await {
                warn!("could not cache the certificate key: {e}");
            }
            Ok(Material {
                cert_pem,
                key_pem,
                from_cache: false,
            })
        }
        Err(e) => {
            warn!("could not fetch a certificate ({e:#}); trying the cached copy");
            let cert_pem = tokio::fs::read(&cert_path)
                .await
                .context("no cached certificate to fall back on")?;
            let key_pem = tokio::fs::read(&key_path)
                .await
                .context("no cached certificate key to fall back on")?;
            Ok(Material {
                cert_pem,
                key_pem,
                from_cache: true,
            })
        }
    }
}

/// Writes a private key with owner-only permissions.
///
/// This particular key is published to the world, so secrecy is not the
/// point -- but the same code path serves a user-supplied cache directory,
/// and a key file that is world-readable by default is the kind of thing
/// that silently becomes wrong the moment someone points this at their own
/// certificate.
async fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    tokio::fs::write(path, bytes).await?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).await?;
    }
    Ok(())
}

async fn read_files(cert: &Path, key: &Path) -> Result<Material> {
    Ok(Material {
        cert_pem: tokio::fs::read(cert)
            .await
            .with_context(|| format!("reading {}", cert.display()))?,
        key_pem: tokio::fs::read(key)
            .await
            .with_context(|| format!("reading {}", key.display()))?,
        from_cache: false,
    })
}

/// Builds the TLS configuration for the https listener.
///
/// `cert_file`/`key_file` take precedence when set, so anyone using a
/// certificate for a domain they actually control never touches the network.
pub async fn load(
    cert_file: Option<&Path>,
    key_file: Option<&Path>,
    cert_url: &str,
    key_url: &str,
    cache_dir: &Path,
) -> Result<RustlsConfig> {
    install_crypto_provider();

    let material = match (cert_file, key_file) {
        (Some(cert), Some(key)) => {
            info!(cert = %cert.display(), "using a locally supplied certificate");
            read_files(cert, key).await?
        }
        (None, None) => {
            let material = obtain(cert_url, key_url, cache_dir).await?;
            if material.from_cache {
                warn!(
                    "serving a cached certificate -- {cert_url} was unreachable. It stays valid \
                     for a few weeks and then expires, at which point Stremio will silently \
                     refuse to load the addon. Check the certificate source, or supply your own \
                     with --tls-cert-file/--tls-key-file."
                );
            }
            material
        }
        _ => anyhow::bail!("--tls-cert-file and --tls-key-file must be given together"),
    };

    RustlsConfig::from_pem(material.cert_pem, material.key_pem)
        .await
        .context("the certificate and key were not a usable pair")
}

/// Keeps the listener's certificate current for the life of the process.
///
/// The certificate is replaced periodically at the source, and this server is
/// expected to run for weeks. `RustlsConfig` swaps in new material without
/// dropping the listener, so a refresh is invisible to anything connected.
/// A failed refresh is logged and retried, never fatal -- the existing
/// certificate keeps working until it genuinely expires.
pub fn spawn_refresh(config: RustlsConfig, cert_url: String, key_url: String, cache_dir: PathBuf) {
    tokio::spawn(async move {
        tokio::time::sleep(REFRESH_INTERVAL).await;
        loop {
            let mut backoff = Duration::from_secs(60);
            let max_backoff = Duration::from_secs(3600);
            loop {
                match obtain(&cert_url, &key_url, &cache_dir).await {
                    Ok(material) => {
                        match config
                            .reload_from_pem(material.cert_pem, material.key_pem)
                            .await
                        {
                            Ok(()) => {
                                debug!("refreshed the https certificate");
                                break; // Success, break out of retry loop
                            }
                            Err(e) => {
                                warn!("refreshed certificate was unusable: {e}");
                                tokio::time::sleep(backoff).await;
                                backoff = (backoff * 2).min(max_backoff);
                            }
                        }
                    }
                    Err(e) => {
                        warn!("certificate refresh failed, retrying in {}s: {e:#}", backoff.as_secs());
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(max_backoff);
                    }
                }
            }
            tokio::time::sleep(REFRESH_INTERVAL).await;
        }
    });
}

/// rustls refuses to build a config until a crypto provider is installed, and
/// installing one twice is an error rather than a no-op. librqbit may have
/// installed the same provider already, so the result is deliberately
/// ignored: either it is now set, or it was already set to the same thing.
fn install_crypto_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hostname_encodes_an_ipv4_address_into_one_label() {
        assert_eq!(
            hostname_for("192.168.1.67".parse().unwrap(), "local-ip.sh"),
            "192-168-1-67.local-ip.sh"
        );
    }

    #[test]
    fn hostname_tolerates_a_suffix_written_with_dots_around_it() {
        // A wildcard covers exactly one label, so a stray leading dot would
        // produce `192-168-1-67..local-ip.sh` -- which resolves to nothing
        // and matches no certificate.
        assert_eq!(
            hostname_for("10.0.0.5".parse().unwrap(), ".local-ip.sh."),
            "10-0-0-5.local-ip.sh"
        );
    }

    #[test]
    fn cache_paths_sit_under_the_cache_directory() {
        let (cert, key) = cache_paths(Path::new("/var/cache/gw"));
        assert_eq!(cert, Path::new("/var/cache/gw/tls/server.pem"));
        assert_eq!(key, Path::new("/var/cache/gw/tls/server.key"));
    }
}
