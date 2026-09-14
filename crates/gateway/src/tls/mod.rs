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

/// When the listener came up on the cached copy, how soon to try the provider
/// again. Coming up on the cache nearly always means the network was not up
/// yet -- a boot or a wake from sleep races the wifi -- and waiting a whole
/// `REFRESH_INTERVAL` for a fresh certificate would leave an expiring one in
/// service for a day for no reason.
const RETRY_AFTER_CACHED: Duration = Duration::from_secs(60);

/// Bounds the fetch so a hung certificate host delays startup briefly rather
/// than indefinitely -- there is a cached copy and an http fallback behind it.
const FETCH_TIMEOUT: Duration = Duration::from_secs(15);

/// Where the certificate being served came from, which decides whether and
/// when it is refreshed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CertOrigin {
    /// `--tls-cert-file`/`--tls-key-file`. Never refreshed from the network:
    /// doing so would replace a certificate for a domain the operator controls
    /// with the public wildcard one.
    LocalFiles,
    /// Fetched from the provider just now.
    Fetched,
    /// The provider was unreachable, so this is the last known-good copy off
    /// disk. The cached copy is a normal ~90-day certificate, so it keeps
    /// working for a while and then simply stops -- and an expired certificate
    /// presents as Stremio refusing to load the addon, with nothing anywhere
    /// saying why.
    Cached,
}

impl CertOrigin {
    /// How long until the first refresh, or `None` when this certificate is
    /// not one to refresh.
    pub fn first_refresh(self) -> Option<Duration> {
        match self {
            CertOrigin::LocalFiles => None,
            CertOrigin::Fetched => Some(REFRESH_INTERVAL),
            CertOrigin::Cached => Some(RETRY_AFTER_CACHED),
        }
    }
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

/// The provider's certificate and key, and the one HTTP client used to fetch
/// them for the life of the process -- rather than a new client, and so a new
/// connection pool and TLS setup, for each of the two files on every fetch.
struct CertSource {
    client: reqwest::Client,
    cert_url: String,
    key_url: String,
    cache_dir: PathBuf,
}

impl CertSource {
    fn new(cert_url: &str, key_url: &str, cache_dir: &Path) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(FETCH_TIMEOUT)
            .build()
            .context("building the certificate fetch client")?;
        Ok(Self {
            client,
            cert_url: cert_url.to_string(),
            key_url: key_url.to_string(),
            cache_dir: cache_dir.to_path_buf(),
        })
    }

    async fn fetch(&self, url: &str) -> Result<Vec<u8>> {
        let response = self
            .client
            .get(url)
            .send()
            .await
            .with_context(|| format!("requesting {url}"))?
            .error_for_status()
            .with_context(|| format!("{url} returned an error status"))?;
        Ok(response.bytes().await?.to_vec())
    }

    async fn fetch_pair(&self) -> Result<(Vec<u8>, Vec<u8>)> {
        tokio::try_join!(self.fetch(&self.cert_url), self.fetch(&self.key_url))
    }

    /// Keeps a pair that has just proved usable, for the next start without
    /// a network.
    ///
    /// Only ever called after the pair was accepted by rustls. It used to be
    /// written the moment both downloads finished, so an error page served
    /// with a 200 replaced a working cached pair with one that fails every
    /// handshake -- and the cache is precisely what is supposed to rescue a
    /// bad fetch. Each file is written atomically, the key owner-only.
    async fn store(&self, cert_pem: &[u8], key_pem: &[u8]) {
        let (cert_path, key_path) = cache_paths(&self.cache_dir);
        if let Err(e) = crate::util::write_atomic(&cert_path, cert_pem.to_vec(), false).await {
            warn!("could not cache the certificate: {e}");
        }
        // This particular key is published to the world, so secrecy is not
        // the point -- but the same code path serves a user-supplied cache
        // directory, and a key file that is world-readable by default is the
        // kind of thing that silently becomes wrong the moment someone points
        // this at their own certificate.
        if let Err(e) = crate::util::write_atomic(&key_path, key_pem.to_vec(), true).await {
            warn!("could not cache the certificate key: {e}");
        }
    }

    async fn read_cache(&self) -> Result<RustlsConfig> {
        let (cert_path, key_path) = cache_paths(&self.cache_dir);
        let cert_pem = tokio::fs::read(&cert_path)
            .await
            .context("no cached certificate to fall back on")?;
        let key_pem = tokio::fs::read(&key_path)
            .await
            .context("no cached certificate key to fall back on")?;
        RustlsConfig::from_pem(cert_pem, key_pem)
            .await
            .context("the cached certificate and key are not a usable pair")
    }

    /// A fresh, usable configuration from the provider, falling back to the
    /// cached copy when the provider is unreachable *or* hands back something
    /// rustls will not accept.
    async fn obtain(&self) -> Result<(RustlsConfig, CertOrigin)> {
        let fetched = match self.fetch_pair().await {
            Ok((cert_pem, key_pem)) => {
                match RustlsConfig::from_pem(cert_pem.clone(), key_pem.clone()).await {
                    Ok(config) => {
                        self.store(&cert_pem, &key_pem).await;
                        return Ok((config, CertOrigin::Fetched));
                    }
                    Err(e) => {
                        anyhow::Error::from(e).context("the fetched certificate was unusable")
                    }
                }
            }
            Err(e) => e,
        };
        warn!("could not fetch a certificate ({fetched:#}); trying the cached copy");
        Ok((self.read_cache().await?, CertOrigin::Cached))
    }

    /// Swaps freshly fetched material into a running listener. Never falls
    /// back to the cache: reloading the copy already being served would
    /// report success while changing nothing, and end the retries early.
    async fn refresh(&self, config: &RustlsConfig) -> Result<()> {
        let (cert_pem, key_pem) = self.fetch_pair().await?;
        config
            .reload_from_pem(cert_pem.clone(), key_pem.clone())
            .await
            .context("the refreshed certificate was unusable")?;
        self.store(&cert_pem, &key_pem).await;
        Ok(())
    }
}

/// Builds the TLS configuration for the https listener, and says where the
/// certificate came from (see `CertOrigin::first_refresh`).
///
/// `cert_file`/`key_file` take precedence when set, so anyone using a
/// certificate for a domain they actually control never touches the network.
pub async fn load(
    cert_file: Option<&Path>,
    key_file: Option<&Path>,
    cert_url: &str,
    key_url: &str,
    cache_dir: &Path,
) -> Result<(RustlsConfig, CertOrigin)> {
    install_crypto_provider();

    match (cert_file, key_file) {
        (Some(cert), Some(key)) => {
            info!(cert = %cert.display(), "using a locally supplied certificate");
            let config = RustlsConfig::from_pem_file(cert, key)
                .await
                .with_context(|| {
                    format!(
                        "{} and {} were not a usable certificate and key",
                        cert.display(),
                        key.display()
                    )
                })?;
            Ok((config, CertOrigin::LocalFiles))
        }
        (None, None) => {
            let (config, origin) = CertSource::new(cert_url, key_url, cache_dir)?
                .obtain()
                .await?;
            if origin == CertOrigin::Cached {
                warn!(
                    "serving a cached certificate -- {cert_url} was unreachable. It stays valid \
                     for a few weeks and then expires, at which point Stremio will silently \
                     refuse to load the addon. Check the certificate source, or supply your own \
                     with --tls-cert-file/--tls-key-file."
                );
            }
            Ok((config, origin))
        }
        _ => anyhow::bail!("--tls-cert-file and --tls-key-file must be given together"),
    }
}

/// Keeps the listener's certificate current for the life of the process.
///
/// The certificate is replaced periodically at the source, and this server is
/// expected to run for weeks. `RustlsConfig` swaps in new material without
/// dropping the listener, so a refresh is invisible to anything connected.
/// A failed refresh is logged and retried with backoff, never fatal -- the
/// existing certificate keeps working until it genuinely expires.
pub fn spawn_refresh(
    config: RustlsConfig,
    cert_url: String,
    key_url: String,
    cache_dir: PathBuf,
    first_refresh: Duration,
) {
    tokio::spawn(async move {
        let source = match CertSource::new(&cert_url, &key_url, &cache_dir) {
            Ok(source) => source,
            Err(e) => {
                warn!("certificate refresh is disabled: {e:#}");
                return;
            }
        };
        let max_backoff = Duration::from_secs(3600);
        let mut wait = first_refresh;
        loop {
            tokio::time::sleep(wait).await;
            let mut backoff = Duration::from_secs(60);
            while let Err(e) = source.refresh(&config).await {
                warn!(
                    "certificate refresh failed, retrying in {}s: {e:#}",
                    backoff.as_secs()
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
            }
            debug!("refreshed the https certificate");
            wait = REFRESH_INTERVAL;
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
    fn only_a_network_certificate_is_refreshed_and_a_cached_one_soonest() {
        assert_eq!(
            CertOrigin::LocalFiles.first_refresh(),
            None,
            "an operator's own certificate must never be swapped for the public one"
        );
        assert!(CertOrigin::Cached.first_refresh() < CertOrigin::Fetched.first_refresh());
    }

    /// The regression: a provider answering 200 with something that is not a
    /// certificate used to be written straight over the cache.
    #[tokio::test]
    async fn an_unusable_download_is_never_cached() {
        let app = axum::Router::new().fallback(|| async { "<html>maintenance</html>" });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await });

        let cache = tempfile::tempdir().unwrap();
        let result = load(
            None,
            None,
            &format!("http://{addr}/server.pem"),
            &format!("http://{addr}/server.key"),
            cache.path(),
        )
        .await;

        assert!(
            result.is_err(),
            "no usable certificate anywhere must be an error"
        );
        let (cert_path, key_path) = cache_paths(cache.path());
        assert!(!cert_path.exists() && !key_path.exists());
    }

    #[test]
    fn cache_paths_sit_under_the_cache_directory() {
        let (cert, key) = cache_paths(Path::new("/var/cache/gw"));
        assert_eq!(cert, Path::new("/var/cache/gw/tls/server.pem"));
        assert_eq!(key, Path::new("/var/cache/gw/tls/server.key"));
    }
}
