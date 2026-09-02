//! Stremio addon protocol: `/manifest.json` and `/stream/{type}/{id}.json`.
//!
//! This addon does not search or index torrents -- it has no catalog and no
//! way to turn an IMDB id into a magnet link. What it *can* do is resolve an
//! id that already carries a magnet link (Stremio only queries an addon for
//! ids matching its declared `idPrefixes`, so we only ever get asked about
//! ids starting with `magnet:`). For anything else the correct, honest
//! response is an empty stream list, not an error -- Stremio queries every
//! installed stream addon for every title, so most calls here are expected
//! to be "not something we can help with".
//!
//! The primary, more general way to use this gateway is the `/play?magnet=`
//! endpoint directly (see the `streaming` module) -- it works from VLC,
//! browsers, Android/TV players, and Stremio's own "paste a link" flow, not
//! just from the addon-catalog flow.

use axum::extract::{Path, State};
use axum::Json;
use serde_json::{json, Value};

use crate::AppState;

pub const ADDON_ID: &str = "com.localgateway.streaminggateway";

pub async fn manifest() -> Json<Value> {
    Json(json!({
        "id": ADDON_ID,
        "version": env!("CARGO_PKG_VERSION"),
        "name": "Local Streaming Gateway",
        "description": "Plays a magnet link or info hash you already have through your own \
            local torrent engine. Not a search/indexer -- for browsing, use \
            GET /play?magnet=<magnet-link> directly (works in VLC, browsers, and TVs too).",
        "logo": "https://raw.githubusercontent.com/Stremio/stremio-brand/master/logos/icon.png",
        "resources": [
            { "name": "stream", "types": ["movie", "series", "other"], "idPrefixes": ["magnet:"] }
        ],
        "types": ["movie", "series", "other"],
        "catalogs": [],
        "behaviorHints": { "configurable": false, "p2p": true }
    }))
}

/// `GET /stream/{type}/{id}.json`
pub async fn stream(
    State(state): State<AppState>,
    Path((_content_type, raw_id)): Path<(String, String)>,
) -> Json<Value> {
    let id = raw_id.strip_suffix(".json").unwrap_or(&raw_id);
    let decoded = urlencoding::decode(id)
        .map(|c| c.into_owned())
        .unwrap_or_else(|_| id.to_string());

    if !decoded.starts_with("magnet:") {
        // Not something we can resolve -- e.g. a plain IMDB id meant for a
        // catalog/indexer addon. An empty list is the correct response, not
        // an error: Stremio fans this same request out to every addon.
        return Json(json!({ "streams": [] }));
    }

    let Ok(resolved) = state.engine.resolve(&decoded).await else {
        return Json(json!({ "streams": [] }));
    };

    let Some(file_idx) = resolved.suggested_file_idx else {
        return Json(json!({ "streams": [] }));
    };
    let Some(file) = resolved.file(file_idx) else {
        return Json(json!({ "streams": [] }));
    };

    let gb = file.length as f64 / 1_073_741_824.0;
    let title = resolved.name.clone().unwrap_or_else(|| file.name.clone());

    Json(json!({
        "streams": [{
            "name": "Local Gateway",
            "title": format!("{title}\n{} \u{2022} {gb:.2} GB", file.name),
            "url": format!("{}/videos/{}/{}", state.base_url, resolved.info_hash, file_idx),
            "behaviorHints": {
                "notWebReady": false,
                "bingeGroup": "local-streaming-gateway"
            }
        }]
    }))
}
