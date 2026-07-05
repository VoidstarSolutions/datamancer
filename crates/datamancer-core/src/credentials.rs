//! Provider credential shapes (spec 2026-07-05, cycle 2).
//!
//! Pure serde types — storage and transport live elsewhere
//! (`datamancer-credentials` for the store, the UDS control surface for the
//! wire). Tagged per provider *shape*, not a universal key/secret pair:
//! IBKR-style `Gateway` credentials contain no secret at all.

use serde::{Deserialize, Serialize};

/// Credentials for one provider, tagged by shape.
///
/// `Gateway` is the IBKR-style shape reserved by the spec appendix: an
/// attach-to-local-companion "credential" that contains no secret. Nothing
/// consumes it yet; the wire tag is stable now so shipped consumers already
/// parse it.
#[non_exhaustive]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProviderCredentials {
    /// A key-id + secret pair (Alpaca-style).
    ApiKeyPair { key_id: String, secret: String },
    /// A local companion-process endpoint (IBKR-style; reserved).
    Gateway {
        host: String,
        port: u16,
        client_id: u32,
    },
}

impl std::fmt::Debug for ProviderCredentials {
    /// Redacts secret material; key ids and endpoints are diagnostic, not
    /// secret.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ApiKeyPair { key_id, .. } => f
                .debug_struct("ApiKeyPair")
                .field("key_id", key_id)
                .field("secret", &"********")
                .finish(),
            Self::Gateway {
                host,
                port,
                client_id,
            } => f
                .debug_struct("Gateway")
                .field("host", host)
                .field("port", port)
                .field("client_id", client_id)
                .finish(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ProviderCredentials;

    #[test]
    fn api_key_pair_round_trips_with_stable_wire_tags() {
        let creds = ProviderCredentials::ApiKeyPair {
            key_id: "AKID".to_string(),
            secret: "s3cret".to_string(),
        };
        let json = serde_json::to_string(&creds).unwrap();
        assert_eq!(
            json,
            r#"{"type":"api_key_pair","key_id":"AKID","secret":"s3cret"}"#
        );
        let back: ProviderCredentials = serde_json::from_str(&json).unwrap();
        assert_eq!(back, creds);
    }

    #[test]
    fn gateway_round_trips_and_carries_no_secret() {
        let creds = ProviderCredentials::Gateway {
            host: "127.0.0.1".to_string(),
            port: 4001,
            client_id: 7,
        };
        let json = serde_json::to_string(&creds).unwrap();
        assert_eq!(
            json,
            r#"{"type":"gateway","host":"127.0.0.1","port":4001,"client_id":7}"#
        );
        assert_eq!(
            serde_json::from_str::<ProviderCredentials>(&json).unwrap(),
            creds
        );
    }

    #[test]
    fn debug_never_reveals_the_secret() {
        let creds = ProviderCredentials::ApiKeyPair {
            key_id: "AKID".to_string(),
            secret: "s3cret".to_string(),
        };
        let debug = format!("{creds:?}");
        assert!(
            !debug.contains("s3cret"),
            "secret leaked into Debug: {debug}"
        );
        assert!(
            debug.contains("AKID"),
            "key id is not secret and aids diagnosis"
        );
    }
}
