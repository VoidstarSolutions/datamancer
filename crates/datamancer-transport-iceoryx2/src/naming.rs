//! Versioned per-client service names, defined once for both ends.
//!
//! The wire version is embedded in the service name so a publisher and a
//! subscriber built against different wire formats can never attach to each
//! other: the [`DataPayload`](crate::DataPayload) layout alone cannot protect
//! against a *reinterpretation* of a field (same bytes, new meaning), which
//! iceoryx2's type check would happily let through. A mismatched peer simply
//! finds no service — loud absence instead of silently 1e9x-wrong sizes.
//!
//! Two types carry a name across the crate boundary instead of strings:
//! [`ServicePrefix`], a daemon's `[server].service_prefix` validated once at
//! construction, and [`DataServiceName`], one client's data-service name,
//! either derived from a prefix and a client id (the creating side) or parsed
//! from the name a daemon advertised (the opening side). One parser, one wire
//! version, and no bare string in between.

use std::fmt;
use std::str::FromStr;

use iceoryx2::prelude::ServiceName;

use crate::error::{Result, TransportError};

/// Version of the POD wire format carried on the data + announcement services.
///
/// Bump this whenever the layout **or the interpretation** of
/// [`DataPayload`](crate::DataPayload) / [`SymbolAnnouncement`](crate::SymbolAnnouncement)
/// fields changes, or the name shape itself. History: v1 (implicit,
/// unversioned service names) carried sizes as whole base units; v2 carries
/// them as raw 1e-9 `Quantity` units under a fixed `datamancer/` namespace;
/// v3 keeps the v2 payload and scopes every per-client name by the daemon's
/// configured service prefix, so the name a daemon advertises is the name of
/// the service it created.
pub const WIRE_VERSION: u32 = 3;

/// A daemon's `[server].service_prefix`, validated once: non-empty, and short
/// enough that every per-client name derived from it is a legal iceoryx2
/// [`ServiceName`]. Constructing one is the only check the prefix ever gets;
/// the sink and the subscriber take a derived [`DataServiceName`] and never
/// see the configured string.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ServicePrefix(String);

impl ServicePrefix {
    /// Validate `prefix` for the longest name it can ever produce.
    ///
    /// # Errors
    ///
    /// Returns [`TransportError::BadServiceName`] if `prefix` is empty or
    /// `{prefix}/v{WIRE_VERSION}/symbols/{u64::MAX}` is not a valid iceoryx2
    /// service name.
    pub fn new(prefix: &str) -> Result<Self> {
        if prefix.is_empty() {
            return Err(TransportError::BadServiceName(
                "service prefix is empty".to_string(),
            ));
        }
        // iceoryx2 rejects a name for being empty, for its leading characters
        // (its internal namespace) or for its length — all properties of the
        // prefix plus the longest suffix, so one check here covers every name
        // derived later (`DataServiceName::service_names` relies on this).
        let longest = DataServiceName::announcement_string(prefix, u64::MAX);
        let _: ServiceName = longest.as_str().try_into().map_err(|e| {
            TransportError::BadServiceName(format!(
                "prefix {prefix:?} makes {longest:?} invalid: {e:?}"
            ))
        })?;
        Ok(Self(prefix.to_string()))
    }

    /// The configured prefix string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ServicePrefix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for ServicePrefix {
    type Err = TransportError;

    fn from_str(prefix: &str) -> Result<Self> {
        Self::new(prefix)
    }
}

/// The data-plane service name of one client under a [`ServicePrefix`]:
/// `{prefix}/v{WIRE_VERSION}/data/{client_id}`, with the companion
/// announcement service at `{prefix}/v{WIRE_VERSION}/symbols/{client_id}`.
///
/// A daemon derives one from its prefix and a client id ([`new`](Self::new)),
/// creates the services under it and advertises it with [`Display`](fmt::Display); a
/// consumer parses the advertised string back ([`FromStr`]) and opens it. A
/// name from another wire version does not parse, so a mismatched peer is
/// refused before any shared-memory attach instead of silently finding no
/// service.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DataServiceName {
    prefix: ServicePrefix,
    client_id: u64,
}

impl DataServiceName {
    /// The name of `client_id`'s data service under `prefix`. Infallible: the
    /// prefix was validated for its longest derivable name.
    #[must_use]
    pub fn new(prefix: &ServicePrefix, client_id: u64) -> Self {
        Self {
            prefix: prefix.clone(),
            client_id,
        }
    }

    /// The prefix this name was derived from (the daemon's configured
    /// `service_prefix`).
    #[must_use]
    pub fn prefix(&self) -> &ServicePrefix {
        &self.prefix
    }

    /// The client id this name was derived from.
    #[must_use]
    pub fn client_id(&self) -> u64 {
        self.client_id
    }

    /// `{prefix}/v{WIRE_VERSION}/symbols/{client_id}`, the longer of the two
    /// derived names (used by [`ServicePrefix::new`] as the length bound).
    fn announcement_string(prefix: &str, client_id: u64) -> String {
        format!("{prefix}/v{WIRE_VERSION}/symbols/{client_id}")
    }

    /// The iceoryx2 names of the data service and its announcement service,
    /// in that order.
    pub(crate) fn service_names(&self) -> (ServiceName, ServiceName) {
        let announcement = Self::announcement_string(self.prefix.as_str(), self.client_id);
        // iceoryx2 0.10's `ServiceName::new` rejects an `iox2_` prefix and an
        // over-length name, nothing else (re-verify on an iceoryx2 bump). Both
        // names are no longer than the one `ServicePrefix::new` accepted and
        // share its leading characters, so neither check can fail here.
        let as_service_name = |name: String| -> ServiceName {
            name.as_str()
                .try_into()
                .expect("derived from a prefix validated for its longest name")
        };
        (
            as_service_name(self.to_string()),
            as_service_name(announcement),
        )
    }
}

impl fmt::Display for DataServiceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/v{WIRE_VERSION}/data/{}", self.prefix, self.client_id)
    }
}

impl FromStr for DataServiceName {
    type Err = TransportError;

    /// Accepts exactly the shape [`Display`](fmt::Display) produces for *this*
    /// [`WIRE_VERSION`]. A prefix may itself contain slashes; the last
    /// `/v{WIRE_VERSION}/data/` marker wins.
    fn from_str(name: &str) -> Result<Self> {
        let bad = || {
            TransportError::BadServiceName(format!(
                "not a v{WIRE_VERSION} data-service name: {name}"
            ))
        };
        let marker = format!("/v{WIRE_VERSION}/data/");
        let at = name.rfind(&marker).ok_or_else(bad)?;
        let id = &name[at + marker.len()..];
        if id.is_empty() || !id.bytes().all(|b| b.is_ascii_digit()) {
            return Err(bad());
        }
        let client_id = id.parse().map_err(|_| bad())?;
        let prefix = ServicePrefix::new(&name[..at]).map_err(|_| bad())?;
        Ok(Self { prefix, client_id })
    }
}

#[cfg(test)]
mod tests {
    use super::{DataServiceName, ServicePrefix, WIRE_VERSION};
    use iceoryx2::prelude::ServiceName;

    fn prefix(p: &str) -> ServicePrefix {
        ServicePrefix::new(p).expect("valid prefix")
    }

    #[test]
    fn prefixes_are_validated_for_the_longest_name_they_can_produce() {
        assert!(ServicePrefix::new("datamancerd").is_ok());
        assert!(ServicePrefix::new("datamancerd-ct-e2e-12345-7").is_ok());
        assert!(ServicePrefix::new("").is_err());
        assert!("".parse::<ServicePrefix>().is_err());
        let too_long = "p".repeat(ServiceName::max_len());
        assert!(ServicePrefix::new(&too_long).is_err());
    }

    /// The longest name a prefix can produce is the bound checked at
    /// construction, so every derived name converts without a second check.
    #[test]
    fn every_name_derived_from_a_valid_prefix_is_a_valid_service_name() {
        let longest_suffix = format!("/v{WIRE_VERSION}/symbols/{}", u64::MAX);
        let longest_prefix = "p".repeat(ServiceName::max_len() - longest_suffix.len());
        let prefix = prefix(&longest_prefix);
        for id in [0, 1, u64::MAX] {
            let (data, announcement) = DataServiceName::new(&prefix, id).service_names();
            assert_eq!(
                data.as_str(),
                format!("{longest_prefix}/v{WIRE_VERSION}/data/{id}")
            );
            assert_eq!(
                announcement.as_str(),
                format!("{longest_prefix}/v{WIRE_VERSION}/symbols/{id}")
            );
        }
        let one_too_many = format!("{longest_prefix}p");
        assert!(ServicePrefix::new(&one_too_many).is_err());
    }

    #[test]
    fn service_names_embed_the_prefix_and_the_wire_version() {
        let name = DataServiceName::new(&prefix("datamancerd"), 7);
        assert_eq!(name.to_string(), "datamancerd/v3/data/7");
        assert_eq!(name.prefix().as_str(), "datamancerd");
        assert_eq!(name.client_id(), 7);
        let (data, announcement) = name.service_names();
        assert_eq!(data.as_str(), "datamancerd/v3/data/7");
        assert_eq!(announcement.as_str(), "datamancerd/v3/symbols/7");
        assert_eq!(
            DataServiceName::new(&prefix("run-42"), 0).to_string(),
            "run-42/v3/data/0"
        );
    }

    #[test]
    fn different_prefixes_never_share_a_name() {
        let a = DataServiceName::new(&prefix("a"), 0);
        let b = DataServiceName::new(&prefix("b"), 0);
        assert_ne!(a, b);
        assert_ne!(a.to_string(), b.to_string());
        assert_ne!(a.service_names().1, b.service_names().1);
    }

    #[test]
    fn versioned_names_never_collide_with_earlier_namespaces() {
        let name = DataServiceName::new(&prefix("datamancer"), 7).to_string();
        assert_ne!(name, "datamancer/data/7");
        assert_ne!(name, "datamancer/v2/data/7");
    }

    #[test]
    fn an_advertised_name_round_trips_to_its_parts() {
        let name = DataServiceName::new(&prefix("datamancerd-ct-e2e-123-4"), 40);
        let parsed: DataServiceName = name.to_string().parse().expect("parses");
        assert_eq!(parsed, name);
        assert_eq!(parsed.prefix().as_str(), "datamancerd-ct-e2e-123-4");
        assert_eq!(parsed.client_id(), 40);
        // A prefix may itself contain slashes; the last marker wins.
        let parsed: DataServiceName = "team/box/v3/data/3".parse().expect("parses");
        assert_eq!(parsed, DataServiceName::new(&prefix("team/box"), 3));
    }

    #[test]
    fn other_shapes_are_rejected_with_the_name_in_the_error() {
        for bad in [
            "datamancer/v2/data/3",
            "datamancer/data/3",
            "/v3/data/3",
            "p/v3/data/",
            "p/v3/data/not-a-number",
            "p/v3/data/-1",
            "p/v3/data/99999999999999999999",
            "p/v3/symbols/3",
            "3",
        ] {
            let err = bad.parse::<DataServiceName>().unwrap_err();
            assert!(err.to_string().contains(bad), "{bad}: {err}");
        }
    }
}
