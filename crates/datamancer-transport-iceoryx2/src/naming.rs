//! Versioned per-client service names, defined once for both ends.
//!
//! The wire version is embedded in the service name so a publisher and a
//! subscriber built against different wire formats can never attach to each
//! other: the [`DataPayload`](crate::DataPayload) layout alone cannot protect
//! against a *reinterpretation* of a field (same bytes, new meaning), which
//! iceoryx2's type check would happily let through. A mismatched peer simply
//! finds no service — loud absence instead of silently 1e9x-wrong sizes.
//!
//! Three types carry a name across the crate boundary instead of strings:
//! [`ServicePrefix`], a daemon's `[server].service_prefix` validated once at
//! construction; [`BootScopedPrefix`], that prefix scoped to one daemon
//! process and the only prefix a name is ever derived from; and
//! [`DataServiceName`], one client's data-service name, either derived from a
//! boot-scoped prefix and a client id (the creating side) or parsed from the
//! name a daemon advertised (the opening side). One parser, one wire version,
//! and no bare string in between.

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
/// [`ServiceName`]. Constructing one is the only check the configured value
/// ever gets. Names are not derived from it directly but from its
/// [`boot_scoped`](Self::boot_scoped) form, so the configured string never
/// reaches the sink or the subscriber.
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

    /// Scope this prefix to one daemon process: `{prefix}/{unix_secs}-{pid}`.
    /// The result is what names are derived from, so two processes with the
    /// same configuration never create the same name, whatever happened to
    /// the earlier one.
    ///
    /// # Errors
    ///
    /// Returns [`TransportError::BadServiceName`] if the scoped prefix no
    /// longer fits the longest name it has to produce.
    pub fn boot_scoped(&self, unix_secs: u64, pid: u32) -> Result<BootScopedPrefix> {
        Self::new(&format!("{self}/{unix_secs}-{pid}")).map(BootScopedPrefix)
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

/// A [`ServicePrefix`] scoped to one daemon process,
/// `{configured}/{unix_secs}-{pid}`, obtained only through
/// [`ServicePrefix::boot_scoped`]. It is the sole input to
/// [`DataServiceName::new`]: a daemon cannot create a name under its bare
/// configured prefix, so a restart never re-creates a crashed predecessor's
/// names and never meets the stale service such a predecessor can leave
/// behind ([`TransportError::StaleService`]).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BootScopedPrefix(ServicePrefix);

impl BootScopedPrefix {
    /// The scoped prefix string, `{configured}/{unix_secs}-{pid}`.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for BootScopedPrefix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// The data-plane service name of one client under a daemon's
/// [`BootScopedPrefix`]: `{prefix}/v{WIRE_VERSION}/data/{client_id}`, with
/// the companion announcement service at
/// `{prefix}/v{WIRE_VERSION}/symbols/{client_id}`.
///
/// A daemon derives one from its boot-scoped prefix and a client id
/// ([`new`](Self::new)), creates the services under it and advertises it with
/// [`Display`](fmt::Display); a consumer parses the advertised string back
/// ([`FromStr`]) and opens it. A name from another wire version does not
/// parse, so a mismatched peer is refused before any shared-memory attach
/// instead of silently finding no service.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DataServiceName {
    prefix: ServicePrefix,
    client_id: u64,
}

impl DataServiceName {
    /// The name of `client_id`'s data service under `prefix`. Infallible: the
    /// prefix was validated for its longest derivable name when it was
    /// scoped.
    #[must_use]
    pub fn new(prefix: &BootScopedPrefix, client_id: u64) -> Self {
        Self {
            prefix: prefix.0.clone(),
            client_id,
        }
    }

    /// Everything before `/v{WIRE_VERSION}/data/`: the daemon's boot-scoped
    /// prefix, which a consumer treats as opaque.
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

    /// Accepts exactly the shape [`Display`](fmt::Display) produces for
    /// *this* [`WIRE_VERSION`]. A prefix may itself contain slashes; the last
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
    use super::{BootScopedPrefix, DataServiceName, ServicePrefix, WIRE_VERSION};
    use iceoryx2::prelude::ServiceName;

    const SECS: u64 = 1_729_600_000;
    const PID: u32 = 4321;

    fn scoped(configured: &str) -> BootScopedPrefix {
        ServicePrefix::new(configured)
            .expect("valid prefix")
            .boot_scoped(SECS, PID)
            .expect("fits")
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

    #[test]
    fn boot_scoping_appends_secs_and_pid_and_keeps_the_budget() {
        assert_eq!(
            scoped("datamancerd").as_str(),
            "datamancerd/1729600000-4321"
        );
        assert_eq!(
            scoped("datamancerd").to_string(),
            "datamancerd/1729600000-4321"
        );
        // A configured prefix can be valid on its own and still leave no room
        // for the boot id: scoping is where that is caught.
        let longest_suffix = format!("/v{WIRE_VERSION}/symbols/{}", u64::MAX);
        let budget = ServiceName::max_len() - longest_suffix.len();
        let configured = ServicePrefix::new(&"p".repeat(budget)).expect("fits unscoped");
        assert!(configured.boot_scoped(SECS, PID).is_err());
    }

    /// The longest name a scoped prefix can produce is the bound checked when
    /// it is scoped, so every derived name converts without a second check.
    #[test]
    fn every_name_derived_from_a_scoped_prefix_is_a_valid_service_name() {
        let longest_suffix = format!("/v{WIRE_VERSION}/symbols/{}", u64::MAX);
        let longest_boot_id = format!("/{}-{}", u64::MAX, u32::MAX);
        let configured_len = ServiceName::max_len() - longest_suffix.len() - longest_boot_id.len();
        let configured = ServicePrefix::new(&"p".repeat(configured_len)).expect("valid");
        let prefix = configured
            .boot_scoped(u64::MAX, u32::MAX)
            .expect("exactly at the budget");
        for id in [0, 1, u64::MAX] {
            let (data, announcement) = DataServiceName::new(&prefix, id).service_names();
            assert_eq!(data.as_str(), format!("{prefix}/v{WIRE_VERSION}/data/{id}"));
            assert_eq!(
                announcement.as_str(),
                format!("{prefix}/v{WIRE_VERSION}/symbols/{id}")
            );
        }
        let one_too_many = ServicePrefix::new(&"p".repeat(configured_len + 1)).expect("valid");
        assert!(one_too_many.boot_scoped(u64::MAX, u32::MAX).is_err());
    }

    #[test]
    fn service_names_embed_the_scoped_prefix_and_the_wire_version() {
        let name = DataServiceName::new(&scoped("datamancerd"), 7);
        assert_eq!(name.to_string(), "datamancerd/1729600000-4321/v3/data/7");
        assert_eq!(name.prefix().as_str(), "datamancerd/1729600000-4321");
        assert_eq!(name.client_id(), 7);
        let (data, announcement) = name.service_names();
        assert_eq!(data.as_str(), "datamancerd/1729600000-4321/v3/data/7");
        assert_eq!(
            announcement.as_str(),
            "datamancerd/1729600000-4321/v3/symbols/7"
        );
        assert_eq!(
            DataServiceName::new(&scoped("run-42"), 0).to_string(),
            "run-42/1729600000-4321/v3/data/0"
        );
    }

    #[test]
    fn different_prefixes_or_boots_never_share_a_name() {
        let a = DataServiceName::new(&scoped("a"), 0);
        let b = DataServiceName::new(&scoped("b"), 0);
        assert_ne!(a, b);
        assert_ne!(a.to_string(), b.to_string());
        assert_ne!(a.service_names().1, b.service_names().1);
        let configured = ServicePrefix::new("a").expect("valid");
        let earlier = configured.boot_scoped(SECS, PID).expect("fits");
        let later = configured.boot_scoped(SECS + 1, PID).expect("fits");
        assert_ne!(
            DataServiceName::new(&earlier, 0),
            DataServiceName::new(&later, 0)
        );
    }

    #[test]
    fn versioned_names_never_collide_with_earlier_namespaces() {
        let name = DataServiceName::new(&scoped("datamancer"), 7).to_string();
        assert_ne!(name, "datamancer/data/7");
        assert_ne!(name, "datamancer/v2/data/7");
        assert_ne!(name, "datamancer/v3/data/7");
    }

    #[test]
    fn an_advertised_name_round_trips_to_its_parts() {
        let name = DataServiceName::new(&scoped("datamancerd-ct-e2e-123-4"), 40);
        let parsed: DataServiceName = name.to_string().parse().expect("parses");
        assert_eq!(parsed, name);
        assert_eq!(
            parsed.prefix().as_str(),
            "datamancerd-ct-e2e-123-4/1729600000-4321"
        );
        assert_eq!(parsed.client_id(), 40);
        // A prefix may itself contain slashes; the last marker wins.
        let parsed: DataServiceName = "team/box/v3/data/3".parse().expect("parses");
        assert_eq!(parsed.prefix().as_str(), "team/box");
        assert_eq!(parsed.client_id(), 3);
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
