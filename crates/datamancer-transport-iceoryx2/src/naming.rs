//! Versioned per-client service names, defined once for both ends.
//!
//! The wire version is embedded in the service name so a publisher and a
//! subscriber built against different wire formats can never attach to each
//! other: the [`DataPayload`](crate::DataPayload) layout alone cannot protect
//! against a *reinterpretation* of a field (same bytes, new meaning), which
//! iceoryx2's type check would happily let through. A mismatched peer simply
//! finds no service — loud absence instead of silently 1e9x-wrong sizes.

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

/// Data-plane service name for `client_id` under `prefix`:
/// `{prefix}/v{WIRE_VERSION}/data/{client_id}`.
///
/// `prefix` is the daemon's `[server].service_prefix`; two daemons with
/// different prefixes never share a service name.
#[must_use]
pub fn data_service_name(prefix: &str, client_id: u64) -> String {
    format!("{prefix}/v{WIRE_VERSION}/data/{client_id}")
}

/// Symbol-announcement service name for `client_id` under `prefix`:
/// `{prefix}/v{WIRE_VERSION}/symbols/{client_id}`.
#[must_use]
pub fn announcement_service_name(prefix: &str, client_id: u64) -> String {
    format!("{prefix}/v{WIRE_VERSION}/symbols/{client_id}")
}

/// Split an advertised data-service name back into `(prefix, client_id)`.
///
/// Accepts exactly the shape [`data_service_name`] produces for *this*
/// [`WIRE_VERSION`]; a name from another wire version, or with an empty
/// prefix, returns `None`. The companion announcement name is then
/// [`announcement_service_name`]`(prefix, client_id)`.
#[must_use]
pub fn parse_data_service_name(name: &str) -> Option<(&str, u64)> {
    let marker = format!("/v{WIRE_VERSION}/data/");
    let at = name.rfind(&marker)?;
    let prefix = &name[..at];
    if prefix.is_empty() {
        return None;
    }
    let id = &name[at + marker.len()..];
    if id.is_empty() || !id.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some((prefix, id.parse().ok()?))
}

/// Check that `prefix` yields valid iceoryx2 service names for every client
/// id, so a daemon can refuse a bad `[server].service_prefix` at boot instead
/// of failing its first `open-client`.
///
/// # Errors
///
/// Returns [`TransportError::BadServiceName`] if the prefix is empty or the
/// longest name it can produce exceeds iceoryx2's service-name length.
pub fn validate_service_prefix(prefix: &str) -> crate::error::Result<()> {
    if prefix.is_empty() {
        return Err(crate::error::TransportError::BadServiceName(
            "service prefix is empty".to_string(),
        ));
    }
    let longest = announcement_service_name(prefix, u64::MAX);
    let _: iceoryx2::prelude::ServiceName = longest.as_str().try_into().map_err(|e| {
        crate::error::TransportError::BadServiceName(format!(
            "prefix {prefix:?} makes {longest:?} invalid: {e:?}"
        ))
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        announcement_service_name, data_service_name, parse_data_service_name,
        validate_service_prefix,
    };

    #[test]
    fn prefixes_are_validated_for_the_longest_name_they_can_produce() {
        assert!(validate_service_prefix("datamancerd").is_ok());
        assert!(validate_service_prefix("datamancerd-ct-e2e-12345-7").is_ok());
        assert!(validate_service_prefix("").is_err());
        let too_long = "p".repeat(iceoryx2::prelude::ServiceName::max_len());
        assert!(validate_service_prefix(&too_long).is_err());
    }

    #[test]
    fn service_names_embed_the_prefix_and_the_wire_version() {
        assert_eq!(data_service_name("datamancerd", 7), "datamancerd/v3/data/7");
        assert_eq!(
            announcement_service_name("datamancerd", 7),
            "datamancerd/v3/symbols/7"
        );
        assert_eq!(data_service_name("run-42", 0), "run-42/v3/data/0");
    }

    #[test]
    fn different_prefixes_never_share_a_name() {
        assert_ne!(data_service_name("a", 0), data_service_name("b", 0));
        assert_ne!(
            announcement_service_name("a", 0),
            announcement_service_name("b", 0)
        );
    }

    #[test]
    fn versioned_names_never_collide_with_earlier_namespaces() {
        assert_ne!(data_service_name("datamancer", 7), "datamancer/data/7");
        assert_ne!(data_service_name("datamancer", 7), "datamancer/v2/data/7");
    }

    #[test]
    fn an_advertised_name_round_trips_to_its_parts() {
        let name = data_service_name("datamancerd-ct-e2e-123-4", 40);
        assert_eq!(
            parse_data_service_name(&name),
            Some(("datamancerd-ct-e2e-123-4", 40))
        );
        // A prefix may itself contain slashes; the last marker wins.
        assert_eq!(
            parse_data_service_name("team/box/v3/data/3"),
            Some(("team/box", 3))
        );
    }

    #[test]
    fn other_shapes_are_rejected() {
        assert_eq!(parse_data_service_name("datamancer/v2/data/3"), None);
        assert_eq!(parse_data_service_name("datamancer/data/3"), None);
        assert_eq!(parse_data_service_name("/v3/data/3"), None);
        assert_eq!(parse_data_service_name("p/v3/data/"), None);
        assert_eq!(parse_data_service_name("p/v3/data/not-a-number"), None);
        assert_eq!(parse_data_service_name("p/v3/data/-1"), None);
        assert_eq!(parse_data_service_name("p/v3/symbols/3"), None);
        assert_eq!(parse_data_service_name("3"), None);
    }
}
