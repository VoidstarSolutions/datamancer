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
/// fields changes. History: v1 (implicit, unversioned service names) carried
/// sizes as whole base units; v2 carries them as raw 1e-9 `Quantity` units.
pub const WIRE_VERSION: u32 = 2;

/// Data-plane service name for `client_id`.
#[must_use]
pub fn data_service_name(client_id: u64) -> String {
    format!("datamancer/v{WIRE_VERSION}/data/{client_id}")
}

/// Symbol-announcement service name for `client_id`.
#[must_use]
pub fn announcement_service_name(client_id: u64) -> String {
    format!("datamancer/v{WIRE_VERSION}/symbols/{client_id}")
}

#[cfg(test)]
mod tests {
    use super::{announcement_service_name, data_service_name};

    #[test]
    fn service_names_embed_the_wire_version() {
        assert_eq!(data_service_name(7), "datamancer/v2/data/7");
        assert_eq!(announcement_service_name(7), "datamancer/v2/symbols/7");
    }

    #[test]
    fn versioned_names_never_collide_with_the_unversioned_v1_namespace() {
        assert_ne!(data_service_name(7), "datamancer/data/7");
        assert_ne!(announcement_service_name(7), "datamancer/symbols/7");
    }
}
