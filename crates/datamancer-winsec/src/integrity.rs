//! Pure, platform-independent integrity-level classification (no `cfg`, no FFI)
//! so the full accept/reject matrix and the operator override are unit-tested in
//! CI, which cannot actually elevate or sandbox a process.
//!
//! Acceptable band: `[MEDIUM_RID, HIGH_RID)` — Medium and the rare Medium-Plus
//! (`UIAccess`) pass; Low/Untrusted are `Lowered`, High/System are `Elevated`.

/// Integrity-level RID (last sub-authority of a mandatory-label SID) for Medium.
const MEDIUM_RID: u32 = 0x2000;
/// Integrity-level RID for High (the lower bound of "elevated").
const HIGH_RID: u32 = 0x3000;

/// Coarse integrity classification used for accept/reject decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegrityClass {
    Lowered,
    Medium,
    Elevated,
}

impl IntegrityClass {
    /// Human-readable label for error/log messages.
    #[must_use]
    pub fn describe(self) -> &'static str {
        match self {
            IntegrityClass::Lowered => "below-Medium (Low/Untrusted)",
            IntegrityClass::Medium => "Medium",
            IntegrityClass::Elevated => "elevated (High/System)",
        }
    }
}

/// Classify a raw integrity RID into the acceptable band or outside it.
#[must_use]
pub fn classify(rid: u32) -> IntegrityClass {
    if rid < MEDIUM_RID {
        IntegrityClass::Lowered
    } else if rid < HIGH_RID {
        IntegrityClass::Medium
    } else {
        IntegrityClass::Elevated
    }
}

/// Whether a process at `rid` may use the control channel. The operator
/// override (`allow_any`) short-circuits to `true`.
#[must_use]
pub fn integrity_ok(rid: u32, allow_any: bool) -> bool {
    allow_any || classify(rid) == IntegrityClass::Medium
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_each_band() {
        assert_eq!(classify(0x0000), IntegrityClass::Lowered); // Untrusted
        assert_eq!(classify(0x1000), IntegrityClass::Lowered); // Low
        assert_eq!(classify(0x2000), IntegrityClass::Medium); // Medium
        assert_eq!(classify(0x2100), IntegrityClass::Medium); // Medium-Plus
        assert_eq!(classify(0x3000), IntegrityClass::Elevated); // High
        assert_eq!(classify(0x4000), IntegrityClass::Elevated); // System
    }

    #[test]
    fn ok_only_for_medium_unless_overridden() {
        assert!(integrity_ok(0x2000, false));
        assert!(integrity_ok(0x2100, false));
        assert!(!integrity_ok(0x1000, false));
        assert!(!integrity_ok(0x3000, false));
        assert!(!integrity_ok(0x4000, false));
        assert!(integrity_ok(0x1000, true));
        assert!(integrity_ok(0x3000, true));
    }

    #[test]
    fn describe_labels_each_class() {
        assert_eq!(classify(0x1000).describe(), "below-Medium (Low/Untrusted)");
        assert_eq!(classify(0x2000).describe(), "Medium");
        assert_eq!(classify(0x3000).describe(), "elevated (High/System)");
    }
}
