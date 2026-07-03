//! Pure coverage-segment bookkeeping shared by storage backends: sorted, non-overlapping half-open [from, to) segments with merge/intersect/gap queries. No I/O.

#![allow(dead_code)]

use serde::{Deserialize, Serialize};

/// Sorted, non-overlapping segments and event count.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct CoverageDoc {
    /// Sorted, non-overlapping [from, to] (in nanos).
    pub(crate) segments: Vec<(i64, i64)>,
    pub(crate) event_count: u64,
    /// Asset class of the covered instrument, recorded so the catalog can
    /// reconstruct a faithful `Instrument`. `None` for rows written before this
    /// field existed (the id and row shapes do not otherwise carry it).
    #[serde(default)]
    pub(crate) asset_class: Option<String>,
}

impl CoverageDoc {
    pub(crate) fn merge_in(&mut self, from: i64, to: i64, added_events: u64) {
        if to <= from {
            return;
        }
        self.event_count = self.event_count.saturating_add(added_events);
        let mut new_seg = (from, to);
        let mut merged: Vec<(i64, i64)> = Vec::with_capacity(self.segments.len() + 1);
        let mut consumed = false;
        for &(a, b) in &self.segments {
            if b < new_seg.0 {
                merged.push((a, b));
            } else if a > new_seg.1 {
                if !consumed {
                    merged.push(new_seg);
                    consumed = true;
                }
                merged.push((a, b));
            } else {
                new_seg.0 = new_seg.0.min(a);
                new_seg.1 = new_seg.1.max(b);
            }
        }
        if !consumed {
            merged.push(new_seg);
        }
        self.segments = merged;
    }

    pub(crate) fn intersect(&self, from: i64, to: i64) -> Option<(i64, i64)> {
        let mut best: Option<(i64, i64)> = None;
        for &(a, b) in &self.segments {
            let lo = a.max(from);
            let hi = b.min(to);
            if lo < hi && best.is_none_or(|(prev_lo, prev_hi)| hi - lo > prev_hi - prev_lo) {
                best = Some((lo, hi));
            }
        }
        best
    }

    pub(crate) fn gaps_within(&self, from: i64, to: i64) -> Vec<(i64, i64)> {
        if from >= to {
            return Vec::new();
        }
        let mut cursor = from;
        let mut gaps = Vec::new();
        for &(a, b) in &self.segments {
            if b <= cursor {
                continue;
            }
            if a >= to {
                break;
            }
            if a > cursor {
                gaps.push((cursor, a.min(to)));
            }
            cursor = cursor.max(b);
            if cursor >= to {
                break;
            }
        }
        if cursor < to {
            gaps.push((cursor, to));
        }
        gaps
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coverage_merges_overlapping_segments() {
        let mut c = CoverageDoc::default();
        c.merge_in(10, 20, 1);
        c.merge_in(15, 30, 1);
        assert_eq!(c.segments, vec![(10, 30)]);
        c.merge_in(50, 60, 1);
        assert_eq!(c.segments, vec![(10, 30), (50, 60)]);
        c.merge_in(25, 55, 1);
        assert_eq!(c.segments, vec![(10, 60)]);
    }

    #[test]
    fn coverage_gaps_within_request() {
        let mut c = CoverageDoc::default();
        c.merge_in(10, 20, 1);
        c.merge_in(40, 50, 1);
        assert_eq!(c.gaps_within(0, 60), vec![(0, 10), (20, 40), (50, 60)]);
        assert_eq!(c.gaps_within(10, 50), vec![(20, 40)]);
        assert_eq!(c.gaps_within(0, 5), vec![(0, 5)]);
        assert!(c.gaps_within(10, 20).is_empty());
    }

    #[test]
    fn coverage_intersect_picks_widest_overlap() {
        let mut c = CoverageDoc::default();
        c.merge_in(0, 100, 1);
        c.merge_in(200, 210, 1);
        assert_eq!(c.intersect(50, 150), Some((50, 100)));
        assert_eq!(c.intersect(150, 250), Some((200, 210)));
    }
}
