//! Schedule evaluation — cron, interval, one-shot next-fire computation.
//!
//! Replaces the stubbed `due_automations` logic in the scheduler with real
//! next-fire math. Pure functions over [`ScheduleType`] + `DateTime<Utc>` —
//! fully unit-testable with fixed times, no tokio, no clock.
//!
//! Mirrors MCode's `schedule.ts` semantics:
//! - `Cron(expr)` → parse + compute the next fire after a reference time.
//!   MCode hand-rolled a 5-field parser (TS limitation); we use the `cron`
//!   crate, which expects 6-7 fields (sec min hour dom mon dow [year]), so
//!   a bare 5-field expression is prefixed with `0 ` (seconds).
//! - `Interval(secs)` → `after + secs`.
//! - `OneShot(time)` → the scheduled time if still in the future, else `None`.
//! - `Manual` → never fires (`None`).

use chrono::{DateTime, Utc};
use std::str::FromStr;

use crate::definition::ScheduleType;

/// Compute the next fire time for a schedule after `after`. Returns `None`
/// when the schedule will never fire again (manual, or a past one-shot).
pub fn next_fire(schedule: &ScheduleType, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
    match schedule {
        ScheduleType::Manual => None,
        ScheduleType::Interval(secs) => Some(after + chrono::Duration::seconds(*secs as i64)),
        ScheduleType::OneShot(time) => {
            // Only fires if the scheduled time is still ahead.
            let target = parse_rfc3339(time)?;
            if target > after { Some(target) } else { None }
        }
        ScheduleType::Cron(expr) => next_cron_fire(expr, after),
    }
}

/// Compute the next fire for a cron expression after `after`.
///
/// The `cron` crate requires 6-7 fields (sec min hour dom mon dow [year]).
/// MCode/POSIX cron uses 5 fields (min hour dom mon dow). We prefix a bare
/// 5-field expression with `0 ` (fire at second 0) so both forms parse.
fn next_cron_fire(expr: &str, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let normalized = normalize_cron_expr(expr);
    let schedule = cron::Schedule::from_str(&normalized).ok()?;
    schedule.after(&after).next()
}

/// Prefix a 5-field cron expression with a `0` seconds field if needed.
/// Expressions that already have 6+ fields are returned unchanged.
/// Pure — unit-testable without a DateTime.
pub(crate) fn normalize_cron_expr(expr: &str) -> String {
    let trimmed = expr.trim();
    let field_count = trimmed.split_whitespace().count();
    if field_count == 5 {
        format!("0 {trimmed}")
    } else {
        trimmed.to_string()
    }
}

/// Whether a scheduled fire time is due relative to `now` (inclusive).
pub fn is_due(next_run_at: &DateTime<Utc>, now: DateTime<Utc>) -> bool {
    *next_run_at <= now
}

/// Coalesce a missed schedule: given a `next_run_at` that is in the past and a
/// schedule, compute the *next* future fire (skipping all missed occurrences).
/// Mirrors MCode's misfire policy `coalesce`/`run-latest` — never replay
/// missed fires, just resume from the next slot after now.
pub fn coalesce_missed(
    schedule: &ScheduleType,
    past_next_run_at: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    // For intervals, fast-forward arithmeticly (don't step one-by-one).
    if let ScheduleType::Interval(secs) = schedule {
        let step = chrono::Duration::seconds(*secs as i64);
        if step.num_seconds() <= 0 {
            return next_fire(schedule, now);
        }
        let mut next = past_next_run_at;
        while next <= now {
            next += step;
        }
        return Some(next);
    }
    // For cron / one-shot, just compute the next fire after now.
    next_fire(schedule, now)
}

fn parse_rfc3339(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn t(minute: u32, second: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, 0, minute, second).unwrap()
    }

    #[test]
    fn manual_never_fires() {
        assert_eq!(next_fire(&ScheduleType::Manual, t(0, 0)), None);
    }

    #[test]
    fn interval_adds_seconds() {
        let after = t(0, 0);
        let next = next_fire(&ScheduleType::Interval(300), after).unwrap();
        assert_eq!(next, t(5, 0));
    }

    #[test]
    fn one_shot_future_fires() {
        let after = t(0, 0);
        let target = "2026-01-01T00:10:00Z";
        let next = next_fire(&ScheduleType::OneShot(target.into()), after).unwrap();
        assert_eq!(next, t(10, 0));
    }

    #[test]
    fn one_shot_past_does_not_fire() {
        let after = t(10, 0);
        let target = "2026-01-01T00:05:00Z";
        assert_eq!(
            next_fire(&ScheduleType::OneShot(target.into()), after),
            None
        );
    }

    #[test]
    fn one_shot_invalid_returns_none() {
        let after = t(0, 0);
        assert_eq!(
            next_fire(&ScheduleType::OneShot("not-a-date".into()), after),
            None
        );
    }

    #[test]
    fn cron_every_minute() {
        // 5-field: minute=*, rest implied. Normalized to "0 * * * * *".
        let after = t(0, 30); // 00:00:30
        let next = next_fire(&ScheduleType::Cron("* * * * *".into()), after).unwrap();
        assert_eq!(next, t(1, 0)); // next minute boundary, second 0
    }

    #[test]
    fn cron_every_5_minutes() {
        let after = t(2, 0); // 00:02:00
        let next = next_fire(&ScheduleType::Cron("*/5 * * * *".into()), after).unwrap();
        assert_eq!(next, t(5, 0));
    }

    #[test]
    fn cron_six_field_passthrough() {
        // Already 6 fields — passed through unchanged.
        let after = t(0, 0);
        let next = next_fire(&ScheduleType::Cron("30 4 * * * *".into()), after).unwrap();
        assert_eq!(next, t(4, 30));
    }

    #[test]
    fn cron_invalid_returns_none() {
        let after = t(0, 0);
        assert_eq!(
            next_fire(&ScheduleType::Cron("not a cron".into()), after),
            None
        );
    }

    #[test]
    fn is_due_inclusive() {
        let now = t(5, 0);
        assert!(is_due(&t(5, 0), now)); // exactly now
        assert!(is_due(&t(4, 59), now)); // before now
        assert!(!is_due(&t(5, 1), now)); // after now
    }

    #[test]
    fn normalize_5_field_prepends_seconds() {
        // 5-field input → prefixed with "0 " → 6-field (sec min hour dom mon dow)
        assert_eq!(normalize_cron_expr("* * * * *"), "0 * * * * *");
        assert_eq!(normalize_cron_expr("*/5 * * * *"), "0 */5 * * * *");
        // 6-field left alone (already has a seconds field)
        assert_eq!(normalize_cron_expr("0 * * * * *"), "0 * * * * *");
        // whitespace trimmed
        assert_eq!(normalize_cron_expr("  * * * * *  "), "0 * * * * *");
    }

    #[test]
    fn coalesce_missed_interval_fast_forwards() {
        // An interval that should have fired 3 times while we were down.
        // next_run_at was 00:00, interval 60s, now is 00:03:30.
        let past_next = t(0, 0);
        let now = t(3, 30);
        let coalesced = coalesce_missed(&ScheduleType::Interval(60), past_next, now).unwrap();
        // Should jump to the first slot after now, not replay 00:01/00:02/00:03.
        assert_eq!(coalesced, t(4, 0));
        assert!(coalesced > now);
    }

    #[test]
    fn coalesce_missed_cron_resumes_from_now() {
        let past_next = t(0, 0);
        let now = t(7, 30);
        let coalesced = coalesce_missed(&ScheduleType::Cron("* * * * *".into()), past_next, now);
        assert_eq!(coalesced, Some(t(8, 0))); // next minute after now
    }
}
