use std::time::SystemTime;

use chrono::{Datelike, Duration, NaiveDate, Timelike, Utc};
use serde::{Deserialize, Serialize};

/// Time-based partition granularity — matches Java archiver's PartitionGranularity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PartitionGranularity {
    #[serde(rename = "5min")]
    FiveMin,
    #[serde(rename = "15min")]
    FifteenMin,
    #[serde(rename = "30min")]
    ThirtyMin,
    #[serde(rename = "hour")]
    Hour,
    #[serde(rename = "day")]
    Day,
    #[serde(rename = "month")]
    Month,
    #[serde(rename = "year")]
    Year,
}

impl PartitionGranularity {
    pub fn approx_seconds(self) -> u64 {
        match self {
            Self::FiveMin => 5 * 60,
            Self::FifteenMin => 15 * 60,
            Self::ThirtyMin => 30 * 60,
            Self::Hour => 3600,
            Self::Day => 86400,
            Self::Month => 31 * 86400,
            Self::Year => 366 * 86400,
        }
    }

    fn approx_minutes(self) -> u32 {
        (self.approx_seconds() / 60) as u32
    }
}

/// `SystemTime` → `DateTime<Utc>` for partition arithmetic. Timestamps
/// beyond chrono's calendar clamp to `MIN_UTC` / `MAX_UTC` instead of
/// panicking in `From`; the storage write path rejects such samples
/// separately (`ArchiverSample::decompose_timestamp`), so clamping
/// here only keeps path derivation and range walks total.
fn utc_datetime_clamped(ts: SystemTime) -> chrono::DateTime<Utc> {
    crate::types::utc_datetime_checked(ts).unwrap_or(if ts < SystemTime::UNIX_EPOCH {
        chrono::DateTime::<Utc>::MIN_UTC
    } else {
        chrono::DateTime::<Utc>::MAX_UTC
    })
}

/// Generate the partition name string for a given timestamp.
/// Matches Java archiver's TimeUtils.getPartitionName().
///
/// Examples:
///   Year  → "2024"
///   Month → "2024_03"
///   Day   → "2024_03_15"
///   Hour  → "2024_03_15_09"
///   5Min  → "2024_03_15_09_30"
pub fn partition_name(ts: SystemTime, granularity: PartitionGranularity) -> String {
    let dt = utc_datetime_clamped(ts);
    let y = dt.year();
    let m = dt.month();
    let d = dt.day();
    let h = dt.hour();
    let min = dt.minute();

    match granularity {
        PartitionGranularity::Year => format!("{y}"),
        PartitionGranularity::Month => format!("{y}_{m:02}"),
        PartitionGranularity::Day => format!("{y}_{m:02}_{d:02}"),
        PartitionGranularity::Hour => format!("{y}_{m:02}_{d:02}_{h:02}"),
        PartitionGranularity::FiveMin
        | PartitionGranularity::FifteenMin
        | PartitionGranularity::ThirtyMin => {
            let approx_min = granularity.approx_minutes();
            let start_min = (min / approx_min) * approx_min;
            format!("{y}_{m:02}_{d:02}_{h:02}_{start_min:02}")
        }
    }
}

/// List all partition names that overlap with [start, end].
pub fn partitions_in_range(
    start: SystemTime,
    end: SystemTime,
    granularity: PartitionGranularity,
) -> Vec<String> {
    let mut names = Vec::new();
    let mut current = start;
    loop {
        let name = partition_name(current, granularity);
        if names.last().map(|n: &String| n.as_str()) != Some(&name) {
            names.push(name);
        }
        if current >= end {
            break;
        }
        current = next_partition_start(current, granularity);
        if current > end {
            // Include the last partition.
            let name = partition_name(end, granularity);
            if names.last().map(|n: &String| n.as_str()) != Some(&name) {
                names.push(name);
            }
            break;
        }
    }
    names
}

/// Get the most recent N partition names up to and including the one containing `ts`.
pub fn recent_partitions(
    ts: SystemTime,
    granularity: PartitionGranularity,
    count: usize,
) -> Vec<String> {
    let mut names = Vec::new();
    let mut current = ts;
    for _ in 0..count {
        names.push(partition_name(current, granularity));
        current = prev_partition_end(current, granularity);
    }
    names.reverse();
    names
}

/// Compute the start of the partition CONTAINING `ts` — the lower bound of
/// that partition's half-open time range `[partition_start, next_partition_start)`.
pub fn partition_start(ts: SystemTime, granularity: PartitionGranularity) -> SystemTime {
    let dt = utc_datetime_clamped(ts);

    let start = match granularity {
        PartitionGranularity::Year => NaiveDate::from_ymd_opt(dt.year(), 1, 1)
            .expect("Jan 1 is always valid")
            .and_hms_opt(0, 0, 0)
            .expect("midnight is always valid")
            .and_utc(),
        PartitionGranularity::Month => NaiveDate::from_ymd_opt(dt.year(), dt.month(), 1)
            .expect("1st of month is always valid")
            .and_hms_opt(0, 0, 0)
            .expect("midnight is always valid")
            .and_utc(),
        PartitionGranularity::Day => dt
            .date_naive()
            .and_hms_opt(0, 0, 0)
            .expect("midnight is always valid")
            .and_utc(),
        PartitionGranularity::Hour => dt
            .date_naive()
            .and_hms_opt(dt.hour(), 0, 0)
            .expect("hour from valid DateTime")
            .and_utc(),
        PartitionGranularity::FiveMin
        | PartitionGranularity::FifteenMin
        | PartitionGranularity::ThirtyMin => {
            let approx_min = granularity.approx_minutes();
            let start_min = (dt.minute() / approx_min) * approx_min;
            dt.date_naive()
                .and_hms_opt(dt.hour(), start_min, 0)
                .expect("aligned minute from valid DateTime")
                .and_utc()
        }
    };

    start.into()
}

/// Compute the start of the next partition after the one containing `ts`.
///
/// Saturates at the end of chrono's calendar instead of panicking:
/// past `MAX_UTC` there is no next partition, and callers that walk
/// forward (`partitions_in_range`) stop when the value no longer
/// advances.
pub fn next_partition_start(ts: SystemTime, granularity: PartitionGranularity) -> SystemTime {
    let dt = utc_datetime_clamped(ts);
    let midnight = |d: NaiveDate| d.and_hms_opt(0, 0, 0).map(|n| n.and_utc());

    let next = match granularity {
        PartitionGranularity::Year => {
            NaiveDate::from_ymd_opt(dt.year() + 1, 1, 1).and_then(midnight)
        }
        PartitionGranularity::Month => {
            let (y, m) = if dt.month() == 12 {
                (dt.year() + 1, 1)
            } else {
                (dt.year(), dt.month() + 1)
            };
            NaiveDate::from_ymd_opt(y, m, 1).and_then(midnight)
        }
        PartitionGranularity::Day => dt
            .date_naive()
            .checked_add_signed(Duration::days(1))
            .and_then(midnight),
        PartitionGranularity::Hour => dt
            .date_naive()
            .and_hms_opt(dt.hour(), 0, 0)
            .map(|n| n.and_utc())
            .and_then(|h| h.checked_add_signed(Duration::hours(1))),
        PartitionGranularity::FiveMin
        | PartitionGranularity::FifteenMin
        | PartitionGranularity::ThirtyMin => {
            let approx_min = granularity.approx_minutes();
            let start_min = (dt.minute() / approx_min) * approx_min;
            dt.date_naive()
                .and_hms_opt(dt.hour(), start_min, 0)
                .map(|n| n.and_utc())
                .and_then(|s| s.checked_add_signed(Duration::minutes(approx_min as i64)))
        }
    };

    next.unwrap_or(chrono::DateTime::<Utc>::MAX_UTC).into()
}

/// Compute the last moment of the previous partition before `ts`.
/// Saturates at `MIN_UTC` instead of panicking (see
/// `next_partition_start`).
fn prev_partition_end(ts: SystemTime, granularity: PartitionGranularity) -> SystemTime {
    let dt = utc_datetime_clamped(ts);
    let last_second = |d: NaiveDate| d.and_hms_opt(23, 59, 59).map(|n| n.and_utc());

    let prev_end = match granularity {
        PartitionGranularity::Year => {
            NaiveDate::from_ymd_opt(dt.year() - 1, 12, 31).and_then(last_second)
        }
        PartitionGranularity::Month => NaiveDate::from_ymd_opt(dt.year(), dt.month(), 1)
            .and_then(|d| d.checked_sub_signed(Duration::days(1)))
            .and_then(last_second),
        PartitionGranularity::Day => dt
            .date_naive()
            .checked_sub_signed(Duration::days(1))
            .and_then(last_second),
        PartitionGranularity::Hour => dt
            .date_naive()
            .and_hms_opt(dt.hour(), 0, 0)
            .map(|n| n.and_utc())
            .and_then(|h| h.checked_sub_signed(Duration::seconds(1))),
        PartitionGranularity::FiveMin
        | PartitionGranularity::FifteenMin
        | PartitionGranularity::ThirtyMin => {
            let approx_min = granularity.approx_minutes();
            let start_min = (dt.minute() / approx_min) * approx_min;
            dt.date_naive()
                .and_hms_opt(dt.hour(), start_min, 0)
                .map(|n| n.and_utc())
                .and_then(|s| s.checked_sub_signed(Duration::seconds(1)))
        }
    };

    prev_end.unwrap_or(chrono::DateTime::<Utc>::MIN_UTC).into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    /// Out-of-calendar timestamps must clamp/saturate, never panic:
    /// `file_path_for` runs inside the shard's append and the ETL move.
    #[test]
    fn partition_arithmetic_saturates_out_of_range() {
        let huge = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1 << 62);
        let max: SystemTime = chrono::DateTime::<Utc>::MAX_UTC.into();
        let min: SystemTime = chrono::DateTime::<Utc>::MIN_UTC.into();
        for g in [
            PartitionGranularity::Year,
            PartitionGranularity::Month,
            PartitionGranularity::Day,
            PartitionGranularity::Hour,
            PartitionGranularity::FiveMin,
        ] {
            assert_eq!(partition_name(huge, g), partition_name(max, g));
            assert!(next_partition_start(max, g) >= max, "{g:?}");
            assert!(prev_partition_end(min, g) <= min, "{g:?}");
            assert!(partition_start(huge, g) <= huge, "{g:?}");
            assert_eq!(partitions_in_range(max, max, g).len(), 1, "{g:?}");
        }
    }

    #[test]
    fn test_partition_name_year() {
        let ts: SystemTime = Utc.with_ymd_and_hms(2024, 6, 15, 10, 30, 0).unwrap().into();
        assert_eq!(partition_name(ts, PartitionGranularity::Year), "2024");
    }

    #[test]
    fn test_partition_name_month() {
        let ts: SystemTime = Utc.with_ymd_and_hms(2024, 3, 15, 10, 30, 0).unwrap().into();
        assert_eq!(partition_name(ts, PartitionGranularity::Month), "2024_03");
    }

    #[test]
    fn test_partition_name_day() {
        let ts: SystemTime = Utc.with_ymd_and_hms(2024, 3, 5, 10, 30, 0).unwrap().into();
        assert_eq!(partition_name(ts, PartitionGranularity::Day), "2024_03_05");
    }

    #[test]
    fn test_partition_name_hour() {
        let ts: SystemTime = Utc.with_ymd_and_hms(2024, 3, 5, 9, 30, 0).unwrap().into();
        assert_eq!(
            partition_name(ts, PartitionGranularity::Hour),
            "2024_03_05_09"
        );
    }

    #[test]
    fn test_partition_name_15min() {
        let ts: SystemTime = Utc.with_ymd_and_hms(2024, 3, 5, 9, 47, 0).unwrap().into();
        assert_eq!(
            partition_name(ts, PartitionGranularity::FifteenMin),
            "2024_03_05_09_45"
        );
    }

    #[test]
    fn test_partition_start_bounds_ts() {
        let ts: SystemTime = Utc.with_ymd_and_hms(2024, 3, 5, 9, 47, 30).unwrap().into();
        // Every granularity: partition_start <= ts < next_partition_start, and
        // the start lands on the exact partition boundary.
        let start = partition_start(ts, PartitionGranularity::Hour);
        let end = next_partition_start(ts, PartitionGranularity::Hour);
        assert!(
            start <= ts && ts < end,
            "ts falls in its own hour partition"
        );
        assert_eq!(
            start,
            Utc.with_ymd_and_hms(2024, 3, 5, 9, 0, 0).unwrap().into()
        );
        assert_eq!(
            end,
            Utc.with_ymd_and_hms(2024, 3, 5, 10, 0, 0).unwrap().into()
        );
        assert_eq!(
            partition_start(ts, PartitionGranularity::Day),
            Utc.with_ymd_and_hms(2024, 3, 5, 0, 0, 0).unwrap().into()
        );
        assert_eq!(
            partition_start(ts, PartitionGranularity::Month),
            Utc.with_ymd_and_hms(2024, 3, 1, 0, 0, 0).unwrap().into()
        );
        assert_eq!(
            partition_start(ts, PartitionGranularity::Year),
            Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap().into()
        );
        assert_eq!(
            partition_start(ts, PartitionGranularity::FifteenMin),
            Utc.with_ymd_and_hms(2024, 3, 5, 9, 45, 0).unwrap().into()
        );
    }

    #[test]
    fn test_partitions_in_range() {
        let start: SystemTime = Utc.with_ymd_and_hms(2024, 3, 5, 10, 0, 0).unwrap().into();
        let end: SystemTime = Utc.with_ymd_and_hms(2024, 3, 5, 12, 30, 0).unwrap().into();
        let names = partitions_in_range(start, end, PartitionGranularity::Hour);
        assert_eq!(
            names,
            vec!["2024_03_05_10", "2024_03_05_11", "2024_03_05_12"]
        );
    }
}
