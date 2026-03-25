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
    let dt = chrono::DateTime::<Utc>::from(ts);
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

/// Compute the start of the next partition after the one containing `ts`.
pub fn next_partition_start(ts: SystemTime, granularity: PartitionGranularity) -> SystemTime {
    let dt = chrono::DateTime::<Utc>::from(ts);

    let next = match granularity {
        PartitionGranularity::Year => {
            NaiveDate::from_ymd_opt(dt.year() + 1, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc()
        }
        PartitionGranularity::Month => {
            let (y, m) = if dt.month() == 12 {
                (dt.year() + 1, 1)
            } else {
                (dt.year(), dt.month() + 1)
            };
            NaiveDate::from_ymd_opt(y, m, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc()
        }
        PartitionGranularity::Day => {
            (dt.date_naive() + Duration::days(1))
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc()
        }
        PartitionGranularity::Hour => {
            let current_hour = dt
                .date_naive()
                .and_hms_opt(dt.hour(), 0, 0)
                .unwrap()
                .and_utc();
            current_hour + Duration::hours(1)
        }
        PartitionGranularity::FiveMin
        | PartitionGranularity::FifteenMin
        | PartitionGranularity::ThirtyMin => {
            let approx_min = granularity.approx_minutes();
            let start_min = (dt.minute() / approx_min) * approx_min;
            let current_start = dt
                .date_naive()
                .and_hms_opt(dt.hour(), start_min, 0)
                .unwrap()
                .and_utc();
            current_start + Duration::minutes(approx_min as i64)
        }
    };

    next.into()
}

/// Compute the last moment of the previous partition before `ts`.
fn prev_partition_end(ts: SystemTime, granularity: PartitionGranularity) -> SystemTime {
    let dt = chrono::DateTime::<Utc>::from(ts);

    let prev_end = match granularity {
        PartitionGranularity::Year => {
            NaiveDate::from_ymd_opt(dt.year() - 1, 12, 31)
                .unwrap()
                .and_hms_opt(23, 59, 59)
                .unwrap()
                .and_utc()
        }
        PartitionGranularity::Month => {
            let first_of_month = NaiveDate::from_ymd_opt(dt.year(), dt.month(), 1).unwrap();
            let prev = first_of_month - Duration::days(1);
            prev.and_hms_opt(23, 59, 59).unwrap().and_utc()
        }
        PartitionGranularity::Day => {
            let prev = dt.date_naive() - Duration::days(1);
            prev.and_hms_opt(23, 59, 59).unwrap().and_utc()
        }
        PartitionGranularity::Hour => {
            dt.date_naive()
                .and_hms_opt(dt.hour(), 0, 0)
                .unwrap()
                .and_utc()
                - Duration::seconds(1)
        }
        PartitionGranularity::FiveMin
        | PartitionGranularity::FifteenMin
        | PartitionGranularity::ThirtyMin => {
            let approx_min = granularity.approx_minutes();
            let start_min = (dt.minute() / approx_min) * approx_min;
            dt.date_naive()
                .and_hms_opt(dt.hour(), start_min, 0)
                .unwrap()
                .and_utc()
                - Duration::seconds(1)
        }
    };

    prev_end.into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_partition_name_year() {
        let ts: SystemTime = Utc
            .with_ymd_and_hms(2024, 6, 15, 10, 30, 0)
            .unwrap()
            .into();
        assert_eq!(partition_name(ts, PartitionGranularity::Year), "2024");
    }

    #[test]
    fn test_partition_name_month() {
        let ts: SystemTime = Utc
            .with_ymd_and_hms(2024, 3, 15, 10, 30, 0)
            .unwrap()
            .into();
        assert_eq!(partition_name(ts, PartitionGranularity::Month), "2024_03");
    }

    #[test]
    fn test_partition_name_day() {
        let ts: SystemTime = Utc
            .with_ymd_and_hms(2024, 3, 5, 10, 30, 0)
            .unwrap()
            .into();
        assert_eq!(partition_name(ts, PartitionGranularity::Day), "2024_03_05");
    }

    #[test]
    fn test_partition_name_hour() {
        let ts: SystemTime = Utc
            .with_ymd_and_hms(2024, 3, 5, 9, 30, 0)
            .unwrap()
            .into();
        assert_eq!(
            partition_name(ts, PartitionGranularity::Hour),
            "2024_03_05_09"
        );
    }

    #[test]
    fn test_partition_name_15min() {
        let ts: SystemTime = Utc
            .with_ymd_and_hms(2024, 3, 5, 9, 47, 0)
            .unwrap()
            .into();
        assert_eq!(
            partition_name(ts, PartitionGranularity::FifteenMin),
            "2024_03_05_09_45"
        );
    }

    #[test]
    fn test_partitions_in_range() {
        let start: SystemTime = Utc
            .with_ymd_and_hms(2024, 3, 5, 10, 0, 0)
            .unwrap()
            .into();
        let end: SystemTime = Utc
            .with_ymd_and_hms(2024, 3, 5, 12, 30, 0)
            .unwrap()
            .into();
        let names = partitions_in_range(start, end, PartitionGranularity::Hour);
        assert_eq!(names, vec!["2024_03_05_10", "2024_03_05_11", "2024_03_05_12"]);
    }
}
