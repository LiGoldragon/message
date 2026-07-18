//! Pre-migration store preservation.
//!
//! Before a store repair mutates `messenger.sema`, the migration copies the
//! file aside under the store directory's preserve naming convention:
//! `<store>.v<target>-premigration-<utc-stamp>Z` — the same convention the
//! orchestrate store uses, so one retention discipline covers both daemons.
//! The copy's age is readable from its own name, keeping it reap-eligible
//! under the standing retention windows instead of accumulating silently.

use std::fmt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use sema_engine::SchemaVersion;

use crate::Result;

/// A copy of the store file taken before migration mutates it, sitting beside
/// the original. Creation refuses to overwrite an existing file, and any
/// failure aborts the migration: no repair runs against an unpreserved store.
#[derive(Debug)]
pub struct PreMigrationPreserve {
    path: PathBuf,
}

impl PreMigrationPreserve {
    /// Copy the store aside before the repair, naming the copy after the
    /// migration's target schema version and the current UTC second.
    pub fn create(store: &Path, target: SchemaVersion) -> Result<Self> {
        let stamp = UtcStamp::now()?;
        let path = Self::path_for(store, target, &stamp)
            .ok_or_else(|| Self::failure(store, "store path has no file name"))?;
        if path.exists() {
            return Err(Self::failure(
                store,
                format!("preserve path already exists: {}", path.display()),
            ));
        }
        std::fs::copy(store, &path).map_err(|source| Self::failure(store, source.to_string()))?;
        Ok(Self { path })
    }

    pub fn as_path(&self) -> &Path {
        &self.path
    }

    /// The sibling path the preserve is written to:
    /// `<store>.v<target>-premigration-<utc-stamp>Z`.
    fn path_for(store: &Path, target: SchemaVersion, stamp: &UtcStamp) -> Option<PathBuf> {
        let file_name = store.file_name()?.to_str()?;
        Some(store.with_file_name(format!(
            "{file_name}.v{}-premigration-{stamp}",
            target.value()
        )))
    }

    fn failure(store: &Path, message: impl Into<String>) -> crate::Error {
        crate::Error::PreMigrationPreserve {
            store: store.display().to_string(),
            message: message.into(),
        }
    }
}

/// A second-resolution UTC wall-clock stamp rendered `YYYYMMDDTHHMMSSZ`,
/// matching the store directory's preserve names.
struct UtcStamp {
    seconds_since_epoch: u64,
}

impl UtcStamp {
    fn now() -> Result<Self> {
        let seconds_since_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|source| crate::Error::PreMigrationPreserve {
                store: String::new(),
                message: format!("wall clock precedes the epoch: {source}"),
            })?
            .as_secs();
        Ok(Self {
            seconds_since_epoch,
        })
    }

    /// The proleptic-Gregorian calendar date for this stamp's day, via Howard
    /// Hinnant's civil-from-days algorithm.
    fn civil_date(&self) -> (i64, u64, u64) {
        let days = (self.seconds_since_epoch / 86_400) as i64;
        let shifted = days + 719_468;
        let era = shifted.div_euclid(146_097);
        let day_of_era = shifted.rem_euclid(146_097) as u64;
        let year_of_era =
            (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
        let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
        let month_index = (5 * day_of_year + 2) / 153;
        let day = day_of_year - (153 * month_index + 2) / 5 + 1;
        let month = if month_index < 10 {
            month_index + 3
        } else {
            month_index - 9
        };
        let year = era * 400 + year_of_era as i64 + i64::from(month <= 2);
        (year, month, day)
    }
}

impl fmt::Display for UtcStamp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (year, month, day) = self.civil_date();
        let seconds_of_day = self.seconds_since_epoch % 86_400;
        write!(
            formatter,
            "{year:04}{month:02}{day:02}T{:02}{:02}{:02}Z",
            seconds_of_day / 3_600,
            (seconds_of_day % 3_600) / 60,
            seconds_of_day % 60,
        )
    }
}
