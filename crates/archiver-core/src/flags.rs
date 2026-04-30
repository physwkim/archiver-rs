//! Process-global named flags (Java's `namedFlagsSet/Get/All` BPL).
//!
//! Operators flip these at runtime via the management API to gate
//! features like "skip LTS reads while NFS is wedged" without restarting
//! the appliance. Currently consumed by:
//! - `storage::tiered::TieredStorage` reads — `SKIP_<TIER>_FOR_RETRIEVAL`
//! - `etl::executor::EtlExecutor` writes — `SKIP_<DEST>_FOR_ETL`
//!
//! Java wires these through `ConfigService.getNamedFlag(...)`. We use a
//! global so both the HTTP handler (in archiver-api) and the storage /
//! ETL paths (in archiver-core) can share state without an extra trait.

use std::sync::OnceLock;

use dashmap::DashMap;

fn store() -> &'static DashMap<String, bool> {
    static STORE: OnceLock<DashMap<String, bool>> = OnceLock::new();
    STORE.get_or_init(DashMap::new)
}

/// Set a named flag. Returns the previous value, or `false` if unset.
pub fn set(name: &str, value: bool) -> bool {
    store().insert(name.to_string(), value).unwrap_or(false)
}

/// Get a named flag. Returns `false` if the flag is unset.
pub fn get(name: &str) -> bool {
    store().get(name).map(|e| *e.value()).unwrap_or(false)
}

/// Snapshot of all flags as `(name, value)` pairs, sorted by name.
pub fn all() -> Vec<(String, bool)> {
    let mut pairs: Vec<(String, bool)> = store()
        .iter()
        .map(|e| (e.key().clone(), *e.value()))
        .collect();
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    pairs
}

/// Convenience: is `SKIP_<tier>_FOR_RETRIEVAL` set? `tier` is uppercased.
pub fn skip_tier_for_retrieval(tier: &str) -> bool {
    get(&format!("SKIP_{}_FOR_RETRIEVAL", tier.to_uppercase()))
}

/// Convenience: is `SKIP_<tier>_FOR_ETL` set?
pub fn skip_tier_for_etl(tier: &str) -> bool {
    get(&format!("SKIP_{}_FOR_ETL", tier.to_uppercase()))
}
