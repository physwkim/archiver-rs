use std::path::Path;

use serde::{Deserialize, Serialize};

use archiver_core::registry::SampleMode;
use archiver_core::types::ArchDbType;

/// Per-PV archiving policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PvPolicy {
    /// The PV name pattern (supports glob).
    pub pv: String,
    /// Stable identifier persisted onto the matched PV's typeinfo so
    /// audit / metrics paths know which policy governed the archive
    /// (Java parity b30f1a6). Defaults to the pattern when omitted.
    #[serde(default)]
    pub name: Option<String>,
    /// Sampling mode.
    #[serde(default = "default_sample_mode")]
    pub sample_mode: PolicySampleMode,
    /// Expected DBR type (auto-detected if None).
    pub dbr_type: Option<ArchDbType>,
    /// Sampling period in seconds (only for Scan mode).
    pub sampling_period: Option<f64>,
}

impl PvPolicy {
    /// Return the policy's stable name, falling back to the pattern.
    pub fn policy_name(&self) -> &str {
        self.name.as_deref().unwrap_or(&self.pv)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PolicySampleMode {
    #[default]
    Monitor,
    Scan,
}

fn default_sample_mode() -> PolicySampleMode {
    PolicySampleMode::Monitor
}

impl PvPolicy {
    pub fn to_sample_mode(&self) -> SampleMode {
        match self.sample_mode {
            PolicySampleMode::Monitor => SampleMode::Monitor,
            PolicySampleMode::Scan => SampleMode::Scan {
                period_secs: self.sampling_period.unwrap_or(1.0),
            },
        }
    }
}

/// Collection of PV policies loaded from a TOML file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyConfig {
    #[serde(rename = "policy")]
    pub policies: Vec<PvPolicy>,
}

impl PolicyConfig {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: PolicyConfig = toml::from_str(&content)?;
        Ok(config)
    }

    /// Find the best matching policy for a PV name.
    pub fn find_policy(&self, pv_name: &str) -> Option<&PvPolicy> {
        // Exact match first.
        if let Some(p) = self.policies.iter().find(|p| p.pv == pv_name) {
            return Some(p);
        }
        // Glob match.
        self.policies.iter().find(|p| glob_match(&p.pv, pv_name))
    }
}

/// Iterative glob matching (supports `*` and `?`).
/// Uses a two-pointer backtracking approach — O(n*m) worst-case, no recursion.
fn glob_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    let (mut pi, mut ti) = (0usize, 0usize);
    let (mut star_pi, mut star_ti) = (usize::MAX, 0usize);

    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star_pi = pi;
            star_ti = ti;
            pi += 1;
        } else if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }

    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }

    pi == p.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_glob_match() {
        assert!(glob_match("SIM:*", "SIM:Sine"));
        assert!(glob_match("SIM:*", "SIM:Cosine:Value"));
        assert!(!glob_match("SIM:*", "OTHER:Sine"));
        assert!(glob_match("SIM:?ine", "SIM:Sine"));
        assert!(glob_match("*", "anything"));
        assert!(glob_match("", ""));
        assert!(!glob_match("", "x"));
        assert!(!glob_match("x", ""));
        assert!(glob_match("**", "abc"));
        assert!(glob_match("a*b*c", "aXXbYYc"));
        assert!(!glob_match("a*b*c", "aXXbYY"));
    }

    #[test]
    fn test_glob_no_exponential_backtracking() {
        // Pattern that would cause exponential backtracking in a naive recursive impl.
        let pattern = "*a*a*a*a*b";
        let text = "aaaaaaaaaaaaaaaaaaaaaaaa"; // no 'b' → must fail fast
        assert!(!glob_match(pattern, text));
    }
}
