use std::path::Path;

use serde::{Deserialize, Serialize};

use archiver_core::registry::SampleMode;
use archiver_core::types::ArchDbType;

/// Per-PV archiving policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PvPolicy {
    /// The PV name pattern (supports glob).
    pub pv: String,
    /// Sampling mode.
    #[serde(default = "default_sample_mode")]
    pub sample_mode: PolicSampleMode,
    /// Expected DBR type (auto-detected if None).
    pub dbr_type: Option<ArchDbType>,
    /// Sampling period in seconds (only for Scan mode).
    pub sampling_period: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PolicSampleMode {
    #[default]
    Monitor,
    Scan,
}

fn default_sample_mode() -> PolicSampleMode {
    PolicSampleMode::Monitor
}

impl PvPolicy {
    pub fn to_sample_mode(&self) -> SampleMode {
        match self.sample_mode {
            PolicSampleMode::Monitor => SampleMode::Monitor,
            PolicSampleMode::Scan => SampleMode::Scan {
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

/// Simple glob matching (supports `*` and `?`).
fn glob_match(pattern: &str, text: &str) -> bool {
    let mut pi = pattern.chars().peekable();
    let mut ti = text.chars().peekable();

    while pi.peek().is_some() || ti.peek().is_some() {
        match pi.peek() {
            Some('*') => {
                pi.next();
                if pi.peek().is_none() {
                    return true;
                }
                while ti.peek().is_some() {
                    let remaining_pattern: String = pi.clone().collect();
                    let remaining_text: String = ti.clone().collect();
                    if glob_match(&remaining_pattern, &remaining_text) {
                        return true;
                    }
                    ti.next();
                }
                return false;
            }
            Some('?') => {
                pi.next();
                if ti.next().is_none() {
                    return false;
                }
            }
            Some(&pc) => {
                if ti.next() != Some(pc) {
                    return false;
                }
                pi.next();
            }
            None => return false,
        }
    }
    true
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
    }
}
