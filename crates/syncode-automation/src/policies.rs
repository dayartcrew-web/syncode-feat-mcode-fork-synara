//! Retry, misfire, and completion policies
//!
//! Defines how the automation system handles failures, missed schedules,
//! and completion conditions.

use serde::{Deserialize, Serialize};

/// What to do when a scheduled run fails
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RetryPolicy {
    /// Do not retry
    None,
    /// Retry up to N times with exponential backoff
    ExponentialBackoff {
        max_retries: u32,
        base_delay_secs: u64,
    },
    /// Retry up to N times with fixed delay
    FixedDelay { max_retries: u32, delay_secs: u64 },
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::ExponentialBackoff {
            max_retries: 3,
            base_delay_secs: 5,
        }
    }
}

impl RetryPolicy {
    /// Get the delay for a given retry attempt (0-indexed)
    pub fn delay_for_attempt(&self, attempt: u32) -> Option<std::time::Duration> {
        match self {
            RetryPolicy::None => None,
            RetryPolicy::ExponentialBackoff {
                max_retries,
                base_delay_secs,
            } => {
                if attempt >= *max_retries {
                    None
                } else {
                    let secs = base_delay_secs * 2u64.pow(attempt);
                    Some(std::time::Duration::from_secs(secs))
                }
            }
            RetryPolicy::FixedDelay {
                max_retries,
                delay_secs,
            } => {
                if attempt >= *max_retries {
                    None
                } else {
                    Some(std::time::Duration::from_secs(*delay_secs))
                }
            }
        }
    }

    /// Whether retries are exhausted
    pub fn exhausted(&self, attempt: u32) -> bool {
        self.delay_for_attempt(attempt).is_none()
    }
}

/// What to do when a scheduled trigger is missed (e.g., system was down)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "snake_case")]
pub enum MisfirePolicy {
    /// Skip the missed run entirely
    #[default]
    Skip,
    /// Run immediately when detected
    RunImmediately,
    /// Run the next scheduled time
    RunNext,
}

/// How to determine if a run completed successfully
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "snake_case")]
pub enum CompletionPolicy {
    /// Consider successful if exit code is 0
    #[default]
    ExitCodeZero,
    /// Consider successful if exit code is in the allowed list
    AllowedExitCodes(Vec<i32>),
    /// Always consider successful (fire and forget)
    AlwaysSuccess,
    /// Use AI evaluation to determine success.
    ///
    /// `stop_when` is a natural-language description of the condition under
    /// which the automation should stop (e.g. "all tests pass"). The
    /// automation is considered complete once the evaluator's confidence that
    /// `stop_when` is satisfied meets or exceeds `confidence_threshold`
    /// (a value in the range `0.0..=1.0`).
    AiEvaluated {
        stop_when: String,
        confidence_threshold: f64,
    },
}

impl CompletionPolicy {
    /// Check if an exit code indicates success
    pub fn is_success(&self, exit_code: i32) -> bool {
        match self {
            CompletionPolicy::ExitCodeZero => exit_code == 0,
            CompletionPolicy::AllowedExitCodes(codes) => codes.contains(&exit_code),
            CompletionPolicy::AlwaysSuccess => true,
            CompletionPolicy::AiEvaluated { .. } => {
                // AI evaluation requires separate processing
                exit_code == 0
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_policy_exponential_backoff() {
        let policy = RetryPolicy::ExponentialBackoff {
            max_retries: 3,
            base_delay_secs: 2,
        };
        assert_eq!(
            policy.delay_for_attempt(0),
            Some(std::time::Duration::from_secs(2))
        );
        assert_eq!(
            policy.delay_for_attempt(1),
            Some(std::time::Duration::from_secs(4))
        );
        assert_eq!(
            policy.delay_for_attempt(2),
            Some(std::time::Duration::from_secs(8))
        );
        assert_eq!(policy.delay_for_attempt(3), None); // exhausted
    }

    #[test]
    fn retry_policy_fixed_delay() {
        let policy = RetryPolicy::FixedDelay {
            max_retries: 2,
            delay_secs: 10,
        };
        assert_eq!(
            policy.delay_for_attempt(0),
            Some(std::time::Duration::from_secs(10))
        );
        assert_eq!(
            policy.delay_for_attempt(1),
            Some(std::time::Duration::from_secs(10))
        );
        assert_eq!(policy.delay_for_attempt(2), None);
    }

    #[test]
    fn retry_policy_none() {
        let policy = RetryPolicy::None;
        assert_eq!(policy.delay_for_attempt(0), None);
    }

    #[test]
    fn retry_policy_exhausted() {
        let policy = RetryPolicy::ExponentialBackoff {
            max_retries: 1,
            base_delay_secs: 1,
        };
        assert!(!policy.exhausted(0));
        assert!(policy.exhausted(1));
    }

    #[test]
    fn completion_policy_exit_code() {
        let policy = CompletionPolicy::ExitCodeZero;
        assert!(policy.is_success(0));
        assert!(!policy.is_success(1));
    }

    #[test]
    fn completion_policy_allowed_codes() {
        let policy = CompletionPolicy::AllowedExitCodes(vec![0, 1, 2]);
        assert!(policy.is_success(0));
        assert!(policy.is_success(1));
        assert!(policy.is_success(2));
        assert!(!policy.is_success(3));
    }

    #[test]
    fn completion_policy_always_success() {
        let policy = CompletionPolicy::AlwaysSuccess;
        assert!(policy.is_success(0));
        assert!(policy.is_success(1));
        assert!(policy.is_success(255));
    }

    #[test]
    fn misfire_policy_serialization() {
        let policies = vec![
            MisfirePolicy::Skip,
            MisfirePolicy::RunImmediately,
            MisfirePolicy::RunNext,
        ];
        for policy in policies {
            let json = serde_json::to_string(&policy).unwrap();
            let back: MisfirePolicy = serde_json::from_str(&json).unwrap();
            assert_eq!(policy, back);
        }
    }

    #[test]
    fn retry_policy_serialization() {
        let policy = RetryPolicy::ExponentialBackoff {
            max_retries: 5,
            base_delay_secs: 10,
        };
        let json = serde_json::to_string(&policy).unwrap();
        assert!(json.contains("exponential_backoff"));
        let back: RetryPolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(policy, back);
    }

    #[test]
    fn completion_policy_ai_evaluated_roundtrip() {
        let policy = CompletionPolicy::AiEvaluated {
            stop_when: "all tests pass".to_string(),
            confidence_threshold: 0.9,
        };
        let json = serde_json::to_string(&policy).unwrap();
        assert!(json.contains("ai_evaluated"));
        assert!(json.contains("stop_when"));
        assert!(json.contains("confidence_threshold"));
        let back: CompletionPolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(policy, back);
    }

    #[test]
    fn completion_policy_ai_evaluated_is_success() {
        let policy = CompletionPolicy::AiEvaluated {
            stop_when: "all tests pass".to_string(),
            confidence_threshold: 0.95,
        };
        // AI evaluation requires separate processing; until then a zero exit
        // code is treated as success.
        assert!(policy.is_success(0));
        assert!(!policy.is_success(1));
    }
}
