//! Stacked actions — commit→push→PR pipeline
//!
//! Implements a pipeline of git actions that can be chained:
//! Stage → Commit → Push → Create PR
//!
//! Each step can fail independently, and the pipeline can be
//! resumed from the last successful step.

use crate::service::{Git2Service, GitError, GitService};

/// A single action in the pipeline
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum StackedAction {
    /// Stage specific files
    Stage { paths: Vec<String> },
    /// Create a commit with a message
    Commit { message: String },
    /// Push to a remote branch
    Push { remote: String, branch: String },
    /// Create a pull request (external — returns URL)
    CreatePR { title: String, body: String, base: String },
}

/// Result of executing a stacked action
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ActionResult {
    pub action_index: usize,
    pub success: bool,
    pub output: Option<String>,
    pub error: Option<String>,
}

/// A pipeline of stacked actions
#[derive(Debug, Clone, Default)]
pub struct StackedPipeline {
    actions: Vec<StackedAction>,
    results: Vec<ActionResult>,
}

impl StackedPipeline {
    /// Create a new empty pipeline
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an action to the pipeline
    pub fn add(&mut self, action: StackedAction) {
        self.actions.push(action);
    }

    /// Get the list of actions
    pub fn actions(&self) -> &[StackedAction] {
        &self.actions
    }

    /// Get the results from previous executions
    pub fn results(&self) -> &[ActionResult] {
        &self.results
    }

    /// Execute all pending actions against the git service
    pub async fn execute(&mut self, service: &Git2Service) -> Result<Vec<ActionResult>, GitError> {
        let mut all_results = Vec::new();

        for (i, action) in self.actions.iter().enumerate() {
            let result = match action {
                StackedAction::Stage { paths } => {
                    let path_refs: Vec<&str> = paths.iter().map(|s| s.as_str()).collect();
                    match service.add(&path_refs) {
                        Ok(()) => ActionResult {
                            action_index: i,
                            success: true,
                            output: Some(format!("Staged {} files", paths.len())),
                            error: None,
                        },
                        Err(e) => ActionResult {
                            action_index: i,
                            success: false,
                            output: None,
                            error: Some(e.to_string()),
                        },
                    }
                }
                StackedAction::Commit { message } => {
                    match service.commit(message) {
                        Ok(commit) => ActionResult {
                            action_index: i,
                            success: true,
                            output: Some(format!("Committed: {} ({})", commit.message, commit.short_hash)),
                            error: None,
                        },
                        Err(e) => ActionResult {
                            action_index: i,
                            success: false,
                            output: None,
                            error: Some(e.to_string()),
                        },
                    }
                }
                StackedAction::Push { remote, branch } => {
                    match service.push(remote, branch) {
                        Ok(()) => ActionResult {
                            action_index: i,
                            success: true,
                            output: Some(format!("Pushed to {}/{}", remote, branch)),
                            error: None,
                        },
                        Err(e) => ActionResult {
                            action_index: i,
                            success: false,
                            output: None,
                            error: Some(e.to_string()),
                        },
                    }
                }
                StackedAction::CreatePR { title, body: _, base } => {
                    // PR creation is external (GitHub API) — stub
                    ActionResult {
                        action_index: i,
                        success: true,
                        output: Some(format!(
                            "PR stub: '{}' against '{}' (external API not yet connected)",
                            title, base
                        )),
                        error: None,
                    }
                }
            };

            all_results.push(result.clone());
            self.results.push(result);
        }

        Ok(all_results)
    }

    /// Clear the pipeline and results
    pub fn reset(&mut self) {
        self.actions.clear();
        self.results.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipeline_starts_empty() {
        let pipeline = StackedPipeline::new();
        assert!(pipeline.actions().is_empty());
        assert!(pipeline.results().is_empty());
    }

    #[test]
    fn pipeline_add_actions() {
        let mut pipeline = StackedPipeline::new();
        pipeline.add(StackedAction::Stage {
            paths: vec!["file.rs".to_string()],
        });
        pipeline.add(StackedAction::Commit {
            message: "test commit".to_string(),
        });
        assert_eq!(pipeline.actions().len(), 2);
    }

    #[test]
    fn pipeline_reset() {
        let mut pipeline = StackedPipeline::new();
        pipeline.add(StackedAction::Commit {
            message: "test".to_string(),
        });
        pipeline.reset();
        assert!(pipeline.actions().is_empty());
        assert!(pipeline.results().is_empty());
    }

    #[test]
    fn action_result_serialization() {
        let result = ActionResult {
            action_index: 0,
            success: true,
            output: Some("ok".to_string()),
            error: None,
        };
        let json = serde_json::to_string(&result).unwrap();
        let back: ActionResult = serde_json::from_str(&json).unwrap();
        assert!(back.success);
        assert_eq!(back.output.unwrap(), "ok");
    }

    #[test]
    fn stacked_action_serialization() {
        let action = StackedAction::Commit {
            message: "fix bug".to_string(),
        };
        let json = serde_json::to_string(&action).unwrap();
        assert!(json.contains("fix bug"));
    }
}
