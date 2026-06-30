//! Project aggregate — root entity for a workspace/project

use serde::{Deserialize, Serialize};
use crate::domain::primitives::{EntityId, Timestamp};

/// A project represents a workspace directory that Syncode manages.
/// It is the root aggregate containing threads, configuration, and metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Project {
    pub id: EntityId,
    pub name: String,
    /// Absolute path to the project root directory
    pub root_path: String,
    /// Provider configuration ID for this project
    pub provider_id: Option<String>,
    /// Default model for new threads
    pub default_model: Option<String>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl Project {
    pub fn new(name: impl Into<String>, root_path: impl Into<String>) -> Self {
        let now = Timestamp::now();
        Self {
            id: EntityId::new(),
            name: name.into(),
            root_path: root_path.into(),
            provider_id: None,
            default_model: None,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn set_provider(&mut self, provider_id: impl Into<String>) {
        self.provider_id = Some(provider_id.into());
        self.updated_at = Timestamp::now();
    }

    pub fn set_default_model(&mut self, model: impl Into<String>) {
        self.default_model = Some(model.into());
        self.updated_at = Timestamp::now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn project_new_sets_defaults() {
        let p = Project::new("my-project", "/tmp/project");
        assert_eq!(p.name, "my-project");
        assert_eq!(p.root_path, "/tmp/project");
        assert!(p.provider_id.is_none());
        assert!(p.default_model.is_none());
    }

    #[test]
    fn project_new_generates_unique_id() {
        let a = Project::new("a", "/a");
        let b = Project::new("b", "/b");
        assert_ne!(a.id, b.id);
    }

    #[test]
    fn project_set_provider() {
        let mut p = Project::new("test", "/test");
        p.set_provider("anthropic");
        assert_eq!(p.provider_id.as_deref(), Some("anthropic"));
    }

    #[test]
    fn project_set_default_model() {
        let mut p = Project::new("test", "/test");
        p.set_default_model("claude-3-opus");
        assert_eq!(p.default_model.as_deref(), Some("claude-3-opus"));
    }

    #[test]
    fn project_serialization_roundtrip() {
        let p = Project::new("serde-test", "/tmp/serde");
        let json = serde_json::to_string(&p).expect("serialize");
        let back: Project = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.name, p.name);
        assert_eq!(back.root_path, p.root_path);
    }
}
