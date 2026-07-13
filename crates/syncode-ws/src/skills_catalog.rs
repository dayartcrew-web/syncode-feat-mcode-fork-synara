//! Agent Skill discovery — multi-origin catalog engine.
//!
//! Port of MCode's `apps/server/src/provider/skillsCatalog.ts`. Aggregates skill
//! folders across 10 origins (mcode/codex/claude/cursor/gemini/grok/kilo/opencode/
//! pi/agents), each resolved at a home root (`~/.<origin>/skills`, with the
//! portable `mcode` origin remapped to syncode's `~/.synara/skills`) and, when a
//! project `cwd` is supplied, at project roots (`<ancestor>/<rootName>/skills`
//! walked up from the cwd). Skills are `<dir>/SKILL.md` (nested one namespace
//! deep); the `pi` origin additionally accepts flat `*.md` files.
//!
//! Discovery dedupes by lowercased name in root order (earlier roots win), with
//! per-provider origin preferences controlling which provider's native copy
//! takes precedence. A short-lived process-global cache (TTL 15s) absorbs burst
//! refetches from the composer/settings panel. Native provider `list_skills`
//! merging is intentionally absent — the syncode `ProviderAdapter` trait exposes
//! no such method, so discovery is purely filesystem-backed.
//!
//! Layer: server provider-discovery helper (read-only; no CQRS surface).

use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{OnceLock, RwLock};
use std::time::{Duration, Instant};

// ── Types ────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
enum FrontmatterValue {
    Str(String),
    Bool(bool),
}

/// A filesystem root to scan, tagged with the origin it represents.
#[derive(Debug, Clone)]
struct SkillRoot {
    path: PathBuf,
    scope: String,
    /// The `pi` origin also accepts flat `*.md` skill files at depth 0.
    include_markdown_files: bool,
}

/// `interface` sub-shape of a `ProviderSkillDescriptor`.
#[derive(Debug, Clone, Serialize)]
pub struct SkillInterface {
    #[serde(rename = "displayName", skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(rename = "shortDescription", skip_serializing_if = "Option::is_none")]
    pub short_description: Option<String>,
}

/// A discovered skill — mirrors MCode's `ProviderSkillDescriptor` wire shape
/// (`{ name, description?, path, enabled, scope, interface? }`).
#[derive(Debug, Clone, Serialize)]
pub struct SkillDescriptor {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub path: String,
    pub enabled: bool,
    pub scope: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interface: Option<SkillInterface>,
}

/// Inputs to a catalog discovery scan.
pub struct DiscoveryInput<'a> {
    /// Optional workspace cwd; when present, project-level skill folders are
    /// walked up from here.
    pub cwd: Option<&'a str>,
    /// Resolved home directory (from `server_home_dir`); when `None`, only
    /// project roots are scanned (home roots are skipped).
    pub home_dir: Option<String>,
    /// Provider whose native copies should win name conflicts (controls origin
    /// preference ordering). `None` → default ordering.
    pub provider: Option<&'a str>,
    /// Settings panel needs every origin (duplicates shown as sources); the
    /// composer picker keeps one winner per name.
    pub include_duplicate_origins: bool,
    /// Bypass the short-lived discovery cache.
    pub force_reload: bool,
}

// ── Frontmatter parsing (port of parseSkillFrontmatter) ──────────────

fn strip_yaml_quotes(value: &str) -> String {
    let trimmed = value.trim();
    let len = trimmed.len();
    if len >= 2
        && ((trimmed.starts_with('"') && trimmed.ends_with('"'))
            || (trimmed.starts_with('\'') && trimmed.ends_with('\'')))
    {
        trimmed[1..len - 1].trim().to_string()
    } else {
        trimmed.to_string()
    }
}

fn parse_yaml_scalar(value: &str) -> FrontmatterValue {
    let unquoted = strip_yaml_quotes(value);
    match unquoted.to_lowercase().as_str() {
        "true" => FrontmatterValue::Bool(true),
        "false" => FrontmatterValue::Bool(false),
        _ => FrontmatterValue::Str(unquoted),
    }
}

/// Parse the small scalar frontmatter subset used by Agent Skills
/// (`---\nkey: value\n...\n---`) without pulling in a YAML dependency.
fn parse_skill_frontmatter(markdown: &str) -> HashMap<String, FrontmatterValue> {
    let normalized = markdown.replace("\r\n", "\n");
    let after_open = match normalized.strip_prefix("---") {
        Some(rest) => rest,
        None => return HashMap::new(),
    };
    // Optional horizontal whitespace, then a newline.
    let after_open = after_open.trim_start_matches([' ', '\t']);
    let after_open = match after_open.strip_prefix('\n') {
        Some(rest) => rest,
        None => return HashMap::new(),
    };
    // Closing fence: `\n---`.
    let end = match after_open.find("\n---") {
        Some(i) => i,
        None => return HashMap::new(),
    };
    let frontmatter = &after_open[..end];

    let mut record = HashMap::new();
    for line in frontmatter.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let separator = match trimmed.find(':') {
            Some(i) if i > 0 => i,
            _ => continue,
        };
        let key = trimmed[..separator].trim().to_string();
        let value = trimmed[separator + 1..].trim().to_string();
        if key.is_empty() || value.is_empty() {
            continue;
        }
        record.insert(key, parse_yaml_scalar(&value));
    }
    record
}

fn read_string_field(fm: &HashMap<String, FrontmatterValue>, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(FrontmatterValue::Str(s)) = fm.get(*key) {
            let trimmed = s.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

fn read_bool_field(fm: &HashMap<String, FrontmatterValue>, keys: &[&str]) -> Option<bool> {
    for key in keys {
        if let Some(FrontmatterValue::Bool(b)) = fm.get(*key) {
            return Some(*b);
        }
    }
    None
}

// ── Filesystem walking (port of collectSkillMarkdownPaths) ───────────

/// Walk `cwd` → root, deepest first (matches MCode `ancestorsFromDeepest`).
/// Does not require the path to exist; a non-canonicalizable path is used as-is.
fn ancestors_from_deepest(cwd: &Path) -> Vec<PathBuf> {
    let resolved = std::fs::canonicalize(cwd).unwrap_or_else(|_| cwd.to_path_buf());
    resolved.ancestors().map(Path::to_path_buf).collect()
}

/// True if `path` is a directory, following symlinks (mirrors MCode's
/// `isWalkableSkillDirectory` which `fs.stat`s symlink entries).
fn is_walkable_dir(path: &Path) -> bool {
    std::fs::metadata(path).map(|m| m.is_dir()).unwrap_or(false)
}

/// True if `path` is a readable markdown file (following symlinks), excluding
/// lowercase `skill.md` (only `SKILL.md` marks a direct-file skill).
fn is_readable_markdown_file(path: &Path) -> bool {
    let name = match path.file_name().and_then(|n| n.to_str()) {
        Some(n) => n,
        None => return false,
    };
    let lower = name.to_lowercase();
    if !lower.ends_with(".md") || lower == "skill.md" {
        return false;
    }
    std::fs::metadata(path)
        .map(|m| m.is_file())
        .unwrap_or(false)
}

/// Collect `SKILL.md` paths under `root`, nested up to depth 2 (one namespace
/// level). At depth 0, when `include_markdown_files` is set, flat `*.md` files
/// are also collected (the `pi` origin). Subdirectories are visited in sorted
/// name order so name-dedup is deterministic across runs.
fn collect_skill_markdown_paths(root: &Path, include_markdown_files: bool) -> Vec<PathBuf> {
    fn visit(dir: &Path, depth: u8, include_markdown_files: bool) -> Vec<PathBuf> {
        let mut out = Vec::new();

        // A `SKILL.md` directly in this directory marks the whole dir as a skill.
        let skill_path = dir.join("SKILL.md");
        if std::fs::metadata(&skill_path)
            .map(|m| m.is_file())
            .unwrap_or(false)
        {
            out.push(skill_path);
            return out;
        }

        if depth >= 2 {
            return out;
        }

        let dirents = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(_) => return out,
        };
        let entries: Vec<PathBuf> = dirents.flatten().map(|e| e.path()).collect();

        // depth 0 + include_markdown_files: direct markdown files (sorted).
        if depth == 0 && include_markdown_files {
            let mut md_files: Vec<PathBuf> = entries
                .iter()
                .filter(|p| is_readable_markdown_file(p))
                .cloned()
                .collect();
            md_files.sort();
            out.extend(md_files);
        }

        // Subdirectories (sorted by name) — recurse.
        let mut subdirs: Vec<PathBuf> = entries
            .iter()
            .filter(|p| is_walkable_dir(p))
            .cloned()
            .collect();
        subdirs.sort();
        for subdir in subdirs {
            out.extend(visit(&subdir, depth + 1, include_markdown_files));
        }

        out
    }

    visit(root, 0, include_markdown_files)
}

/// Read one `SKILL.md` and build a `SkillDescriptor` tagged with `scope`.
fn read_skill_descriptor(skill_path: &Path, scope: &str) -> Option<SkillDescriptor> {
    let raw = std::fs::read_to_string(skill_path).ok()?;
    let fm = parse_skill_frontmatter(&raw);

    let filename = skill_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("");
    let fallback_name = if filename.to_lowercase() == "skill.md" {
        skill_path
            .parent()
            .and_then(|p| p.file_name())
            .and_then(|n| n.to_str())
            .filter(|s| !s.is_empty())?
            .to_string()
    } else {
        skill_path
            .file_stem()
            .and_then(|n| n.to_str())
            .filter(|s| !s.is_empty())?
            .to_string()
    };

    let name = read_string_field(&fm, &["name"]).unwrap_or(fallback_name);
    let description = read_string_field(&fm, &["description"]);
    let display_name = read_string_field(&fm, &["display-name", "displayName", "title"]);
    let short_description =
        read_string_field(&fm, &["short-description", "shortDescription", "summary"]);
    let disabled =
        read_bool_field(&fm, &["disable-model-invocation", "disableModelInvocation"]) == Some(true);

    let abs_path = std::fs::canonicalize(skill_path)
        .unwrap_or_else(|_| skill_path.to_path_buf())
        .to_string_lossy()
        .into_owned();

    let interface = if display_name.is_some() || short_description.is_some() {
        Some(SkillInterface {
            display_name,
            short_description,
        })
    } else {
        None
    };

    Some(SkillDescriptor {
        name,
        description,
        path: abs_path,
        enabled: !disabled,
        scope: scope.to_string(),
        interface,
    })
}

fn skill_name_key(name: &str) -> String {
    name.trim().to_lowercase()
}

// ── Origin roots (port of SKILL_ORIGIN_ROOTS) ─────────────────────────

/// Origin scan order: native provider homes first (mcode = portable fallback).
const HOME_ORIGIN_ORDER: &[&str] = &[
    "mcode", "codex", "claude", "cursor", "gemini", "grok", "kilo", "opencode", "pi", "agents",
];

/// Home roots for an origin. The `mcode` origin is remapped to syncode's
/// `~/.synara/skills` portable folder; all others follow MCode's paths.
fn home_roots_for_origin(origin: &str, home_dir: &str) -> Vec<PathBuf> {
    let home = Path::new(home_dir);
    match origin {
        "mcode" => vec![home.join(".synara").join("skills")],
        "codex" => vec![home.join(".codex").join("skills")],
        "claude" => vec![home.join(".claude").join("skills")],
        "cursor" => vec![
            home.join(".cursor").join("skills-cursor"),
            home.join(".cursor").join("skills"),
        ],
        "gemini" => vec![home.join(".gemini").join("skills")],
        "grok" => vec![home.join(".grok").join("skills")],
        "kilo" => vec![home.join(".kilo").join("skills")],
        "opencode" => vec![home.join(".config").join("opencode").join("skills")],
        "pi" => vec![home.join(".pi").join("agent").join("skills")],
        "agents" => vec![home.join(".agents").join("skills")],
        _ => vec![],
    }
}

/// Per-provider agent-definition folders (the `agents/` sibling of `skills/`).
/// Each holds one file per subagent — `.md` with YAML frontmatter
/// (`name` + `description`) for Claude/Codex, occasionally `.toml` config
/// (skipped at read time). Returns the same provider → home-path mapping as
/// [`home_roots_for_origin`] but pointing at the `agents` subfolder so the
/// catalog surfaces subagents on the Agents settings page.
fn agent_roots_for_origin(origin: &str, home_dir: &str) -> Vec<PathBuf> {
    let home = Path::new(home_dir);
    match origin {
        "codex" => vec![home.join(".codex").join("agents")],
        "claude" => vec![home.join(".claude").join("agents")],
        "cursor" => vec![home.join(".cursor").join("agents")],
        "gemini" => vec![home.join(".gemini").join("agents")],
        "grok" => vec![home.join(".grok").join("agents")],
        "kilo" => vec![home.join(".kilo").join("agents")],
        "opencode" => vec![home.join(".config").join("opencode").join("agents")],
        "pi" => vec![home.join(".pi").join("agent").join("agents")],
        _ => vec![],
    }
}

/// Per-origin project root directory names (e.g. `.claude`, `.mcode`).
fn project_root_names_for_origin(origin: &str) -> &'static [&'static str] {
    match origin {
        "mcode" => &[".mcode"],
        "codex" => &[".codex"],
        "claude" => &[".claude"],
        "cursor" => &[".cursor"],
        "gemini" => &[".gemini"],
        "grok" => &[".grok"],
        "kilo" => &[".kilo"],
        "opencode" => &[".opencode"],
        "pi" => &[".pi"],
        "agents" => &[".agents"],
        _ => &[],
    }
}

/// Per-provider preferred origins (port of PROVIDER_SKILL_ORIGIN_PREFERENCES).
/// `claude`/`claudeAgent` both map to `["claude"]` (syncode uses `claude`).
fn provider_preferences(provider: &str) -> &'static [&'static str] {
    match provider {
        "codex" => &["codex", "agents"],
        "claude" | "claudeAgent" => &["claude"],
        "cursor" => &["cursor", "agents", "claude", "codex"],
        "gemini" => &["agents", "gemini"],
        "grok" => &["grok", "claude", "agents"],
        "kilo" => &["kilo", "agents", "claude"],
        "opencode" => &["opencode", "claude", "agents"],
        "pi" => &["pi", "agents"],
        _ => &[],
    }
}

/// Native copies first, then the portable `mcode` fallback, then remaining
/// provider homes for cross-provider reuse (port of orderedOriginsForProvider).
fn ordered_origins_for_provider(
    provider: Option<&str>,
    include_mcode: bool,
    include_remaining: bool,
) -> Vec<String> {
    let mut ordered: Vec<String> = Vec::new();
    if let Some(p) = provider {
        for origin in provider_preferences(p) {
            if !ordered.iter().any(|o| o == origin) {
                ordered.push((*origin).to_string());
            }
        }
    }
    if include_mcode && !ordered.iter().any(|o| o == "mcode") {
        ordered.push("mcode".to_string());
    }
    if !include_remaining {
        return ordered
            .into_iter()
            .filter(|o| include_mcode || o != "mcode")
            .collect();
    }
    for origin in HOME_ORIGIN_ORDER {
        if !include_mcode && *origin == "mcode" {
            continue;
        }
        if !ordered.iter().any(|o| o == origin) {
            ordered.push((*origin).to_string());
        }
    }
    ordered
}

/// Build the ordered root list: project roots (walked up from cwd, deduped
/// against home roots) followed by home roots (port of rootsForOrderedOrigins).
fn roots_for_ordered_origins(input: &DiscoveryInput, origins: &[String]) -> Vec<SkillRoot> {
    let mut home_roots: Vec<SkillRoot> = Vec::new();
    if let Some(home_dir) = &input.home_dir {
        for origin in origins {
            for path in home_roots_for_origin(origin, home_dir) {
                home_roots.push(SkillRoot {
                    path,
                    scope: origin.clone(),
                    include_markdown_files: origin == "pi",
                });
            }
        }
        // Agent subagent definitions: each provider may ship an `agents/`
        // subfolder next to its `skills/` folder (e.g. `~/.claude/agents/`,
        // `~/.codex/agents/`) containing per-agent markdown/toml files with
        // YAML frontmatter (name + description). These surface on the Agents
        // settings page alongside the shared `.agents/skills` entries. We scan
        // every provider's agents folder regardless of the `origins` filter so
        // the Agents page is complete even when a single provider's skills are
        // requested. Scope is `agents-<provider>` so the UI can group by
        // provider. `.md` files are read directly (include_markdown_files);
        // `.toml` config files are skipped (they carry no name/description).
        for provider in HOME_ORIGIN_ORDER {
            if *provider == "mcode" || *provider == "agents" {
                continue;
            }
            for path in agent_roots_for_origin(provider, home_dir) {
                home_roots.push(SkillRoot {
                    path,
                    scope: format!("agents-{provider}"),
                    include_markdown_files: true,
                });
            }
        }
    }
    let home_root_paths: HashSet<PathBuf> = home_roots
        .iter()
        .map(|r| std::fs::canonicalize(&r.path).unwrap_or_else(|_| r.path.clone()))
        .collect();

    let mut project_roots: Vec<SkillRoot> = Vec::new();
    if let Some(cwd) = input.cwd.map(str::trim).filter(|s| !s.is_empty()) {
        for ancestor in ancestors_from_deepest(Path::new(cwd)) {
            let mut seen: HashSet<&str> = HashSet::new();
            for origin in origins {
                for root_name in project_root_names_for_origin(origin) {
                    if !seen.insert(root_name) {
                        continue;
                    }
                    let root_path = ancestor.join(root_name).join("skills");
                    // Skip project roots that resolve to a home skill folder so
                    // each folder is scanned once and keeps its true origin.
                    let canon =
                        std::fs::canonicalize(&root_path).unwrap_or_else(|_| root_path.clone());
                    if home_root_paths.contains(&canon) {
                        continue;
                    }
                    project_roots.push(SkillRoot {
                        path: root_path,
                        scope: "project".to_string(),
                        include_markdown_files: origin == "pi",
                    });
                }
            }
        }
    }

    project_roots.extend(home_roots);
    project_roots
}

// ── Catalog aggregation (port of collectSkillsFromRoots / discover) ───

/// Scan every root and dedupe by lowercased name in root order (earlier roots
/// keep precedence). Within a root, `SKILL.md` path order is preserved.
fn collect_skills_from_roots(roots: &[SkillRoot]) -> Vec<SkillDescriptor> {
    let mut all: Vec<SkillDescriptor> = Vec::new();
    for root in roots {
        let paths = collect_skill_markdown_paths(&root.path, root.include_markdown_files);
        for path in paths {
            if let Some(descriptor) = read_skill_descriptor(&path, &root.scope) {
                all.push(descriptor);
            }
        }
    }

    let mut by_name: HashMap<String, ()> = HashMap::new();
    let mut out: Vec<SkillDescriptor> = Vec::new();
    for skill in all {
        let key = skill_name_key(&skill.name);
        if by_name.contains_key(&key) {
            continue;
        }
        by_name.insert(key, ());
        out.push(skill);
    }
    out
}

/// Scan every root WITHOUT deduping (settings panel shows duplicate origins as
/// sources within one skill row).
fn collect_skill_descriptors_from_roots(roots: &[SkillRoot]) -> Vec<SkillDescriptor> {
    let mut out: Vec<SkillDescriptor> = Vec::new();
    for root in roots {
        let paths = collect_skill_markdown_paths(&root.path, root.include_markdown_files);
        for path in paths {
            if let Some(descriptor) = read_skill_descriptor(&path, &root.scope) {
                out.push(descriptor);
            }
        }
    }
    out
}

// ── Cache (TTL 15s, process-global) ───────────────────────────────────

const SKILLS_CATALOG_CACHE_TTL: Duration = Duration::from_secs(15);
const SKILLS_CATALOG_CACHE_MAX_ENTRIES: usize = 64;

type SkillsCatalogCacheMap = HashMap<String, (Instant, Vec<SkillDescriptor>)>;

static SKILLS_CATALOG_CACHE: OnceLock<RwLock<SkillsCatalogCacheMap>> = OnceLock::new();

fn cache() -> &'static RwLock<SkillsCatalogCacheMap> {
    SKILLS_CATALOG_CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Drop-in test hook (mirrors MCode `clearSkillsCatalogCacheForTests`). Clears
/// the cache so unit tests start from a known empty state.
#[cfg(test)]
pub(crate) fn clear_skills_catalog_cache_for_tests() {
    if let Ok(mut map) = cache().write() {
        map.clear();
    }
}

fn cache_key(input: &DiscoveryInput) -> String {
    format!(
        "{}\u{0}{}\u{0}{}\u{0}{}",
        input.cwd.unwrap_or("").trim(),
        input.provider.unwrap_or(""),
        input.home_dir.as_deref().unwrap_or(""),
        input.include_duplicate_origins,
    )
}

/// Discover the skills catalog, consulting the TTL cache first. When
/// `include_duplicate_origins` is set, duplicate skills across origins are kept
/// (for the settings panel); otherwise the first occurrence per name wins
/// (for the composer picker).
pub fn discover_skills_catalog(input: DiscoveryInput) -> Vec<SkillDescriptor> {
    let key = cache_key(&input);

    if !input.force_reload
        && let Ok(map) = cache().read()
        && let Some((at, skills)) = map.get(&key)
        && at.elapsed() <= SKILLS_CATALOG_CACHE_TTL
    {
        return skills.clone();
    }

    let origins = ordered_origins_for_provider(input.provider, true, true);
    let roots = roots_for_ordered_origins(&input, &origins);
    let skills = if input.include_duplicate_origins {
        collect_skill_descriptors_from_roots(&roots)
    } else {
        collect_skills_from_roots(&roots)
    };

    if let Ok(mut map) = cache().write() {
        map.insert(key.clone(), (Instant::now(), skills.clone()));
        // Evict the stalest entry past capacity (largest elapsed = oldest scan).
        while map.len() > SKILLS_CATALOG_CACHE_MAX_ENTRIES {
            let oldest_key = map
                .iter()
                .max_by_key(|(_, (at, _))| at.elapsed())
                .map(|(k, _)| k.clone());
            match oldest_key {
                Some(k) => {
                    map.remove(&k);
                }
                None => break,
            }
        }
    }

    skills
}

/// Remove skills whose lowercased name appears in `disabled_names` (mirrors
/// MCode `filterDisabledSkills`, keyed by `skillNameKey`).
pub fn filter_disabled_skills(
    skills: &[SkillDescriptor],
    disabled_names: &[String],
) -> Vec<SkillDescriptor> {
    if disabled_names.is_empty() {
        return skills.to_vec();
    }
    let disabled: HashSet<String> = disabled_names.iter().map(|n| skill_name_key(n)).collect();
    skills
        .iter()
        .filter(|s| !disabled.contains(&skill_name_key(&s.name)))
        .cloned()
        .collect()
}

// ── Syncode portable folder (replaces MCode mcodeBaseDir) ─────────────

/// `~/.synara/skills` — the syncode portable skills folder (the `mcode` origin
/// home root). `None` when the home directory cannot be resolved.
pub fn synara_skills_dir() -> Option<PathBuf> {
    let home = crate::settings::server_home_dir()?;
    Some(Path::new(&home).join(".synara").join("skills"))
}

/// Ensure `~/.synara/skills` exists (recursive `create_dir_all`), returning the
/// path on success. Discovery still works without the folder — reads simply
/// return nothing — but creating it gives users a drop-in target and lets the
/// settings panel surface a real `mcodeSkillsDir`.
pub fn ensure_synara_skills_dir() -> Option<PathBuf> {
    let dir = synara_skills_dir()?;
    let _ = std::fs::create_dir_all(&dir);
    Some(dir)
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_dir(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "syncode-ws-skills-{}-{}-{}",
            label,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0),
        ))
    }

    #[test]
    fn parses_scalar_frontmatter() {
        let md = "---\nname: review\ndescription: \"Code review\"\ndisplay-name: Review\ndisable-model-invocation: true\n---\n# body\n";
        let fm = parse_skill_frontmatter(md);
        assert_eq!(
            fm.get("name").and_then(|v| match v {
                FrontmatterValue::Str(s) => Some(s.as_str()),
                _ => None,
            }),
            Some("review")
        );
        assert_eq!(
            fm.get("description").and_then(|v| match v {
                FrontmatterValue::Str(s) => Some(s.as_str()),
                _ => None,
            }),
            Some("Code review")
        );
        assert_eq!(
            fm.get("display-name").and_then(|v| match v {
                FrontmatterValue::Str(s) => Some(s.as_str()),
                _ => None,
            }),
            Some("Review")
        );
        assert_eq!(
            fm.get("disable-model-invocation").and_then(|v| match v {
                FrontmatterValue::Bool(b) => Some(*b),
                _ => None,
            }),
            Some(true)
        );
    }

    #[test]
    fn frontmatter_missing_returns_empty() {
        let fm = parse_skill_frontmatter("# no frontmatter here\nbody");
        assert!(fm.is_empty());
    }

    #[test]
    fn collects_nested_skill_md() {
        let tmp = tmp_dir("nested");
        let root = tmp.join(".claude").join("skills");
        let skill_dir = root.join("review");
        std::fs::create_dir_all(&skill_dir).unwrap();
        std::fs::write(
            skill_dir.join("SKILL.md"),
            "---\nname: review\ndescription: Review specialist.\n---\n# Review\nbody",
        )
        .unwrap();
        // A non-markdown sibling must be ignored.
        std::fs::write(skill_dir.join("notes.txt"), "ignore").unwrap();

        let paths = collect_skill_markdown_paths(&root, false);
        assert_eq!(paths.len(), 1);
        assert!(paths[0].ends_with("SKILL.md"));

        let descriptor = read_skill_descriptor(&paths[0], "claude").unwrap();
        assert_eq!(descriptor.name, "review");
        assert_eq!(
            descriptor.description.as_deref(),
            Some("Review specialist.")
        );
        assert!(descriptor.enabled);
        assert_eq!(descriptor.scope, "claude");

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn collects_flat_markdown_for_pi_origin() {
        let tmp = tmp_dir("pi-flat");
        let root = tmp.join(".pi").join("agent").join("skills");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("alpha.md"), "---\nname: alpha\n---\n# Alpha").unwrap();
        std::fs::write(root.join("beta.md"), "# Beta").unwrap();
        // lowercase skill.md must be ignored even with include_markdown_files.
        std::fs::write(root.join("skill.md"), "# ignored").unwrap();

        let paths = collect_skill_markdown_paths(&root, true);
        assert_eq!(paths.len(), 2);

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn dedup_by_name_keeps_first_root() {
        let tmp = tmp_dir("dedup");
        let a = tmp.join("a");
        let b = tmp.join("b");
        std::fs::create_dir_all(a.join("foo")).unwrap();
        std::fs::write(a.join("foo").join("SKILL.md"), "---\nname: foo\n---\n").unwrap();
        std::fs::create_dir_all(b.join("foo")).unwrap();
        std::fs::write(b.join("foo").join("SKILL.md"), "---\nname: foo\n---\n").unwrap();

        let roots = vec![
            SkillRoot {
                path: a.clone(),
                scope: "claude".into(),
                include_markdown_files: false,
            },
            SkillRoot {
                path: b.clone(),
                scope: "mcode".into(),
                include_markdown_files: false,
            },
        ];
        let skills = collect_skills_from_roots(&roots);
        assert_eq!(skills.len(), 1, "dedup should keep one");
        assert_eq!(skills[0].scope, "claude", "first root wins");

        // include_duplicate_origins keeps both.
        let dup = collect_skill_descriptors_from_roots(&roots);
        assert_eq!(dup.len(), 2);

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn filter_disabled_skills_by_lowercase_name() {
        let skills = vec![
            SkillDescriptor {
                name: "Review".into(),
                description: None,
                path: "/x".into(),
                enabled: true,
                scope: "claude".into(),
                interface: None,
            },
            SkillDescriptor {
                name: "Explore".into(),
                description: None,
                path: "/y".into(),
                enabled: true,
                scope: "mcode".into(),
                interface: None,
            },
        ];
        let filtered = filter_disabled_skills(&skills, &["review".into()]);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].name, "Explore");
    }

    #[test]
    fn disable_model_invocation_marks_disabled() {
        let tmp = tmp_dir("disabled");
        let root = tmp.join("s");
        std::fs::create_dir_all(root.join("off")).unwrap();
        std::fs::write(
            root.join("off").join("SKILL.md"),
            "---\nname: off\ndisable-model-invocation: true\n---\n",
        )
        .unwrap();
        let paths = collect_skill_markdown_paths(&root, false);
        let d = read_skill_descriptor(&paths[0], "mcode").unwrap();
        assert!(!d.enabled);

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn ancestors_from_deepest_starts_with_cwd() {
        let tmp = tmp_dir("ancestors");
        std::fs::create_dir_all(&tmp).unwrap();
        let ancestors = ancestors_from_deepest(&tmp);
        assert_eq!(ancestors[0], std::fs::canonicalize(&tmp).unwrap());
        assert!(ancestors.len() >= 2);
        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn discover_uses_cache_within_ttl() {
        clear_skills_catalog_cache_for_tests();
        let tmp = tmp_dir("cache");
        let root = tmp.join(".claude").join("skills");
        std::fs::create_dir_all(root.join("cached")).unwrap();
        std::fs::write(
            root.join("cached").join("SKILL.md"),
            "---\nname: cached\n---\n",
        )
        .unwrap();

        // `discover_skills_catalog` uses `input.home_dir` directly (not the HOME
        // env), so home roots resolve under the temp dir deterministically.
        let input = DiscoveryInput {
            cwd: None,
            home_dir: Some(tmp.to_string_lossy().into_owned()),
            provider: None,
            include_duplicate_origins: false,
            force_reload: true,
        };
        let first = discover_skills_catalog(input);
        assert!(first.iter().any(|s| s.name == "cached"));

        // Mutate the filesystem after the first scan; without force_reload the
        // cache should mask the change within the TTL window.
        std::fs::create_dir_all(root.join("added")).ok();
        std::fs::write(
            root.join("added").join("SKILL.md"),
            "---\nname: added\n---\n",
        )
        .unwrap();

        let cached = discover_skills_catalog(DiscoveryInput {
            cwd: None,
            home_dir: Some(tmp.to_string_lossy().into_owned()),
            provider: None,
            include_duplicate_origins: false,
            force_reload: false,
        });
        assert!(
            !cached.iter().any(|s| s.name == "added"),
            "cache should hide the new file"
        );

        // force_reload bypasses the cache.
        let fresh = discover_skills_catalog(DiscoveryInput {
            cwd: None,
            home_dir: Some(tmp.to_string_lossy().into_owned()),
            provider: None,
            include_duplicate_origins: false,
            force_reload: true,
        });
        assert!(fresh.iter().any(|s| s.name == "added"));

        std::fs::remove_dir_all(&tmp).ok();
    }
}
