// FILE: AgentsSettingsPanel.tsx
// Purpose: Settings → Agents panel. Lists the cross-provider agent skills
//          installed in the shared `.agents/skills` folder (scope "agents"),
//          which are split out from the Skills panel so each surface has its
//          own navigation entry. The toggle semantics are identical to
//          SkillsSettingsPanel — disabling an agent hides it from the composer
//          skill picker on every provider.
// Layer: Settings panel

import type { ProviderKind, ServerSettings } from "@t3tools/contracts";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useMemo } from "react";

import { ProviderIcon } from "~/components/ProviderIcon";
import { SettingsRow, SettingsSection } from "~/components/settings/SettingsPanelPrimitives";
import { Switch } from "~/components/ui/switch";
import { SkillCubeIcon } from "~/lib/icons";
import { ensureNativeApi } from "~/nativeApi";
import {
  providerDiscoveryQueryKeys,
  skillsCatalogQueryOptions,
} from "~/lib/providerDiscoveryReactQuery";
import { serverQueryKeys, serverSettingsQueryOptions } from "~/lib/serverReactQuery";
import {
  buildSettingsSkillGroups,
  buildSettingsSkillSections,
  providerDisplayName,
  settingsSkillNameKey,
} from "./skillsSettingsModel";

function SkillProviderStack({ providers }: { providers: ReadonlyArray<ProviderKind> }) {
  if (providers.length === 0) {
    return null;
  }
  const label = providers.map(providerDisplayName).join(", ");
  const stackLabel = `Provider ${providers.length === 1 ? "copy" : "copies"}: ${label}`;
  return (
    <span
      className="inline-flex shrink-0 items-center -space-x-1"
      aria-label={stackLabel}
      title={stackLabel}
    >
      {providers.map((provider) => (
        <span
          key={provider}
          className="inline-flex size-4 items-center justify-center rounded-full border border-background bg-background"
        >
          <ProviderIcon provider={provider} className="size-3" />
        </span>
      ))}
    </span>
  );
}

export function AgentsSettingsPanel() {
  const queryClient = useQueryClient();
  const catalogQuery = useQuery(skillsCatalogQueryOptions());
  const serverSettingsQuery = useQuery(serverSettingsQueryOptions());

  const disabledSkillNames = useMemo(
    () =>
      new Set(
        (serverSettingsQuery.data?.skills.disabled ?? []).map((name) => settingsSkillNameKey(name)),
      ),
    [serverSettingsQuery.data?.skills.disabled],
  );

  // Agent skills: entries from the shared `.agents/skills` folder (scope
  // "agents") PLUS per-provider subagent folders (`.claude/agents`,
  // `.codex/agents`, …, scope "agents-<provider>"). Provider-folder skills
  // (.codex/skills, .claude/skills, …) live on the Skills settings page.
  const agentSkills = useMemo(
    () =>
      (catalogQuery.data?.skills ?? []).filter((skill) => {
        const scope = skill.scope ?? "";
        return scope === "agents" || scope.startsWith("agents-");
      }),
    [catalogQuery.data?.skills],
  );

  const skillGroups = useMemo(() => buildSettingsSkillGroups(agentSkills), [agentSkills]);
  const skillSections = useMemo(
    () => buildSettingsSkillSections(agentSkills, "Shared agents"),
    [agentSkills],
  );

  const setSkillEnabled = (skillName: string, enabled: boolean) => {
    // Read through the query cache (not the render closure) so rapid toggles
    // build on each other instead of clobbering the previous patch.
    const latestSettings = queryClient.getQueryData<ServerSettings>(serverQueryKeys.settings());
    const currentDisabled = latestSettings?.skills.disabled ?? [...disabledSkillNames];
    const key = settingsSkillNameKey(skillName);
    const next = new Set(currentDisabled.map((name) => settingsSkillNameKey(name)));
    if (enabled) {
      next.delete(key);
    } else {
      next.add(key);
    }
    const disabled = [...next].sort();
    if (latestSettings) {
      // Optimistic flip; a failed patch invalidates back to the server state.
      queryClient.setQueryData(serverQueryKeys.settings(), {
        ...latestSettings,
        skills: { disabled },
      });
    }
    void ensureNativeApi()
      .server.updateSettings({ skills: { disabled } })
      .then((nextSettings) => {
        queryClient.setQueryData(serverQueryKeys.settings(), nextSettings);
        // Composer skill pickers are served filtered by these toggles.
        void queryClient.invalidateQueries({ queryKey: providerDiscoveryQueryKeys.all });
      })
      .catch(() => {
        void queryClient.invalidateQueries({ queryKey: serverQueryKeys.settings() });
      });
  };

  const totalSkills = skillGroups.length;
  const enabledSkills = skillGroups.filter((group) => !disabledSkillNames.has(group.key)).length;

  return (
    <div className="space-y-8">
      <SettingsSection title="Shared agents">
        <SettingsRow
          title="Agent skills folder"
          description="Agents from your shared .agents/skills folder and each provider's agents/ folder (~/.claude/agents, ~/.codex/agents, …) are listed below. Add portable agents to ~/.syncode/agents to share them across every provider."
          control={
            <span className="text-xs font-medium text-muted-foreground">
              {catalogQuery.isLoading
                ? "Scanning…"
                : `${enabledSkills} of ${totalSkills} agent${totalSkills === 1 ? "" : "s"} enabled`}
            </span>
          }
        />
      </SettingsSection>

      {catalogQuery.isError ? (
        <SettingsSection title="Agents">
          <SettingsRow
            title="Agent discovery failed"
            description="MCode could not scan the agent skill folders. Retry after checking that the server is running."
          />
        </SettingsSection>
      ) : null}

      {!catalogQuery.isLoading && !catalogQuery.isError && totalSkills === 0 ? (
        <SettingsSection title="Agents">
          <SettingsRow
            title="No agents found"
            description="Add a skill folder containing a SKILL.md to your .agents/skills folder to make it available across providers."
          />
        </SettingsSection>
      ) : null}

      {skillSections.map((section) => (
        <SettingsSection key={section.key} title={section.title}>
          {section.groups.map((group) => {
            const enabled = !disabledSkillNames.has(group.key);
            return (
              <SettingsRow
                key={group.key}
                title={
                  <span className="inline-flex min-w-0 items-center gap-1.5">
                    <SkillCubeIcon
                      aria-hidden="true"
                      className="size-3.5 shrink-0 text-muted-foreground"
                    />
                    <span className="truncate">{group.displayName}</span>
                  </span>
                }
                description={group.description}
                status={
                  <span className="flex min-w-0 flex-col gap-1">
                    <span className="flex min-w-0 items-center gap-1.5">
                      <SkillProviderStack providers={group.providers} />
                      <span className="truncate text-[11px] text-muted-foreground">
                        {group.sources.map((source) => source.originInfo.label).join(" · ")}
                      </span>
                    </span>
                    {group.sources.map((source) => (
                      <code
                        key={source.skill.path}
                        className="truncate text-[11px] text-muted-foreground"
                      >
                        {source.skill.path}
                      </code>
                    ))}
                  </span>
                }
                control={
                  <Switch
                    checked={enabled}
                    onCheckedChange={(checked) =>
                      setSkillEnabled(group.primarySkill.name, Boolean(checked))
                    }
                    aria-label={`Enable the ${group.displayName} agent`}
                  />
                }
              />
            );
          })}
        </SettingsSection>
      ))}
    </div>
  );
}
