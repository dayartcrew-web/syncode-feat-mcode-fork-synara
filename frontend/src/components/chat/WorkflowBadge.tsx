/**
 * Compact workflow-context badge rendered in `ChatHeader`. Shows the active
 * workflow phase (v1: always `EXECUTE`) plus the current task title; a
 * tooltip surfaces the workflowId + task index for debugging.
 *
 * Renders `null` when no workflow state has been received yet (no
 * `thread.workflow-context-bound` event has landed for this thread — e.g.
 * in-memory mode, brand-new thread before first turn, or pre-C3 backend).
 */
import { memo } from "react";
import { FiActivity } from "react-icons/fi";
import { Badge } from "../ui/badge";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../ui/tooltip";
import { cn } from "~/lib/utils";
import type { ThreadWorkflowState } from "../../hooks/useThreadWorkflowState";

interface WorkflowBadgeProps {
  readonly workflow: ThreadWorkflowState | null;
  /** Optional className passthrough so the header can tweak sizing. */
  readonly className?: string;
}

function truncate(text: string, max: number): string {
  if (text.length <= max) return text;
  return `${text.slice(0, Math.max(0, max - 1)).trimEnd()}…`;
}

function phaseLabel(phase: string): string {
  // Phase strings come from the backend as uppercase tokens
  // (INIT/ANALYZE/PLAN/EXECUTE/VERIFY/DONE). Title-case for display so the
  // badge reads as a label rather than a shouty constant. Falls through
  // unchanged for anything that doesn't match the canonical phase list.
  switch (phase) {
    case "INIT":
      return "Init";
    case "ANALYZE":
      return "Analyze";
    case "PLAN":
      return "Plan";
    case "EXECUTE":
      return "Execute";
    case "VERIFY":
      return "Verify";
    case "DONE":
      return "Done";
    default:
      return phase;
  }
}

function WorkflowBadgeImpl({ workflow, className }: WorkflowBadgeProps) {
  if (!workflow) return null;
  const task = workflow.currentTask ? truncate(workflow.currentTask, 48) : null;
  const tipParts: string[] = [`Workflow ${workflow.workflowId}`, `Phase: ${workflow.phase}`];
  if (workflow.currentTaskIndex !== null && workflow.totalTasks !== null) {
    tipParts.push(`Task ${workflow.currentTaskIndex}/${workflow.totalTasks}`);
  }
  if (task) tipParts.push(`Current: ${workflow.currentTask ?? ""}`);
  const tooltipText = tipParts.join(" · ");

  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <Badge
            variant="outline"
            className={cn(
              "hidden !h-6 shrink-0 items-center justify-center gap-1 rounded-md px-1.5 text-[10px] font-medium uppercase tracking-wide text-muted-foreground sm:inline-flex",
              className,
            )}
            data-testid="workflow-badge"
          >
            <FiActivity className="size-3 shrink-0 opacity-70" />
            <span>{phaseLabel(workflow.phase)}</span>
            {task ? <span className="normal-case text-muted-foreground/80">· {task}</span> : null}
          </Badge>
        }
      />
      <TooltipPopup>{tooltipText}</TooltipPopup>
    </Tooltip>
  );
}

export const WorkflowBadge = memo(WorkflowBadgeImpl);
