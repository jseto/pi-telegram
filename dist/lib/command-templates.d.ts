/**
 * Command-template execution standard.
 * Zones: shell-free command parsing, placeholder expansion, local process execution, composition semantics
 * Owns portable command-template parsing, expansion, risk checks, retries, timeouts, and direct execution.
 */
export type CommandTemplateFailureScope = "continue" | "branch" | "root";
export interface CommandTemplateActorRecipeContext {
    alias?: string;
    file?: string;
    name?: string;
    path?: string;
    role?: string;
}
export interface CommandTemplateObjectConfig {
    actorRecipeContext?: CommandTemplateActorRecipeContext;
    label?: string;
    parallel?: boolean;
    when?: boolean | string;
    template?: CommandTemplateValue;
    args?: string[];
    defaults?: Record<string, unknown>;
    timeout?: number | string;
    delay?: number | string;
    output?: string;
    retry?: number | string;
    failure?: CommandTemplateFailureScope;
    recover?: CommandTemplateValue;
    repeat?: number | string;
}
export type CommandTemplateValue = string | CommandTemplateConfig[] | CommandTemplateObjectConfig;
export type CommandTemplateConfig = string | CommandTemplateObjectConfig;
export interface CommandTemplateLeafConfig extends CommandTemplateObjectConfig {
    template: string;
    /** `when` guards of the composition nodes enclosing this leaf; every one must pass. */
    inheritedWhen?: readonly (boolean | string)[];
}
type CommandTemplateExpansionContext = Pick<CommandTemplateLeafConfig, "args" | "defaults" | "inheritedWhen">;
export interface CommandTemplateInvocation {
    command: string;
    args: string[];
}
export interface CommandTemplateExecOptions {
    cwd?: string;
    timeout?: number;
    signal?: AbortSignal;
    stdin?: string;
    killGrace?: number;
    retry?: number;
    /** Cleanup between a failed attempt and the next retry; a thrown error stops retries. */
    recover?: () => Promise<void>;
}
export interface CommandTemplateExecResult {
    stdout: string;
    stderr: string;
    code: number;
    killed: boolean;
}
export declare function normalizeCommandTemplateConfig(config: CommandTemplateConfig): CommandTemplateObjectConfig;
export declare function expandCommandTemplateConfigs(config: CommandTemplateConfig, inherited?: CommandTemplateExpansionContext): CommandTemplateLeafConfig[];
/** Whether a node's own and enclosing `when` guards all pass for its resolved placeholder values. */
export declare function shouldRunCommandTemplateConfig(config: CommandTemplateConfig | CommandTemplateLeafConfig, values: Record<string, unknown>): boolean;
/** A node's `recover` template as one cleanup run: output ignored, failure stops retries unless a leaf opts into `continue`. */
export declare function createCommandTemplateRecovery(config: CommandTemplateConfig, values: Record<string, unknown>, options: {
    cwd: string;
    timeout?: number;
    execCommand: (command: string, args: string[], options?: CommandTemplateExecOptions) => Promise<CommandTemplateExecResult>;
}): (() => Promise<void>) | undefined;
export declare function splitCommandTemplate(input: string): string[];
export declare function shouldRunCommandTemplateNode(value: boolean | string | undefined, values: Record<string, unknown>): boolean;
/** Resolve one optional non-negative numeric control field, substituting a template token when given as text. */
export declare function resolveCommandTemplateNumericField(value: number | string | undefined, values: Record<string, unknown>, label: string): number | undefined;
/** A node's own `timeout`; string leaves carry none. */
export declare function getCommandTemplateConfiguredTimeout(config: CommandTemplateConfig | undefined): number | undefined;
/**
 * Bound one composed step by its own timeout and the handler budget left after `elapsedMs`.
 * Without `elapsedMs` the step receives the full budget.
 */
export declare function getCommandTemplateStepTimeout(budgetMs: number, step: CommandTemplateConfig, elapsedMs?: number): number;
/** Array templates expand into ordered composition steps; single-command templates have none. */
export declare function getCommandTemplateCompositionSteps(config: CommandTemplateObjectConfig): CommandTemplateLeafConfig[];
export declare function execCommandTemplate(command: string, args: string[], options?: CommandTemplateExecOptions): Promise<CommandTemplateExecResult>;
export declare function buildCommandTemplateInvocation(config: CommandTemplateConfig, values: Record<string, unknown>, cwd: string, options?: {
    emptyMessage?: string;
    missingLabel?: string;
}): CommandTemplateInvocation;
export {};
