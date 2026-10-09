/**
 * Command-template execution standard.
 * Zones: shell-free command parsing, placeholder expansion, local process execution, composition semantics
 * Owns portable command-template parsing, expansion, risk checks, retries, timeouts, and direct execution.
 */

import { spawn } from "node:child_process";
import { homedir } from "node:os";
import { extname, isAbsolute, normalize, resolve } from "node:path";

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

export type CommandTemplateValue =
  string | CommandTemplateConfig[] | CommandTemplateObjectConfig;

export type CommandTemplateConfig = string | CommandTemplateObjectConfig;

export interface CommandTemplateLeafConfig extends CommandTemplateObjectConfig {
  template: string;
  /** `when` guards of the composition nodes enclosing this leaf; every one must pass. */
  inheritedWhen?: readonly (boolean | string)[];
}

type CommandTemplateExpansionContext = Pick<
  CommandTemplateLeafConfig,
  "args" | "defaults" | "inheritedWhen"
>;

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

function normalizeCommandTemplateArgs(value: string[] | undefined): string[] {
  if (!Array.isArray(value)) return [];
  return value.map(String).map((item) => item.trim());
}

export function normalizeCommandTemplateConfig(
  config: CommandTemplateConfig,
): CommandTemplateObjectConfig {
  return typeof config === "string" ? { template: config } : config;
}

function normalizeRecoverConfig(
  config: CommandTemplateValue | undefined,
): CommandTemplateConfig | undefined {
  if (config === undefined) return undefined;
  return Array.isArray(config) ? { template: config } : config;
}

function normalizeCommandTemplateDefaults(
  defaults: Record<string, unknown> | undefined,
): Record<string, unknown> | undefined {
  if (!defaults) return undefined;
  const normalized: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(defaults)) {
    normalized[key] = Array.isArray(value)
      ? value
      : value === undefined || value === null
        ? ""
        : String(value);
  }
  return normalized;
}

function resolveInheritedDefaultReferences(
  ownDefaults: Record<string, unknown> | undefined,
  inheritedDefaults: Record<string, unknown> | undefined,
  runtimeValues: Record<string, unknown> = {},
): Record<string, unknown> | undefined {
  if (!ownDefaults || !inheritedDefaults) return ownDefaults;
  const resolved = { ...ownDefaults };
  for (const [key, value] of Object.entries(ownDefaults)) {
    if (typeof value !== "string") continue;
    const exact = /^\{([A-Za-z_][A-Za-z0-9_-]*)\}$/.exec(value);
    if (
      !exact ||
      Object.hasOwn(runtimeValues, exact[1]) ||
      !Object.hasOwn(inheritedDefaults, exact[1])
    )
      continue;
    resolved[key] = inheritedDefaults[exact[1]];
  }
  return resolved;
}

function resolveCommandTemplateRepeat(
  value: number | string | undefined,
  values: Record<string, unknown> = {},
): number | undefined {
  if (value === undefined) return undefined;
  if (typeof value === "number") {
    if (!Number.isInteger(value) || value < 1)
      throw new Error("Command template repeat must be a positive integer.");
    return value;
  }
  const trimmed = value.trim();
  if (/^\d+$/.test(trimmed)) return Number(trimmed);
  const lengthMatch = trimmed.match(
    /^\{?([A-Za-z_][A-Za-z0-9_-]*)\.length\}?$/,
  );
  if (lengthMatch) {
    const source = values[lengthMatch[1]];
    if (Array.isArray(source)) return source.length;
    if (source === undefined) return undefined;
  }
  throw new Error(
    "Command template repeat must be a positive integer or {array.length}.",
  );
}

function pad(value: number, width: number): string {
  return String(value).padStart(width, "0");
}

function getCommandTemplateRepeatDefaults(
  index: number,
  repeat: number,
): Record<string, string> {
  const prev = (index - 1 + repeat) % repeat;
  const next = (index + 1) % repeat;
  const values: Record<string, string> = {
    index: String(index),
    next: String(next),
    prev: String(prev),
    repeat: String(repeat),
  };
  for (const name of ["index", "prev", "next", "repeat"]) {
    const numeric = Number(values[name]);
    for (let underscores = 1; underscores <= 6; underscores += 1) {
      values[`${"_".repeat(underscores)}${name}`] = pad(
        numeric,
        underscores + 1,
      );
    }
  }
  return values;
}

function expandRepeatConfig(
  config: CommandTemplateObjectConfig,
  context: Pick<CommandTemplateObjectConfig, "args" | "defaults">,
): CommandTemplateObjectConfig[] | undefined {
  const repeat = resolveCommandTemplateRepeat(
    config.repeat,
    context.defaults ?? {},
  );
  if (repeat === undefined) return undefined;
  return Array.from({ length: repeat }, (_unused, index0) => {
    const { repeat: _repeat, ...rest } = config;
    return {
      ...rest,
      defaults: {
        ...(context.defaults ?? {}),
        ...(rest.defaults ?? {}),
        ...getCommandTemplateRepeatDefaults(index0, repeat),
      },
    };
  });
}

export function expandCommandTemplateConfigs(
  config: CommandTemplateConfig,
  inherited: CommandTemplateExpansionContext = {},
): CommandTemplateLeafConfig[] {
  const normalizedConfig = normalizeCommandTemplateConfig(config);
  const inheritedDefaults = normalizeCommandTemplateDefaults(
    inherited.defaults,
  );
  const ownDefaults = resolveInheritedDefaultReferences(
    normalizeCommandTemplateDefaults(normalizedConfig.defaults),
    inheritedDefaults,
  );
  const context: CommandTemplateExpansionContext = {
    ...(inherited.inheritedWhen?.length
      ? { inheritedWhen: inherited.inheritedWhen }
      : {}),
    ...(inherited.args !== undefined ? { args: inherited.args } : {}),
    ...(inheritedDefaults ? { defaults: inheritedDefaults } : {}),
    ...(normalizedConfig.args !== undefined
      ? { args: normalizedConfig.args }
      : {}),
    ...(ownDefaults
      ? { defaults: { ...(inheritedDefaults ?? {}), ...ownDefaults } }
      : {}),
  };
  const repeated = expandRepeatConfig(normalizedConfig, context);
  if (repeated) {
    return repeated.flatMap((step) =>
      expandCommandTemplateConfigs(step, context),
    );
  }
  // `recover` is cleanup between failed retries, never a sequence step; runners attach it per leaf.
  if (Array.isArray(normalizedConfig.template)) {
    const childContext: CommandTemplateExpansionContext =
      normalizedConfig.when === undefined
        ? context
        : {
            ...context,
            inheritedWhen: [
              ...(context.inheritedWhen ?? []),
              normalizedConfig.when,
            ],
          };
    return normalizedConfig.template.flatMap((step) =>
      expandCommandTemplateConfigs(step, childContext),
    );
  }
  if (typeof normalizedConfig.template !== "string") return [];
  return [
    {
      ...normalizedConfig,
      ...context,
      template: normalizedConfig.template,
      retry: normalizedConfig.retry,
    },
  ];
}

/** Whether a node's own and enclosing `when` guards all pass for its resolved placeholder values. */
export function shouldRunCommandTemplateConfig(
  config: CommandTemplateConfig | CommandTemplateLeafConfig,
  values: Record<string, unknown>,
): boolean {
  if (typeof config === "string") return true;
  const inheritedWhen =
    "inheritedWhen" in config ? (config.inheritedWhen ?? []) : [];
  if (config.when === undefined && inheritedWhen.length === 0) return true;
  const resolved = { ...getCommandTemplateDefaults(config), ...values };
  return [...inheritedWhen, config.when].every((condition) =>
    shouldRunCommandTemplateNode(condition, resolved),
  );
}

/** A node's `recover` template as one cleanup run: output ignored, failure stops retries unless a leaf opts into `continue`. */
export function createCommandTemplateRecovery(
  config: CommandTemplateConfig,
  values: Record<string, unknown>,
  options: {
    cwd: string;
    timeout?: number;
    execCommand: (
      command: string,
      args: string[],
      options?: CommandTemplateExecOptions,
    ) => Promise<CommandTemplateExecResult>;
  },
): (() => Promise<void>) | undefined {
  if (typeof config === "string") return undefined;
  const recoverConfig = normalizeRecoverConfig(config.recover);
  if (!recoverConfig) return undefined;
  const steps = expandCommandTemplateConfigs(recoverConfig, {
    ...(config.args !== undefined ? { args: config.args } : {}),
    ...(config.defaults !== undefined ? { defaults: config.defaults } : {}),
  });
  return async () => {
    for (const step of steps) {
      if (!shouldRunCommandTemplateConfig(step, values)) continue;
      const invocation = buildCommandTemplateInvocation(
        step,
        values,
        options.cwd,
        { missingLabel: "command template recover" },
      );
      const result = await options.execCommand(
        invocation.command,
        invocation.args,
        {
          cwd: options.cwd,
          ...(options.timeout !== undefined
            ? { timeout: options.timeout }
            : {}),
        },
      );
      if (result.code !== 0 && step.failure !== "continue") {
        throw new Error(
          `Command template recover exited with code ${result.code}${result.killed ? " (killed)" : ""}`,
        );
      }
    }
  };
}

function parseCommandTemplateArgToken(value: string): {
  name: string;
  defaultValue?: string;
} {
  const separatorIndex = value.indexOf("=");
  const rawName =
    separatorIndex === -1 ? value : value.slice(0, separatorIndex);
  const colonIndex = rawName.indexOf(":");
  return {
    name: (colonIndex === -1 ? rawName : rawName.slice(0, colonIndex)).trim(),
    ...(separatorIndex === -1
      ? {}
      : { defaultValue: value.slice(separatorIndex + 1).trim() }),
  };
}

function parseCommandTemplatePlaceholderContent(
  content: string,
): { name: string; inlineDefault?: string } | undefined {
  const match = content.match(
    /^([A-Za-z_][A-Za-z0-9_-]*)(?::(?:string|path|int|number|bool|array|enum\([^)]*\)))?(?:=([^}]*))?$/,
  );
  if (!match) return undefined;
  return {
    name: match[1],
    ...(match[2] !== undefined ? { inlineDefault: match[2] } : {}),
  };
}

function getCommandTemplateDefaults(
  config: CommandTemplateConfig | undefined,
): Record<string, string> {
  const normalizedConfig = config
    ? normalizeCommandTemplateConfig(config)
    : undefined;
  const defaults: Record<string, string> = {};
  for (const item of normalizeCommandTemplateArgs(normalizedConfig?.args)) {
    if (!item) continue;
    const parsed = parseCommandTemplateArgToken(item);
    if (!parsed.name || parsed.defaultValue === undefined) continue;
    defaults[parsed.name] = parsed.defaultValue;
  }
  for (const [key, value] of Object.entries(normalizedConfig?.defaults ?? {})) {
    defaults[key] = value === undefined || value === null ? "" : String(value);
  }
  return defaults;
}

export function splitCommandTemplate(input: string): string[] {
  const words: string[] = [];
  let current = "";
  let quote: "'" | '"' | undefined;
  let escaped = false;
  let active = false;
  for (let index = 0; index < input.length; index += 1) {
    const char = input[index] ?? "";
    if (escaped) {
      current += char;
      escaped = false;
      active = true;
      continue;
    }
    if (char === "\\" && quote !== "'") {
      const next = input[index + 1];
      const escapesNext =
        quote === '"'
          ? next === '"' || next === "\\"
          : next !== undefined &&
            (/\s/u.test(next) || next === "'" || next === '"' || next === "\\");
      if (escapesNext) {
        escaped = true;
      } else {
        current += "\\";
      }
      active = true;
      continue;
    }
    if (quote) {
      if (char === quote) quote = undefined;
      else current += char;
      active = true;
      continue;
    }
    if (char === "'" || char === '"') {
      quote = char;
      active = true;
      continue;
    }
    if (/\s/.test(char)) {
      if (active) words.push(current);
      if (active) current = "";
      active = false;
      continue;
    }
    current += char;
    active = true;
  }
  if (escaped) current += "\\";
  if (active || current) words.push(current);
  return words;
}

function expandCommandTemplateExecutable(command: string, cwd: string): string {
  if (command === "~") return homedir();
  if (command.startsWith("~/")) return resolve(homedir(), command.slice(2));
  if (command.includes("/") && !isAbsolute(command))
    return resolve(cwd, command);
  return command;
}

function evaluateCommandTemplateExpression(
  expression: string,
  values: Record<string, unknown>,
): number {
  let index = 0;
  const source = expression.replace(/\s+/g, "");
  const peek = (): string | undefined => source[index];
  const consume = (char: string): boolean => {
    if (peek() !== char) return false;
    index += 1;
    return true;
  };
  const parsePrimary = (): number => {
    if (consume("(")) {
      const value = parseExpression();
      if (!consume(")"))
        throw new Error(`Invalid command template expression: ${expression}`);
      return value;
    }
    const numberMatch = source.slice(index).match(/^\d+/);
    if (numberMatch) {
      index += numberMatch[0].length;
      return Number(numberMatch[0]);
    }
    const nameMatch = source.slice(index).match(/^[A-Za-z_][A-Za-z0-9_-]*/);
    if (nameMatch) {
      index += nameMatch[0].length;
      const value = values[nameMatch[0]];
      if (value === undefined || !/^-?\d+$/.test(String(value)))
        throw new Error(
          `Invalid command template expression variable: ${nameMatch[0]}`,
        );
      return Number(value);
    }
    throw new Error(`Invalid command template expression: ${expression}`);
  };
  const parseTerm = (): number => {
    let value = parsePrimary();
    while (true) {
      if (consume("*")) value *= parsePrimary();
      else if (consume("/")) value = Math.trunc(value / parsePrimary());
      else if (consume("%")) value %= parsePrimary();
      else return value;
    }
  };
  const parseExpression = (): number => {
    let value = parseTerm();
    while (true) {
      if (consume("+")) value += parseTerm();
      else if (consume("-")) value -= parseTerm();
      else return value;
    }
  };
  const value = parseExpression();
  if (index !== source.length)
    throw new Error(`Invalid command template expression: ${expression}`);
  return value;
}

function substituteCommandTemplateExpression(
  content: string,
  values: Record<string, unknown>,
): string | undefined {
  const padded = content.match(/^(_{1,6})\((.+)\)$/);
  if (padded) {
    return pad(
      evaluateCommandTemplateExpression(padded[2], values),
      padded[1].length + 1,
    );
  }
  if (!/[()+\-*\/%]/.test(content)) return undefined;
  return String(evaluateCommandTemplateExpression(content, values));
}

function shouldResolveEmbeddedCommandTemplateToken(
  token: string,
  values: Record<string, unknown>,
): boolean {
  const matches = [...token.matchAll(/\{([^{}]+)\}/g)];
  if (matches.length === 0) return false;
  return matches.every((match) => {
    const content = match[1];
    if (resolveCommandTemplateNullish(content, values) !== undefined)
      return true;
    if (resolveCommandTemplateTernary(content, values) !== undefined)
      return true;
    const indexed = content.match(
      /^([A-Za-z_][A-Za-z0-9_-]*)\[([A-Za-z_][A-Za-z0-9_-]*|\d+)\]$/,
    );
    if (indexed) return Object.hasOwn(values, indexed[1]);
    const simple = parseCommandTemplatePlaceholderContent(content);
    if (simple)
      return (
        Object.hasOwn(values, simple.name) || simple.inlineDefault !== undefined
      );
    try {
      return substituteCommandTemplateExpression(content, values) !== undefined;
    } catch {
      return false;
    }
  });
}

function isFalsyCommandTemplateValue(value: unknown): boolean {
  if (value === undefined || value === null || value === false) return true;
  const normalized = String(value).trim().toLowerCase();
  return (
    normalized === "" ||
    normalized === "0" ||
    normalized === "false" ||
    normalized === "no"
  );
}

function resolveCommandTemplateCondition(
  condition: string,
  values: Record<string, unknown>,
): unknown {
  const trimmed = condition.trim();
  const negated = trimmed.startsWith("!");
  const name = negated ? trimmed.slice(1).trim() : trimmed;
  const value = /^[A-Za-z_][A-Za-z0-9_-]*$/.test(name)
    ? values[name]
    : undefined;
  return negated ? isFalsyCommandTemplateValue(value) : value;
}

export function shouldRunCommandTemplateNode(
  value: boolean | string | undefined,
  values: Record<string, unknown>,
): boolean {
  if (value === undefined) return true;
  if (typeof value === "boolean") return value;
  const trimmed = value.trim();
  if (!trimmed) return false;
  const exact = /^\{([^{}]+)\}$/.exec(trimmed);
  const resolved = exact
    ? resolveCommandTemplateValue(exact[1], values, "command template when")
    : resolveCommandTemplateCondition(trimmed, values);
  return !isFalsyCommandTemplateValue(resolved);
}

function resolveCommandTemplateNullish(
  content: string,
  values: Record<string, unknown>,
): string | undefined {
  const coalescing = content.match(/^([A-Za-z_][A-Za-z0-9_-]*)\?\?(.*)$/);
  if (!coalescing) return undefined;
  const value = values[coalescing[1]];
  return isFalsyCommandTemplateValue(value) ? coalescing[2] : String(value);
}

function resolveCommandTemplateTernary(
  content: string,
  values: Record<string, unknown>,
): string | undefined {
  const ternary = content.match(/^([^?:]+)\?([^:]*):(.*)$/);
  if (!ternary) return undefined;
  const condition = resolveCommandTemplateCondition(ternary[1], values);
  return isFalsyCommandTemplateValue(condition) ? ternary[3] : ternary[2];
}

function resolveCommandTemplateValue(
  content: string,
  values: Record<string, unknown>,
  missingLabel: string,
  depth = 0,
): string | undefined {
  if (depth > 5)
    throw new Error(`Command template value recursion exceeded: ${content}`);
  const nullish = resolveCommandTemplateNullish(content, values);
  if (nullish !== undefined) return nullish;
  const ternary = resolveCommandTemplateTernary(content, values);
  if (ternary !== undefined) return ternary;
  const indexed = content.match(
    /^([A-Za-z_][A-Za-z0-9_-]*)\[([A-Za-z_][A-Za-z0-9_-]*|\d+)\]$/,
  );
  if (indexed) {
    const source = values[indexed[1]];
    const indexValue = /^\d+$/.test(indexed[2])
      ? indexed[2]
      : values[indexed[2]];
    const index = Number(indexValue);
    if (
      !Array.isArray(source) ||
      !Number.isInteger(index) ||
      index < 0 ||
      index >= source.length
    ) {
      throw new Error(`Missing ${missingLabel} value: ${content}`);
    }
    return String(source[index] ?? "");
  }
  const simple = parseCommandTemplatePlaceholderContent(content);
  if (simple) {
    if (Object.hasOwn(values, simple.name)) {
      const raw = values[simple.name] ?? "";
      if (
        typeof raw === "string" &&
        shouldResolveEmbeddedCommandTemplateToken(raw, values)
      ) {
        return substituteCommandTemplateToken(
          raw,
          values,
          missingLabel,
          depth + 1,
        );
      }
      return Array.isArray(raw) ? JSON.stringify(raw) : String(raw);
    }
    if (simple.inlineDefault !== undefined) return simple.inlineDefault;
  }
  const expression = substituteCommandTemplateExpression(content, values);
  if (expression !== undefined) return expression;
  return undefined;
}

/** Resolve one optional non-negative numeric control field, substituting a template token when given as text. */
export function resolveCommandTemplateNumericField(
  value: number | string | undefined,
  values: Record<string, unknown>,
  label: string,
): number | undefined {
  if (value === undefined) return undefined;
  const resolved =
    typeof value === "string"
      ? substituteCommandTemplateToken(value, values, label)
      : value;
  if (resolved === "") return undefined;
  const numeric = Number(resolved);
  if (!Number.isFinite(numeric) || numeric < 0)
    throw new Error(`Command template ${label} must be a non-negative number.`);
  return numeric;
}

/** A node's own `timeout`; string leaves carry none. */
export function getCommandTemplateConfiguredTimeout(
  config: CommandTemplateConfig | undefined,
): number | undefined {
  const timeout = typeof config === "string" ? undefined : config?.timeout;
  return resolveCommandTemplateNumericField(timeout, {}, "timeout");
}

/**
 * Bound one composed step by its own timeout and the handler budget left after `elapsedMs`.
 * Without `elapsedMs` the step receives the full budget.
 */
export function getCommandTemplateStepTimeout(
  budgetMs: number,
  step: CommandTemplateConfig,
  elapsedMs?: number,
): number {
  const remaining =
    elapsedMs === undefined ? budgetMs : Math.max(1, budgetMs - elapsedMs);
  const stepTimeout = getCommandTemplateConfiguredTimeout(step);
  return stepTimeout === undefined
    ? remaining
    : Math.min(stepTimeout, remaining);
}

/** Array templates expand into ordered composition steps; single-command templates have none. */
export function getCommandTemplateCompositionSteps(
  config: CommandTemplateObjectConfig,
): CommandTemplateLeafConfig[] {
  return Array.isArray(config.template)
    ? expandCommandTemplateConfigs(config)
    : [];
}

function substituteCommandTemplateToken(
  token: string,
  values: Record<string, unknown>,
  missingLabel = "command template",
  depth = 0,
): string {
  return token.replace(/\{([^{}]+)\}/g, (_match, content: string) => {
    const resolved = resolveCommandTemplateValue(
      content,
      values,
      missingLabel,
      depth,
    );
    if (resolved !== undefined) return resolved;
    throw new Error(`Missing ${missingLabel} value: ${content}`);
  });
}

export async function execCommandTemplate(
  command: string,
  args: string[],
  options: CommandTemplateExecOptions = {},
): Promise<CommandTemplateExecResult> {
  const maxAttempts = options.retry ?? 1;
  let lastResult: CommandTemplateExecResult = {
    stdout: "",
    stderr: "",
    code: 1,
    killed: false,
  };
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const result = await execCommandTemplateOnce(command, args, options);
    if (result.code === 0) return result;
    lastResult = result;
    if (attempt < maxAttempts) await options.recover?.();
  }
  return lastResult;
}

const WINDOWS_COMMAND_META_CHARS = /([()\][%!^"`<>&|;, *?])/g;

function escapeWindowsCommand(value: string): string {
  return value.replace(WINDOWS_COMMAND_META_CHARS, "^$1");
}

function escapeWindowsCommandArgument(
  value: string,
  doubleEscapeMetaChars: boolean,
): string {
  let escaped = value
    .replace(/(?=(\\+?)?)\1"/g, '$1$1\\"')
    .replace(/(?=(\\+?)?)\1$/g, "$1$1");
  escaped = `"${escaped}"`.replace(WINDOWS_COMMAND_META_CHARS, "^$1");
  return doubleEscapeMetaChars
    ? escaped.replace(WINDOWS_COMMAND_META_CHARS, "^$1")
    : escaped;
}

function resolveCommandTemplateSpawn(
  command: string,
  args: string[],
): {
  command: string;
  args: string[];
  windowsVerbatimArguments?: boolean;
} {
  if (
    process.platform !== "win32" ||
    ![".bat", ".cmd"].includes(extname(command).toLowerCase())
  ) {
    return { command, args };
  }
  const normalizedCommand = normalize(command);
  const isNodeModulesShim =
    /[\\/]node_modules[\\/]\.bin[\\/][^\\/]+\.cmd$/iu.test(normalizedCommand);
  const shellCommand = [
    escapeWindowsCommand(normalizedCommand),
    ...args.map((arg) => escapeWindowsCommandArgument(arg, isNodeModulesShim)),
  ].join(" ");
  return {
    command: process.env.ComSpec ?? process.env.COMSPEC ?? "cmd.exe",
    args: ["/d", "/s", "/c", `"${shellCommand}"`],
    windowsVerbatimArguments: true,
  };
}

function execCommandTemplateOnce(
  command: string,
  args: string[],
  options: CommandTemplateExecOptions = {},
): Promise<CommandTemplateExecResult> {
  return new Promise((resolve) => {
    const invocation = resolveCommandTemplateSpawn(command, args);
    const proc = spawn(invocation.command, invocation.args, {
      cwd: options.cwd,
      shell: false,
      windowsVerbatimArguments: invocation.windowsVerbatimArguments,
      stdio: [options.stdin === undefined ? "ignore" : "pipe", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    let killed = false;
    let settled = false;
    let timeoutId: NodeJS.Timeout | undefined;
    let killTimeoutId: NodeJS.Timeout | undefined;
    const killProcess = (): void => {
      if (killed) return;
      killed = true;
      proc.kill("SIGTERM");
      killTimeoutId = setTimeout(() => {
        if (!settled) proc.kill("SIGKILL");
      }, options.killGrace ?? 5000);
    };
    const settle = (code: number): void => {
      if (settled) return;
      settled = true;
      if (timeoutId) clearTimeout(timeoutId);
      if (killTimeoutId) clearTimeout(killTimeoutId);
      if (options.signal)
        options.signal.removeEventListener("abort", killProcess);
      resolve({ stdout, stderr, code, killed });
    };
    if (options.signal) {
      if (options.signal.aborted) killProcess();
      else
        options.signal.addEventListener("abort", killProcess, { once: true });
    }
    if (options.timeout !== undefined && options.timeout > 0)
      timeoutId = setTimeout(killProcess, options.timeout);
    proc.stdout?.on("data", (data) => {
      stdout += data.toString();
    });
    proc.stderr?.on("data", (data) => {
      stderr += data.toString();
    });
    proc.stdin?.on("error", () => {});
    if (options.stdin !== undefined) proc.stdin?.end(options.stdin);
    proc.on("error", (error) => {
      stderr += error instanceof Error ? error.message : String(error);
      settle(1);
    });
    proc.on("close", (code) => {
      settle(code ?? (killed ? 1 : 0));
    });
  });
}

export function buildCommandTemplateInvocation(
  config: CommandTemplateConfig,
  values: Record<string, unknown>,
  cwd: string,
  options: { emptyMessage?: string; missingLabel?: string } = {},
): CommandTemplateInvocation {
  const normalizedConfig = normalizeCommandTemplateConfig(config);
  if (Array.isArray(normalizedConfig.template)) {
    throw new Error(
      options.emptyMessage ??
        "Command template sequence cannot be executed as one command",
    );
  }
  if (!normalizedConfig.template)
    throw new Error(options.emptyMessage ?? "Command template is required");
  if (typeof normalizedConfig.template !== "string") {
    throw new Error(
      options.emptyMessage ??
        "Command template object cannot be executed as one command",
    );
  }
  const parts = splitCommandTemplate(normalizedConfig.template);
  const commandPart = parts[0];
  if (!commandPart)
    throw new Error(options.emptyMessage ?? "Command template is empty");
  const resolvedValues = {
    ...getCommandTemplateDefaults(normalizedConfig),
    ...values,
  };
  const command = expandCommandTemplateExecutable(
    substituteCommandTemplateToken(
      commandPart,
      resolvedValues,
      options.missingLabel,
    ),
    cwd,
  );
  const args = parts
    .slice(1)
    .map((part) =>
      substituteCommandTemplateToken(
        part,
        resolvedValues,
        options.missingLabel,
      ),
    )
    .filter((part) => part !== "");
  return { command, args };
}
