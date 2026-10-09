/**
 * Telegram diagnostics logs
 * Zones: telegram diagnostics, filesystem, session observability
 * Owns bounded JSONL runtime evidence files, previous-log preservation, and profile-aware log paths without becoming routing state
 */

import {
  appendFileSync,
  copyFileSync,
  existsSync,
  mkdirSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { basename, dirname, join } from "node:path";

import { withTelegramFileTransaction } from "./locks.ts";
import {
  resolveAgentDir,
  resolveTelegramPreviousSharedRuntimeLogPath,
  resolveTelegramProfileTempFilePath,
  resolveTelegramRuntimeLogPath,
} from "./paths.ts";
import * as Status from "./status.ts";

type TelegramLogPathInput = string | (() => string);

interface TelegramRuntimeJsonlEvent {
  at: number;
  category: string;
  message: string;
  details?: Record<string, unknown>;
}

interface TelegramRuntimeJsonlLogOptions {
  path?: TelegramLogPathInput;
  previousPath?: TelegramLogPathInput;
  maxBytes?: number;
  getNowMs?: () => number;
  canReset?: () => boolean;
  commitReset?: (commit: () => void) => boolean;
  /** Explicit shared-file protocol; profile-labelled scope reset is an append, never truncation. */
  sharedProfiles?: {
    getProfileName: () => string | undefined;
    captureAuthority: () => (() => boolean) | undefined;
  };
}

interface TelegramRuntimeJsonlLog {
  getPath: () => string;
  reset: (reason: string, scope?: Record<string, unknown>) => void;
  resetIfScopeChanged: (
    scopeKey: string,
    reason: string,
    scope?: Record<string, unknown>,
  ) => void;
  record: (event: TelegramRuntimeJsonlEvent) => void;
}

const DEFAULT_MAX_LOG_BYTES = 5 * 1024 * 1024;

export function getTelegramRuntimeLogPath(
  agentDir = resolveAgentDir(),
  profileName?: string,
): string {
  return resolveTelegramProfileTempFilePath(
    "logs",
    "jsonl",
    agentDir,
    profileName,
  );
}

export function getTelegramPreviousRuntimeLogPath(
  agentDir = resolveAgentDir(),
  profileName?: string,
): string {
  return resolveTelegramProfileTempFilePath(
    "logs",
    "_prev.jsonl",
    agentDir,
    profileName,
  );
}

function safeJsonLine(value: unknown): string {
  return JSON.stringify(value, (_key, item) => {
    if (item instanceof Error) return item.message;
    if (typeof item === "bigint") return item.toString();
    if (typeof item === "function" || typeof item === "symbol")
      return undefined;
    return item;
  });
}

export function createTelegramRuntimeJsonlLog(
  options: TelegramRuntimeJsonlLogOptions = {},
): TelegramRuntimeJsonlLog {
  const shared = options.sharedProfiles
    ? { ...options.sharedProfiles }
    : undefined;
  const pathSource = options.path,
    previousSource = options.previousPath;
  const canReset = options.canReset,
    commitReset = options.commitReset;
  const resolvePath = () =>
    typeof pathSource === "function"
      ? pathSource()
      : (pathSource ?? getTelegramRuntimeLogPath());
  const resolvePreviousPath = () => {
    if (typeof previousSource === "function") return previousSource();
    if (previousSource) return previousSource;
    return shared
      ? join(
          dirname(resolvePath()),
          "logs",
          `${basename(resolvePath(), ".jsonl")}._prev.jsonl`,
        )
      : resolvePath().replace(/\.jsonl$/u, "._prev.jsonl");
  };
  const maxBytes = options.maxBytes ?? DEFAULT_MAX_LOG_BYTES;
  const getNowMs = options.getNowMs ?? Date.now;
  const scopeKeys = new Map<string, string | undefined>();
  let pending: Promise<void> = Promise.resolve();
  let appendScheduled = false;
  let queuedAppends: {
    path: string;
    previousPath: string;
    line: string;
    profile?: string;
    rotationCurrent?: () => boolean;
  }[] = [];
  const getProfile = () => shared?.getProfileName() ?? "default";
  const transactionPath = (path: string) =>
    shared
      ? join(dirname(path), "runtime", `${basename(path)}.transaction`)
      : `${path}.transaction`;
  const captureRotation = (
    path: string,
    previousPath: string,
    profile: string,
  ): (() => boolean) | undefined => {
    if (!shared) return undefined;
    const authority = shared.captureAuthority();
    return () =>
      authority?.() === true &&
      resolvePath() === path &&
      resolvePreviousPath() === previousPath &&
      getProfile() === profile;
  };
  const scopeStorageKey = (path: string, profile: string) =>
    shared ? JSON.stringify([path, profile]) : path;

  const ensureParent = (path: string) => {
    mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
  };

  const preserveCurrentLog = (path: string, previousPath: string) => {
    if (!existsSync(path)) return;
    mkdirSync(dirname(previousPath), { recursive: true, mode: 0o700 });
    copyFileSync(path, previousPath);
  };

  const writeResetLocked = (
    path: string,
    previousPath: string,
    reason: string,
    scope?: Record<string, unknown>,
    profile?: string,
    isCurrent?: () => boolean,
  ): boolean => {
    const line =
      safeJsonLine({
        at: getNowMs(),
        kind: "reset",
        reason,
        scope,
        previousPath,
        ...(shared ? { profile } : {}),
      }) + "\n";
    if (shared && !isCurrent?.()) return false;
    ensureParent(path);
    preserveCurrentLog(path, previousPath);
    if (shared && !isCurrent?.()) return false;
    writeFileSync(path, line, { mode: 0o600 });
    return true;
  };

  const writeReset = (
    reason: string,
    scope: Record<string, unknown> | undefined,
    path: string,
    profile: string,
  ): boolean => {
    const previousPath = resolvePreviousPath();
    const isCurrent = captureRotation(path, previousPath, profile);
    if (canReset && !canReset()) return false;
    let didReset = false;
    withTelegramFileTransaction(transactionPath(path), () => {
      const commit = () => {
        if (shared && !isCurrent?.()) return;
        if (shared) {
          const line =
            safeJsonLine({
              at: getNowMs(),
              kind: "reset",
              reason,
              scope,
              profile,
            }) + "\n";
          if (!isCurrent?.()) return;
          ensureParent(path);
          if (
            existsSync(path) &&
            statSync(path).size + Buffer.byteLength(line) > maxBytes &&
            !writeResetLocked(
              path,
              previousPath,
              "max-bytes",
              { maxBytes },
              profile,
              isCurrent,
            )
          )
            return;
          if (!isCurrent?.()) return;
          appendFileSync(path, line, { mode: 0o600 });
          didReset = true;
        } else didReset = writeResetLocked(path, previousPath, reason, scope);
      };
      if (commitReset) {
        commitReset(commit);
      } else {
        commit();
      }
    });
    return didReset && (!shared || isCurrent?.() === true);
  };

  const appendEvent = (event: TelegramRuntimeJsonlEvent) => {
    const path = resolvePath(),
      previousPath = resolvePreviousPath(),
      profile = getProfile();
    const rotationCurrent = captureRotation(path, previousPath, profile);
    const line =
      safeJsonLine({
        kind: "event",
        ...event,
        ...(shared ? { profile } : {}),
      }) + "\n";
    queuedAppends.push({
      path,
      previousPath,
      line,
      ...(shared ? { profile, rotationCurrent } : {}),
    });
    if (appendScheduled) return;
    appendScheduled = true;
    pending = pending
      .then(() => {
        appendScheduled = false;
        const batch = queuedAppends;
        queuedAppends = [];
        const groups = new Map<
          string,
          { path: string; previousPath: string; entries: typeof queuedAppends }
        >();
        for (const entry of batch) {
          const key = `${entry.path}\u0000${entry.previousPath}`;
          const group = groups.get(key);
          if (group) group.entries.push(entry);
          else groups.set(key, { ...entry, entries: [entry] });
        }
        for (const group of groups.values()) {
          try {
            ensureParent(group.path);
            withTelegramFileTransaction(transactionPath(group.path), () => {
              let currentSize = existsSync(group.path)
                ? statSync(group.path).size
                : 0;
              let chunk = "";
              let chunkBytes = 0;
              const flushChunk = () => {
                if (!chunk) return;
                appendFileSync(group.path, chunk, { mode: 0o600 });
                currentSize += chunkBytes;
                chunk = "";
                chunkBytes = 0;
              };
              const rotate = (
                entry: (typeof queuedAppends)[number],
              ): boolean => {
                if (canReset && !canReset()) return false;
                if (shared && !entry.rotationCurrent?.()) return false;
                let rotated = false;
                const commit = () => {
                  if (shared && !entry.rotationCurrent?.()) return;
                  rotated = writeResetLocked(
                    group.path,
                    group.previousPath,
                    "max-bytes",
                    { maxBytes },
                    entry.profile,
                    entry.rotationCurrent,
                  );
                };
                if (commitReset) commitReset(commit);
                else commit();
                if (rotated) currentSize = statSync(group.path).size;
                return rotated;
              };
              for (const entry of group.entries) {
                const line = entry.line;
                const lineBytes = Buffer.byteLength(line);
                if (
                  currentSize + chunkBytes > 0 &&
                  currentSize + chunkBytes + lineBytes > maxBytes
                ) {
                  flushChunk();
                  rotate(entry);
                }
                chunk += line;
                chunkBytes += lineBytes;
              }
              flushChunk();
            });
          } catch {
            // Diagnostics failures for one profile must not drop other groups.
          }
        }
      })
      .catch(() => undefined);
  };

  return {
    getPath: resolvePath,
    reset(reason, scope) {
      const path = resolvePath(),
        profile = getProfile();
      try {
        if (writeReset(reason, scope, path, profile)) {
          scopeKeys.set(
            scopeStorageKey(path, profile),
            scope ? safeJsonLine(scope) : undefined,
          );
        }
      } catch {
        // Diagnostics must never break Telegram runtime behavior.
      }
    },
    resetIfScopeChanged(nextScopeKey, reason, scope) {
      const path = resolvePath(),
        profile = getProfile(),
        key = scopeStorageKey(path, profile);
      if (scopeKeys.get(key) === nextScopeKey) return;
      try {
        if (writeReset(reason, scope, path, profile))
          scopeKeys.set(key, nextScopeKey);
      } catch {
        // Diagnostics must never break Telegram runtime behavior.
      }
    },
    record(event) {
      try {
        appendEvent(event);
      } catch {
        // Diagnostics must never break Telegram runtime behavior.
      }
    },
  };
}

interface TelegramRuntimeDiagnosticsStatusPorts<TContext> {
  instanceId: string;
  updateStatus(ctx: TContext, error?: string): void;
  getStatusState(): Status.TelegramBridgeStatusLineState;
  persistSnapshot(
    snapshot: ReturnType<typeof Status.createTelegramStatusSnapshot>,
  ): Promise<void>;
  session?: {
    get(): TContext | undefined;
    getGeneration(): number;
    isCurrent(ctx: TContext, generation?: number): boolean;
  };
}

interface TelegramRuntimeDiagnosticsRuntime<TContext> {
  events: Status.TelegramRuntimeEventRecorder;
  recordRuntimeEvent(
    category: string,
    error: unknown,
    details?: Record<string, unknown>,
  ): void;
  bindStorage(ports: {
    getBotToken(): string | undefined;
    getProfileName(): string | undefined;
    canReset(): boolean;
    commitReset(commit: () => void): boolean;
    captureAuthority?: () => (() => boolean) | undefined;
  }): void;
  bindStatus(ports: TelegramRuntimeDiagnosticsStatusPorts<TContext>): void;
  onSessionStart(): void;
  onSessionShutdown(): Promise<void>;
  updateStatus(ctx: TContext, error?: string): void;
  getStatusLines(options?: Status.TelegramBridgeStatusLineOptions): string[];
  scheduleSnapshotPersist(): void;
}

export function createTelegramRuntimeDiagnosticsRuntime<TContext>(
  options: {
    sharedFile?: boolean;
    snapshotTimer?: Pick<
      Parameters<
        typeof Status.createTelegramRuntimeDiagnosticsSnapshotScheduler
      >[0],
      "setTimer" | "clearTimer"
    >;
  } = {},
): TelegramRuntimeDiagnosticsRuntime<TContext> {
  const sharedFile = options.sharedFile === true;
  let getBotToken = (): string | undefined => undefined;
  let getProfileName = (): string | undefined => undefined;
  let canReset = (): boolean => false;
  let commitReset = (_commit: () => void): boolean => false;
  let captureAuthority = (): (() => boolean) | undefined => undefined;
  let statusPorts: TelegramRuntimeDiagnosticsStatusPorts<TContext> | undefined;
  const events = Status.createTelegramRuntimeEventRecorder({
    getBotToken: () => getBotToken(),
  });
  const jsonl = createTelegramRuntimeJsonlLog({
    path: () =>
      sharedFile
        ? resolveTelegramRuntimeLogPath()
        : getTelegramRuntimeLogPath(undefined, getProfileName()),
    previousPath: () =>
      sharedFile
        ? resolveTelegramPreviousSharedRuntimeLogPath()
        : getTelegramPreviousRuntimeLogPath(undefined, getProfileName()),
    ...(sharedFile
      ? {
          sharedProfiles: {
            getProfileName: () => getProfileName(),
            captureAuthority: () => captureAuthority(),
          },
        }
      : {}),
    canReset: () => canReset(),
    commitReset: (commit) => commitReset(commit),
  });
  const recordRuntimeEvent = function (
    category: string,
    error: unknown,
    details?: Record<string, unknown>,
  ): void {
    events.record(category, error, details);
    const latestEvent = events.getEvents().at(-1);
    if (latestEvent) jsonl.record(latestEvent);
    requestSnapshotPersist();
  };
  const captureSnapshotScope = (): (() => boolean) | undefined => {
    const ports = statusPorts;
    if (!ports) return undefined;
    if (!ports.session) return () => statusPorts === ports;
    const session = ports.session,
      ctx = session.get(),
      generation = session.getGeneration();
    if (ctx === undefined || !session.isCurrent(ctx, generation))
      return undefined;
    return () => statusPorts === ports && session.isCurrent(ctx, generation);
  };
  const requestSnapshotPersist =
    Status.createTelegramRuntimeDiagnosticsSnapshotScheduler({
      ...options.snapshotTimer,
      captureScope: captureSnapshotScope,
      async persistSnapshot(isCurrent) {
        const ports = statusPorts;
        // Projection may acquire journal guards or recover a snapshot: fence before reading it.
        if (!ports || !isCurrent()) return;
        const snapshot = Status.createTelegramStatusSnapshot(
          ports.getStatusState(),
        );
        if (!isCurrent()) return;
        await ports.persistSnapshot(snapshot);
      },
      recordError(error) {
        events.record("telegram", error, {
          phase: "runtime-diagnostics-snapshot-persist",
        });
      },
    });
  const updateRuntimeLogScope = function (reason: string): void {
    if (!statusPorts) return;
    const scope = Status.createTelegramRuntimeLogScope({
      state: statusPorts.getStatusState(),
      instanceId: statusPorts.instanceId,
    });
    jsonl.resetIfScopeChanged(JSON.stringify(scope), reason, scope);
  };
  return {
    events,
    recordRuntimeEvent,
    bindStorage(ports) {
      getBotToken = ports.getBotToken;
      getProfileName = ports.getProfileName;
      canReset = ports.canReset;
      commitReset = ports.commitReset;
      captureAuthority = ports.captureAuthority ?? (() => undefined);
    },
    bindStatus(ports) {
      statusPorts = ports;
      if (ports.session) void requestSnapshotPersist.suspend();
    },
    onSessionStart: requestSnapshotPersist.resume,
    onSessionShutdown: requestSnapshotPersist.suspend,
    updateStatus(ctx, error) {
      if (!statusPorts || statusPorts.session?.isCurrent(ctx) === false) return;
      statusPorts.updateStatus(ctx, error);
      updateRuntimeLogScope("status-scope-change");
    },
    getStatusLines(options) {
      if (!statusPorts) return [];
      requestSnapshotPersist();
      return Status.buildTelegramBridgeStatusLines(
        statusPorts.getStatusState(),
        options,
      );
    },
    scheduleSnapshotPersist() {
      requestSnapshotPersist();
    },
  };
}
