/**
 * Telegram diagnostics logs
 * Zones: telegram diagnostics, filesystem, session observability
 * Owns bounded JSONL runtime evidence files, previous-log preservation, and profile-aware log paths without becoming routing state
 */
import { appendFileSync, copyFileSync, existsSync, mkdirSync, statSync, writeFileSync, } from "node:fs";
import { basename, dirname, join } from "node:path";
import { withTelegramFileTransaction } from "./locks.js";
import { resolveAgentDir, resolveTelegramPreviousSharedRuntimeLogPath, resolveTelegramProfileTempFilePath, resolveTelegramRuntimeLogPath, } from "./paths.js";
import * as Status from "./status.js";
const DEFAULT_MAX_LOG_BYTES = 5 * 1024 * 1024;
export function getTelegramRuntimeLogPath(agentDir = resolveAgentDir(), profileName) {
    return resolveTelegramProfileTempFilePath("logs", "jsonl", agentDir, profileName);
}
export function getTelegramPreviousRuntimeLogPath(agentDir = resolveAgentDir(), profileName) {
    return resolveTelegramProfileTempFilePath("logs", "_prev.jsonl", agentDir, profileName);
}
function safeJsonLine(value) {
    return JSON.stringify(value, (_key, item) => {
        if (item instanceof Error)
            return item.message;
        if (typeof item === "bigint")
            return item.toString();
        if (typeof item === "function" || typeof item === "symbol")
            return undefined;
        return item;
    });
}
export function createTelegramRuntimeJsonlLog(options = {}) {
    const shared = options.sharedProfiles
        ? { ...options.sharedProfiles }
        : undefined;
    const pathSource = options.path, previousSource = options.previousPath;
    const canReset = options.canReset, commitReset = options.commitReset;
    const resolvePath = () => typeof pathSource === "function"
        ? pathSource()
        : (pathSource ?? getTelegramRuntimeLogPath());
    const resolvePreviousPath = () => {
        if (typeof previousSource === "function")
            return previousSource();
        if (previousSource)
            return previousSource;
        return shared
            ? join(dirname(resolvePath()), "logs", `${basename(resolvePath(), ".jsonl")}._prev.jsonl`)
            : resolvePath().replace(/\.jsonl$/u, "._prev.jsonl");
    };
    const maxBytes = options.maxBytes ?? DEFAULT_MAX_LOG_BYTES;
    const getNowMs = options.getNowMs ?? Date.now;
    const scopeKeys = new Map();
    let pending = Promise.resolve();
    let appendScheduled = false;
    let queuedAppends = [];
    const getProfile = () => shared?.getProfileName() ?? "default";
    const transactionPath = (path) => shared
        ? join(dirname(path), "runtime", `${basename(path)}.transaction`)
        : `${path}.transaction`;
    const captureRotation = (path, previousPath, profile) => {
        if (!shared)
            return undefined;
        const authority = shared.captureAuthority();
        return () => authority?.() === true &&
            resolvePath() === path &&
            resolvePreviousPath() === previousPath &&
            getProfile() === profile;
    };
    const scopeStorageKey = (path, profile) => shared ? JSON.stringify([path, profile]) : path;
    const ensureParent = (path) => {
        mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
    };
    const preserveCurrentLog = (path, previousPath) => {
        if (!existsSync(path))
            return;
        mkdirSync(dirname(previousPath), { recursive: true, mode: 0o700 });
        copyFileSync(path, previousPath);
    };
    const writeResetLocked = (path, previousPath, reason, scope, profile, isCurrent) => {
        const line = safeJsonLine({
            at: getNowMs(),
            kind: "reset",
            reason,
            scope,
            previousPath,
            ...(shared ? { profile } : {}),
        }) + "\n";
        if (shared && !isCurrent?.())
            return false;
        ensureParent(path);
        preserveCurrentLog(path, previousPath);
        if (shared && !isCurrent?.())
            return false;
        writeFileSync(path, line, { mode: 0o600 });
        return true;
    };
    const writeReset = (reason, scope, path, profile) => {
        const previousPath = resolvePreviousPath();
        const isCurrent = captureRotation(path, previousPath, profile);
        if (canReset && !canReset())
            return false;
        let didReset = false;
        withTelegramFileTransaction(transactionPath(path), () => {
            const commit = () => {
                if (shared && !isCurrent?.())
                    return;
                if (shared) {
                    const line = safeJsonLine({
                        at: getNowMs(),
                        kind: "reset",
                        reason,
                        scope,
                        profile,
                    }) + "\n";
                    if (!isCurrent?.())
                        return;
                    ensureParent(path);
                    if (existsSync(path) &&
                        statSync(path).size + Buffer.byteLength(line) > maxBytes &&
                        !writeResetLocked(path, previousPath, "max-bytes", { maxBytes }, profile, isCurrent))
                        return;
                    if (!isCurrent?.())
                        return;
                    appendFileSync(path, line, { mode: 0o600 });
                    didReset = true;
                }
                else
                    didReset = writeResetLocked(path, previousPath, reason, scope);
            };
            if (commitReset) {
                commitReset(commit);
            }
            else {
                commit();
            }
        });
        return didReset && (!shared || isCurrent?.() === true);
    };
    const appendEvent = (event) => {
        const path = resolvePath(), previousPath = resolvePreviousPath(), profile = getProfile();
        const rotationCurrent = captureRotation(path, previousPath, profile);
        const line = safeJsonLine({
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
        if (appendScheduled)
            return;
        appendScheduled = true;
        pending = pending
            .then(() => {
            appendScheduled = false;
            const batch = queuedAppends;
            queuedAppends = [];
            const groups = new Map();
            for (const entry of batch) {
                const key = `${entry.path}\u0000${entry.previousPath}`;
                const group = groups.get(key);
                if (group)
                    group.entries.push(entry);
                else
                    groups.set(key, { ...entry, entries: [entry] });
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
                            if (!chunk)
                                return;
                            appendFileSync(group.path, chunk, { mode: 0o600 });
                            currentSize += chunkBytes;
                            chunk = "";
                            chunkBytes = 0;
                        };
                        const rotate = (entry) => {
                            if (canReset && !canReset())
                                return false;
                            if (shared && !entry.rotationCurrent?.())
                                return false;
                            let rotated = false;
                            const commit = () => {
                                if (shared && !entry.rotationCurrent?.())
                                    return;
                                rotated = writeResetLocked(group.path, group.previousPath, "max-bytes", { maxBytes }, entry.profile, entry.rotationCurrent);
                            };
                            if (commitReset)
                                commitReset(commit);
                            else
                                commit();
                            if (rotated)
                                currentSize = statSync(group.path).size;
                            return rotated;
                        };
                        for (const entry of group.entries) {
                            const line = entry.line;
                            const lineBytes = Buffer.byteLength(line);
                            if (currentSize + chunkBytes > 0 &&
                                currentSize + chunkBytes + lineBytes > maxBytes) {
                                flushChunk();
                                rotate(entry);
                            }
                            chunk += line;
                            chunkBytes += lineBytes;
                        }
                        flushChunk();
                    });
                }
                catch {
                    // Diagnostics failures for one profile must not drop other groups.
                }
            }
        })
            .catch(() => undefined);
    };
    return {
        getPath: resolvePath,
        reset(reason, scope) {
            const path = resolvePath(), profile = getProfile();
            try {
                if (writeReset(reason, scope, path, profile)) {
                    scopeKeys.set(scopeStorageKey(path, profile), scope ? safeJsonLine(scope) : undefined);
                }
            }
            catch {
                // Diagnostics must never break Telegram runtime behavior.
            }
        },
        resetIfScopeChanged(nextScopeKey, reason, scope) {
            const path = resolvePath(), profile = getProfile(), key = scopeStorageKey(path, profile);
            if (scopeKeys.get(key) === nextScopeKey)
                return;
            try {
                if (writeReset(reason, scope, path, profile))
                    scopeKeys.set(key, nextScopeKey);
            }
            catch {
                // Diagnostics must never break Telegram runtime behavior.
            }
        },
        record(event) {
            try {
                appendEvent(event);
            }
            catch {
                // Diagnostics must never break Telegram runtime behavior.
            }
        },
    };
}
export function createTelegramRuntimeDiagnosticsRuntime(options = {}) {
    const sharedFile = options.sharedFile === true;
    let getBotToken = () => undefined;
    let getProfileName = () => undefined;
    let canReset = () => false;
    let commitReset = (_commit) => false;
    let captureAuthority = () => undefined;
    let statusPorts;
    const events = Status.createTelegramRuntimeEventRecorder({
        getBotToken: () => getBotToken(),
    });
    const jsonl = createTelegramRuntimeJsonlLog({
        path: () => sharedFile
            ? resolveTelegramRuntimeLogPath()
            : getTelegramRuntimeLogPath(undefined, getProfileName()),
        previousPath: () => sharedFile
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
    const recordRuntimeEvent = function (category, error, details) {
        events.record(category, error, details);
        const latestEvent = events.getEvents().at(-1);
        if (latestEvent)
            jsonl.record(latestEvent);
        requestSnapshotPersist();
    };
    const captureSnapshotScope = () => {
        const ports = statusPorts;
        if (!ports)
            return undefined;
        if (!ports.session)
            return () => statusPorts === ports;
        const session = ports.session, ctx = session.get(), generation = session.getGeneration();
        if (ctx === undefined || !session.isCurrent(ctx, generation))
            return undefined;
        return () => statusPorts === ports && session.isCurrent(ctx, generation);
    };
    const requestSnapshotPersist = Status.createTelegramRuntimeDiagnosticsSnapshotScheduler({
        ...options.snapshotTimer,
        captureScope: captureSnapshotScope,
        async persistSnapshot(isCurrent) {
            const ports = statusPorts;
            // Projection may acquire journal guards or recover a snapshot: fence before reading it.
            if (!ports || !isCurrent())
                return;
            const snapshot = Status.createTelegramStatusSnapshot(ports.getStatusState());
            if (!isCurrent())
                return;
            await ports.persistSnapshot(snapshot);
        },
        recordError(error) {
            events.record("telegram", error, {
                phase: "runtime-diagnostics-snapshot-persist",
            });
        },
    });
    const updateRuntimeLogScope = function (reason) {
        if (!statusPorts)
            return;
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
            if (ports.session)
                void requestSnapshotPersist.suspend();
        },
        onSessionStart: requestSnapshotPersist.resume,
        onSessionShutdown: requestSnapshotPersist.suspend,
        updateStatus(ctx, error) {
            if (!statusPorts || statusPorts.session?.isCurrent(ctx) === false)
                return;
            statusPorts.updateStatus(ctx, error);
            updateRuntimeLogScope("status-scope-change");
        },
        getStatusLines(options) {
            if (!statusPorts)
                return [];
            requestSnapshotPersist();
            return Status.buildTelegramBridgeStatusLines(statusPorts.getStatusState(), options);
        },
        scheduleSnapshotPersist() {
            requestSnapshotPersist();
        },
    };
}
