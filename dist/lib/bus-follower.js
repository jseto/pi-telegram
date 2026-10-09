/**
 * Telegram bus follower runtime
 * Zones: multi-instance bus, follower lifecycle, manual registration
 * Owns this Pi instance's follower-side bus behavior: manual registration,
 * heartbeat, forwarded-update receiving, and follower-routed API calls.
 * It must not spawn Pi processes or create hidden Telegram-originated instances.
 */
import { createHash } from "node:crypto";
import { basename } from "node:path";
import { isDeepStrictEqual } from "node:util";
import { getTelegramBusTransportRetryPolicy, TELEGRAM_BUS_REGISTRATION_RETRY, } from "./bus-transport.js";
import { createTelegramBusFollowerDeliveryIdentity, createTelegramBusForeignOwnedUpdateForwarder, createTelegramBusLocalServer, createTelegramBusRequestIdFactory, createUnauthorizedBusAck, rejectTelegramBusRequest, getTelegramBusProtocolCompatibility, getTelegramBusSocketPath, hasTelegramBusCapability, isTelegramBusEnvelopeAuthorized, parseTelegramBusEnvelope, resolveTelegramBusSocketPath, sendTelegramBusLocalEnvelope, TELEGRAM_BUS_CAPABILITY_DIRECTORY_DISPLAY_FORMAT, TELEGRAM_BUS_CAPABILITY_LIVE_REBIND_APPLY, TELEGRAM_BUS_CAPABILITY_LIVE_REBIND_COMMAND_SET, TELEGRAM_BUS_CAPABILITY_LIVE_REBIND_SAVE, TELEGRAM_BUS_CAPABILITY_LIVE_REBIND_SETTLE, TELEGRAM_BUS_CAPABILITY_SELECTED_MENU_DELIVERY, TELEGRAM_BUS_CAPABILITY_SESSION_REPLACEMENT_INTENT, TELEGRAM_BUS_CAPABILITY_THREAD_DISPLAY_MODE, TELEGRAM_BUS_CAPABILITY_WORKSPACE_RESTORE, TELEGRAM_BUS_CAPABILITY_WORKSPACE_THREAD_RENAME, TELEGRAM_BUS_HELD_COMMAND_CAPABILITIES, TELEGRAM_BUS_LIVE_REBIND_CAPABILITIES, TelegramBusLocalAuthorityError, } from "./bus.js";
import { parseTelegramUpdateJournalQueueOwner, } from "./journal.js";
import { TELEGRAM_BUS_LEADER_STALE_HEARTBEAT_MS, } from "./locks.js";
import { isPiStaleContextError } from "./pi.js";
import { parseTelegramTarget as parseTarget, } from "./target.js";
import { isTelegramApiMethodRetrySafe, TelegramApiAuthorityError, TelegramApiCommitUnknownError, TelegramApiStaleTargetError, } from "./telegram-api.js";
import * as Threads from "./threads.js";
import { isWireRecord as isRecord } from "./wire.js";
import { createTelegramWorkspaceAdmissionOperationId, runWithTelegramWorkspaceAdmissionsAsync, } from "./workspace-admission.js";
import * as WorkspaceIdentity from "./workspace-identity.js";
const TELEGRAM_BUS_FOLLOWER_PROMOTION_GRACE_MS = 2_500;
const TELEGRAM_FOLLOWER_SESSION_HANDOFF_TTL_MS = 30_000;
const TELEGRAM_BUS_FOLLOWER_CLIENT_TIMEOUT_MS = 30_000;
const TELEGRAM_BUS_FOLLOWER_REGISTRATION_WAIT_MS = 30_000;
export const TELEGRAM_BUS_FOLLOWER_HEARTBEAT_TIMEOUT_MS = TELEGRAM_BUS_LEADER_STALE_HEARTBEAT_MS;
const TELEGRAM_BUS_FOLLOWER_REGISTRATION_RETRY_ATTEMPTS = TELEGRAM_BUS_REGISTRATION_RETRY.attempts;
const TELEGRAM_BUS_FOLLOWER_REGISTRATION_RETRY_DELAY_MS = TELEGRAM_BUS_REGISTRATION_RETRY.delayMs;
const TELEGRAM_FOLLOWER_SESSION_HANDOFF_KEY = "__piTelegramFollowerSessionHandoff";
export function getTelegramFollowerSessionHandoff() {
    const value = globalThis[TELEGRAM_FOLLOWER_SESSION_HANDOFF_KEY];
    if (!value || typeof value !== "object")
        return undefined;
    const handoff = value;
    if (typeof handoff.pid !== "number" ||
        typeof handoff.instanceId !== "string" ||
        typeof handoff.createdAtMs !== "number" ||
        !handoff.target ||
        typeof handoff.target !== "object" ||
        typeof handoff.target.chatId !== "number") {
        return undefined;
    }
    return handoff;
}
export function setTelegramFollowerSessionHandoff(handoff) {
    const store = globalThis;
    if (!handoff)
        delete store[TELEGRAM_FOLLOWER_SESSION_HANDOFF_KEY];
    else
        store[TELEGRAM_FOLLOWER_SESSION_HANDOFF_KEY] = handoff;
}
function isTelegramFollowerSessionHandoffFresh(handoff, options = {}) {
    if (!handoff)
        return false;
    const pid = options.pid ?? process.pid;
    const nowMs = options.nowMs ?? Date.now();
    const ttlMs = options.ttlMs ?? TELEGRAM_FOLLOWER_SESSION_HANDOFF_TTL_MS;
    return handoff.pid === pid && nowMs - handoff.createdAtMs <= ttlMs;
}
export function createTelegramManualFollowerProfileKeyResolver(input) {
    return () => Threads.getTelegramThreadOwnerKey({
        kind: "manual-follower",
        instanceId: input.manualFollowerOwnerId,
        telegramProfile: input.getActiveProfileName(),
    });
}
function runTelegramBusFollowerWorkspaceMutation(deps, operationKind, operation) {
    if (!deps.getWorkspaceAdmission)
        return operation();
    const admission = deps.getWorkspaceAdmission();
    if (!admission) {
        throw new Error("Telegram Workspace admission authority is unavailable.");
    }
    return runWithTelegramWorkspaceAdmissionsAsync({
        ledger: admission,
        operationId: createTelegramWorkspaceAdmissionOperationId(),
        operationKind,
        scopes: [{ kind: "profile" }],
        operation,
        onReleaseError(error) {
            deps.recordRuntimeEvent?.("bus", error, {
                phase: "workspace-admission-release",
                operationKind,
            });
        },
    });
}
export function createTelegramBusFollowerPromotionHandler(input) {
    return (ctx, binding, election) => runTelegramBusFollowerWorkspaceMutation(input, "workspace.promote-follower", async () => {
        let promotedRecord;
        const promoted = await input.startLeader(ctx, election, async () => {
            promotedRecord = await Threads.promoteTelegramFollowerBindingToLeader({
                store: input.topicTargetStore,
                instanceId: input.instanceId,
                cwd: ctx.cwd,
                sessionId: input.getSessionId?.(ctx),
                telegramProfile: input.getActiveProfileName(),
                target: binding.target,
                slot: binding.slot,
                threadName: binding.threadName,
            });
            if (!promotedRecord && typeof binding.target?.threadId === "number") {
                throw new Error("Telegram follower promotion slot authority is unavailable.");
            }
            if (promotedRecord) {
                input.recordRuntimeEvent("bus", "Follower thread binding promoted to leader", {
                    phase: "follower-promoted-binding",
                    chatId: promotedRecord.target.chatId,
                    threadId: promotedRecord.target.threadId,
                    slot: promotedRecord.slot,
                    threadName: promotedRecord.threadName,
                });
            }
        });
        if (promoted &&
            promotedRecord &&
            typeof binding.target?.threadId === "number") {
            const profileKey = Threads.getTelegramThreadOwnerKey({
                kind: "leader",
                cwd: ctx.cwd,
                instanceId: input.instanceId,
                telegramProfile: input.getActiveProfileName(),
            });
            Threads.setTelegramLeaderSessionHandoff({
                pid: input.getPid?.() ?? process.pid,
                instanceId: input.instanceId,
                createdAtMs: input.getNowMs?.() ?? Date.now(),
                profileKey,
                target: {
                    chatId: binding.target.chatId,
                    threadId: binding.target.threadId,
                },
                slot: promotedRecord.slot,
                threadName: promotedRecord.threadName,
            });
            input.recordRuntimeEvent("bus", "Promoted leader binding retained for session replacement", {
                phase: "follower-promoted-session-handoff",
                chatId: binding.target.chatId,
                threadId: binding.target.threadId,
                slot: promotedRecord.slot,
                threadName: promotedRecord.threadName,
            });
        }
        return promoted;
    });
}
export function createTelegramBusFollowerRuntimeAssembly(ports) {
    // Binding startup needs registration identity before it can grant inbound authority.
    let readyContext;
    const isReadyContext = (ctx) => Boolean(readyContext &&
        (ctx === undefined || readyContext.ctx === ctx) &&
        ports.registrationState.getGeneration() === readyContext.generation &&
        ports.registration.getSessionGeneration?.() ===
            readyContext.sessionGeneration &&
        ports.registration.getSessionId?.(readyContext.ctx) ===
            readyContext.sessionId &&
        ports.registration.isContextActive?.(readyContext.ctx) !== false);
    const prepareContext = async (ctx) => {
        const generation = ports.registrationState.getGeneration();
        const sessionGeneration = ports.registration.getSessionGeneration?.();
        const sessionId = ports.registration.getSessionId?.(ctx);
        readyContext = undefined;
        if (!generation ||
            (sessionId !== undefined &&
                ports.registrationState.getSessionId(sessionId) !== sessionId))
            throw new Error("Telegram follower session change requires acknowledged leader registration.");
        await ports.registration.onRegistered?.(ctx);
        if (generation &&
            ports.registrationState.getGeneration() === generation &&
            ports.registration.getSessionGeneration?.() === sessionGeneration &&
            ports.registration.getSessionId?.(ctx) === sessionId &&
            ports.registration.isContextActive?.(ctx) !== false)
            readyContext = { generation, ctx, sessionGeneration, sessionId };
    };
    const sharedRuntimeDeps = {
        instanceId: ports.instanceId,
        registrationState: ports.registrationState,
        recordRuntimeEvent: ports.recordRuntimeEvent,
    };
    const receiver = createTelegramBusForwardedUpdateReceiverRuntime({
        ...ports.receiver,
        instanceId: ports.instanceId,
        recordRuntimeEvent: ports.recordRuntimeEvent,
        getRegistrationGeneration() {
            return isReadyContext() ? readyContext?.generation : undefined;
        },
    });
    let registration;
    const recovery = createTelegramBusFollowerHeartbeatRecoveryHandler({
        ...ports.recovery,
        registrationState: ports.registrationState,
        recordRuntimeEvent: ports.recordRuntimeEvent,
        getRegistrationRuntime: () => registration,
    });
    registration = createTelegramBusFollowerRegistrationRuntime({
        ...ports.registration,
        ...sharedRuntimeDeps,
        startReceiving: receiver.start,
        stopReceiving: receiver.stop,
        onRegistered: prepareContext,
        onHeartbeatFailure: recovery,
    });
    const baseRegistration = registration;
    registration = {
        ...baseRegistration,
        async setContext(ctx) {
            const sessionGeneration = ports.registration.getSessionGeneration?.();
            if (ports.registration.isContextActive?.(ctx) === false)
                return;
            await baseRegistration.setContext(ctx);
            if (ports.registration.getSessionGeneration?.() !== sessionGeneration ||
                ports.registration.isContextActive?.(ctx) === false)
                return;
            if (!isReadyContext(ctx))
                await prepareContext(ctx);
        },
    };
    return {
        receiver,
        registration,
        getReadySessionId: () => isReadyContext() ? readyContext?.sessionId : undefined,
    };
}
/** Binds Restore to real Pi lifetime and the current authenticated transport owner, not request fields. */
export function createTelegramBusFollowerRestoreContextGetter(deps) {
    return (ctx) => {
        if (!deps.isContextCurrent(ctx))
            return undefined;
        const leader = deps.getLeaderState();
        const secret = deps.getAuthenticatedSecret();
        const leaderProtocol = deps.getLeaderProtocol();
        if (!leaderProtocol ||
            !hasTelegramBusCapability(leaderProtocol, deps.capability ?? TELEGRAM_BUS_CAPABILITY_WORKSPACE_RESTORE))
            return undefined;
        if (leader.kind !== "active-elsewhere" ||
            !secret ||
            leader.lock.busSecret !== secret ||
            !leader.lock.instanceId ||
            leader.lock.leaderEpoch === undefined)
            return undefined;
        const profileBindingKey = deps.getProfileBindingKey();
        const operatorUserId = deps.getOperatorUserId();
        const sessionId = deps.getSessionId(ctx);
        const cwd = deps.getCwd(ctx);
        const generation = deps.getGeneration();
        if (!profileBindingKey ||
            !sessionId ||
            !cwd ||
            typeof operatorUserId !== "number" ||
            !Number.isSafeInteger(operatorUserId) ||
            operatorUserId <= 0 ||
            !Number.isSafeInteger(generation) ||
            generation < 0 ||
            !deps.isContextCurrent(ctx))
            return undefined;
        return {
            executor: {
                instanceId: leader.lock.instanceId,
                leaderEpoch: String(leader.lock.leaderEpoch),
            },
            profileBindingKey,
            operatorUserId,
            sessionId,
            cwd,
            generation,
            leaderProtocol: structuredClone(leaderProtocol),
        };
    };
}
/** Shared canonical/local target owner and scoped live-carrier hooks; caller authenticates transport, no canonical writes or cleanup. */
export function createTelegramBusFollowerWorkspaceRestoreHandler(deps) {
    const handler = async (input, ctx) => {
        const scope = input.liveRebind, ports = scope && {
            isCurrent: scope.isCurrent,
            release: scope.release,
            observeWork: scope.observeWork,
        };
        const suppliedTarget = scope?.expectedTarget, expectedTarget = suppliedTarget && { ...suppliedTarget };
        const sourceUpdateIds = scope && [...scope.sourceUpdateIds].sort((a, b) => a - b);
        const liveCurrent = scope && ports.isCurrent.bind(scope), release = scope && ports.release?.bind(scope), workObserver = scope && ports.observeWork?.bind(scope);
        const captured = deps.getContextAuthority(ctx);
        if (!captured)
            throw new Error("Stale Telegram follower Restore context.");
        const authority = structuredClone(captured);
        const request = structuredClone({
            operationId: input.operationId,
            registrationGeneration: input.registrationGeneration,
            mode: input.mode,
        });
        const readIntent = () => liveCurrent
            ? deps.readLiveRebindIntent?.(request.operationId, authority.profileBindingKey)
            : deps.readRestoreIntent(request.operationId, authority.profileBindingKey);
        let sampleWork;
        if (suppliedTarget !== undefined && !expectedTarget)
            throw new Error("Invalid prepared live-rebind target.");
        if (expectedTarget &&
            (!Number.isSafeInteger(expectedTarget.chatId) ||
                expectedTarget.chatId !== authority.operatorUserId ||
                !Number.isSafeInteger(expectedTarget.threadId) ||
                expectedTarget.threadId <= 0))
            throw new Error("Invalid prepared live-rebind target.");
        if ((release || workObserver) &&
            !hasTelegramBusCapability(authority.leaderProtocol, TELEGRAM_BUS_CAPABILITY_LIVE_REBIND_SETTLE))
            throw new Error("Live-rebind settlement capability is unavailable.");
        if (liveCurrent &&
            !hasTelegramBusCapability(authority.leaderProtocol, TELEGRAM_BUS_CAPABILITY_LIVE_REBIND_APPLY))
            throw new Error("Live-rebind apply capability is unavailable.");
        if (!authority.profileBindingKey ||
            !authority.sessionId ||
            !authority.cwd ||
            !authority.executor?.instanceId ||
            !authority.executor.leaderEpoch ||
            !(typeof authority.generation === "string"
                ? authority.generation.length > 0
                : Number.isSafeInteger(authority.generation) &&
                    authority.generation >= 0) ||
            !Number.isSafeInteger(authority.operatorUserId) ||
            authority.operatorUserId <= 0 ||
            !request.operationId ||
            !request.registrationGeneration ||
            !["apply", "inspect"].includes(request.mode))
            throw new Error("Invalid Telegram follower Restore authority.");
        const assertCurrent = () => {
            if (!isDeepStrictEqual(deps.getContextAuthority(ctx), authority) ||
                liveCurrent?.() === false ||
                (scope &&
                    (input.liveRebind !== scope ||
                        Object.entries(ports).some(([key, value]) => Reflect.get(scope, key) !== value))) ||
                deps.registrationState.getGeneration() !==
                    request.registrationGeneration)
                throw new Error("Stale Telegram follower Restore authority.");
        };
        assertCurrent();
        if (!deps.getWorkspaceAdmission)
            throw new Error("Telegram Workspace admission authority is unavailable.");
        const result = await runTelegramBusFollowerWorkspaceMutation(deps, "workspace.restore-follower-target", async () => {
            assertCurrent();
            const intent = readIntent();
            assertCurrent();
            if (!intent ||
                intent.request.operationId !== request.operationId ||
                (liveCurrent
                    ? !("kind" in intent) ||
                        intent.kind !== "live-rebind" ||
                        !isDeepStrictEqual([...intent.request.source.updateIds].sort((a, b) => a - b), sourceUpdateIds) ||
                        (request.mode === "apply"
                            ? intent.phase !== "rebound"
                            : !["rebound", "released", "finished"].includes(intent.phase))
                    : "kind" in intent ||
                        (intent.phase !== "recipient-issued" &&
                            intent.phase !== "ready")) ||
                (expectedTarget &&
                    !isDeepStrictEqual(intent.request.target, expectedTarget)) ||
                !isDeepStrictEqual(intent.executor, authority.executor) ||
                intent.operatorUserId !== authority.operatorUserId ||
                !intent.recipient ||
                ((request.mode === "apply" || liveCurrent) &&
                    (intent.recipient.kind !== "follower" ||
                        intent.recipient.instanceId !== deps.instanceId ||
                        intent.recipient.generation !==
                            request.registrationGeneration)) ||
                intent.recipient.sessionId !== authority.sessionId ||
                intent.request.binding.sessionId !== authority.sessionId ||
                intent.request.binding.cwd !==
                    WorkspaceIdentity.normalizeTelegramWorkspacePath(authority.cwd))
                throw new Error("Telegram follower Restore intent does not match this recipient.");
            const expected = structuredClone(intent);
            await deps.topicTargetStore.load();
            assertCurrent();
            const observe = (canonical) => {
                if (!isDeepStrictEqual(readIntent(), expected))
                    throw new Error("Telegram follower Restore intent changed.");
                assertCurrent();
                const { binding, target } = expected.request;
                const bindings = (canonical.workspaceBindings ?? []).filter((value) => value.bindingKey === binding.bindingKey);
                const snapshot = canonical.threads;
                const records = snapshot.filter((value) => value.instanceId === deps.instanceId);
                const owners = snapshot.filter((value) => value.status === "active" &&
                    (value.slot === binding.slot ||
                        isDeepStrictEqual(value.target, target)));
                if (bindings.length !== 1 ||
                    records.length !== 1 ||
                    owners.length !== 1 ||
                    bindings[0]?.cwd !== binding.cwd ||
                    bindings[0]?.sessionId !== binding.sessionId ||
                    bindings[0]?.slot !== binding.slot ||
                    bindings[0]?.inactiveSinceMs !== undefined ||
                    !isDeepStrictEqual(bindings[0]?.target, target) ||
                    records[0]?.status !== "active" ||
                    records[0]?.owner?.kind !== "manual-follower" ||
                    records[0]?.slot !== binding.slot ||
                    !isDeepStrictEqual(records[0]?.target, target) ||
                    deps.registrationState.getSlot() !== binding.slot)
                    throw new Error("Telegram follower Restore canonical binding is not committed.");
                const localTarget = deps.registrationState.getTarget();
                const alreadyReady = isDeepStrictEqual(localTarget, target);
                if (!alreadyReady && !isDeepStrictEqual(localTarget, binding.target))
                    throw new Error("Telegram follower Restore local target changed.");
                assertCurrent();
                if (!alreadyReady && request.mode === "apply") {
                    if (expected.phase !== (liveCurrent ? "rebound" : "recipient-issued"))
                        throw new Error("Telegram follower Restore readiness regressed.");
                    deps.registrationState.setRegistered(true, target, {
                        slot: binding.slot,
                        threadName: records[0]?.threadName,
                        generation: request.registrationGeneration,
                        leaderProtocol: deps.registrationState.getLeaderProtocol(),
                    });
                }
                assertCurrent();
                const observedTarget = deps.registrationState.getTarget();
                if (!observedTarget ||
                    (!isDeepStrictEqual(observedTarget, target) &&
                        !isDeepStrictEqual(observedTarget, binding.target)))
                    throw new Error("Telegram follower Restore local target changed.");
                const ready = isDeepStrictEqual(observedTarget, target);
                assertCurrent();
                return {
                    operationId: request.operationId,
                    recipient: {
                        kind: "follower",
                        instanceId: deps.instanceId,
                        sessionId: authority.sessionId,
                        generation: request.registrationGeneration,
                    },
                    target: {
                        chatId: observedTarget.chatId,
                        threadId: observedTarget.threadId,
                    },
                    slot: binding.slot,
                    ready,
                };
            };
            let result;
            deps.topicTargetStore.withWorkspaceRestoreSnapshot(expected, (snapshot) => {
                result = observe(snapshot);
            });
            if (!result)
                throw new Error("Telegram follower Restore observation was not completed.");
            if (release || workObserver) {
                if (!("kind" in expected) ||
                    expected.kind !== "live-rebind" ||
                    expected.phase !== "released" ||
                    !result.ready ||
                    request.mode !== "inspect")
                    throw new Error("Live-rebind dispatch has no canonical released/local-applied grant.");
                // Journal reads stay outside the canonical transaction; each barrier check reobserves only Workspace authority.
                if (workObserver)
                    sampleWork = () => {
                        deps.topicTargetStore.withWorkspaceRestoreSnapshot(expected, (snapshot) => {
                            if (!observe(snapshot).ready)
                                throw new Error("Live-rebind work observation lost local target application.");
                            workObserver(expected.request.binding.target);
                            assertCurrent();
                        });
                    };
                release?.(() => {
                    assertCurrent();
                    let ready = false;
                    deps.topicTargetStore.withWorkspaceRestoreSnapshot(expected, (snapshot) => {
                        ready = observe(snapshot).ready;
                    });
                    assertCurrent();
                    return ready;
                });
            }
            return result;
        });
        assertCurrent();
        if (!isDeepStrictEqual(deps.registrationState.getTarget(), result.target) ||
            deps.registrationState.getSlot() !== result.slot)
            throw new Error("Telegram follower Restore local registration changed after admission release.");
        // Work can change while admission releases; sample only after that await under a fresh canonical snapshot.
        sampleWork?.();
        assertCurrent();
        return result;
    };
    return Object.assign(handler, {
        /** Capture availability only; asserting current recipient requires canonical released/local-applied ownership. */
        prepareLiveCommandRecipient(input, ctx) {
            const expected = structuredClone(input), getAuthority = deps.getContextAuthority, read = deps.readLiveRebindIntent, store = deps.topicTargetStore;
            const snapshot = store.withWorkspaceLiveRebindSnapshot, state = deps.registrationState;
            const statePorts = {
                getTarget: state.getTarget,
                getSlot: state.getSlot,
                getGeneration: state.getGeneration,
                getLeaderProtocol: state.getLeaderProtocol,
            };
            const instanceId = deps.instanceId, authority = structuredClone(getAuthority.call(deps, ctx)), oldTarget = structuredClone(state.getTarget()), slot = state.getSlot();
            if (!authority ||
                !read ||
                !snapshot ||
                !instanceId ||
                !slot ||
                !expected.operationId?.trim() ||
                !expected.registrationGeneration?.trim() ||
                expected.sessionId !== authority.sessionId ||
                expected.sourceUpdateIds.length !== 1 ||
                !Number.isSafeInteger(expected.sourceUpdateIds[0]) ||
                expected.sourceUpdateIds[0] < 0 ||
                typeof authority.generation !== "number" ||
                !Number.isSafeInteger(authority.generation) ||
                authority.generation < 0 ||
                !authority.profileBindingKey?.trim() ||
                !authority.cwd?.trim() ||
                !authority.executor?.instanceId?.trim() ||
                !authority.executor.leaderEpoch?.trim() ||
                !Number.isSafeInteger(authority.operatorUserId) ||
                authority.operatorUserId <= 0 ||
                expected.target.chatId !== authority.operatorUserId ||
                !Number.isSafeInteger(expected.target.threadId) ||
                expected.target.threadId <= 0 ||
                !oldTarget ||
                oldTarget.chatId !== expected.target.chatId ||
                !Number.isSafeInteger(oldTarget.threadId) ||
                oldTarget.threadId <= 0)
                return undefined;
            const portsCurrent = () => deps.instanceId === instanceId &&
                deps.getContextAuthority === getAuthority &&
                deps.readLiveRebindIntent === read &&
                deps.topicTargetStore === store &&
                store.withWorkspaceLiveRebindSnapshot === snapshot &&
                deps.registrationState === state &&
                Object.entries(statePorts).every(([key, value]) => Reflect.get(state, key) === value);
            const isCurrent = () => {
                try {
                    if (!portsCurrent() ||
                        !isDeepStrictEqual(getAuthority.call(deps, ctx), authority) ||
                        state.getGeneration() !== expected.registrationGeneration ||
                        state.getSlot() !== slot ||
                        !isDeepStrictEqual(state.getLeaderProtocol(), authority.leaderProtocol) ||
                        ![
                            ...TELEGRAM_BUS_LIVE_REBIND_CAPABILITIES,
                            ...TELEGRAM_BUS_HELD_COMMAND_CAPABILITIES,
                        ].every((cap) => hasTelegramBusCapability(authority.leaderProtocol, cap)) ||
                        ![oldTarget, expected.target].some((target) => isDeepStrictEqual(state.getTarget(), target)))
                        return false;
                    return (portsCurrent() &&
                        isDeepStrictEqual(getAuthority.call(deps, ctx), authority) &&
                        state.getGeneration() === expected.registrationGeneration &&
                        state.getSlot() === slot &&
                        isDeepStrictEqual(state.getLeaderProtocol(), authority.leaderProtocol) &&
                        [oldTarget, expected.target].some((target) => isDeepStrictEqual(state.getTarget(), target)) &&
                        portsCurrent());
                }
                catch {
                    return false;
                }
            };
            if (!isCurrent())
                return undefined;
            return {
                isCurrent,
                assertRecipientCurrent() {
                    if (!isCurrent())
                        throw new Error("Selected command recipient authority changed.");
                    const value = read.call(deps, expected.operationId, authority.profileBindingKey);
                    if (!value ||
                        value.kind !== "live-rebind" ||
                        value.phase !== "released" ||
                        value.cleanup ||
                        value.request.operationId !== expected.operationId ||
                        value.recipient.kind !== "follower" ||
                        value.recipient.instanceId !== instanceId ||
                        value.recipient.sessionId !== expected.sessionId ||
                        value.recipient.generation !== expected.registrationGeneration ||
                        !isDeepStrictEqual(value.executor, authority.executor) ||
                        value.operatorUserId !== authority.operatorUserId ||
                        !isDeepStrictEqual(value.request.source.updateIds, expected.sourceUpdateIds) ||
                        !isDeepStrictEqual(value.request.target, expected.target) ||
                        !isDeepStrictEqual(value.request.binding.target, oldTarget) ||
                        value.request.owner.instanceId !== instanceId ||
                        value.request.owner.owner?.kind !== "manual-follower" ||
                        value.request.owner.slot !== slot ||
                        !isDeepStrictEqual(value.request.owner.target, oldTarget) ||
                        value.request.binding.cwd !==
                            WorkspaceIdentity.normalizeTelegramWorkspacePath(authority.cwd) ||
                        value.request.binding.sessionId !== expected.sessionId ||
                        value.request.binding.slot !== slot ||
                        !isCurrent())
                        throw new Error("Selected command recipient has no exact released intent.");
                    const intent = structuredClone(value);
                    let confirmed = false;
                    snapshot.call(store, intent, (canonical) => {
                        if (!isCurrent() ||
                            !isDeepStrictEqual(read.call(deps, expected.operationId, authority.profileBindingKey), intent))
                            return;
                        const bindings = (canonical.workspaceBindings ?? []).filter((row) => row.bindingKey === intent.request.binding.bindingKey);
                        const owners = canonical.threads.filter((row) => row.status === "active" &&
                            (row.slot === slot ||
                                isDeepStrictEqual(row.target, expected.target)));
                        const row = bindings[0], owner = owners[0];
                        confirmed =
                            bindings.length === 1 &&
                                row?.sessionId === expected.sessionId &&
                                row.cwd === intent.request.binding.cwd &&
                                row.slot === slot &&
                                row.workspaceKey === intent.request.binding.workspaceKey &&
                                row.inactiveSinceMs === undefined &&
                                isDeepStrictEqual(row.target, expected.target) &&
                                owners.length === 1 &&
                                owner?.instanceId === instanceId &&
                                owner.owner?.kind === "manual-follower" &&
                                owner.slot === slot &&
                                owner.profileKey === intent.request.owner.profileKey &&
                                isDeepStrictEqual(owner.owner, intent.request.owner.owner) &&
                                isDeepStrictEqual(owner.target, expected.target) &&
                                isDeepStrictEqual(state.getTarget(), expected.target) &&
                                isCurrent() &&
                                isDeepStrictEqual(read.call(deps, expected.operationId, authority.profileBindingKey), intent);
                    });
                    if (!confirmed || !isCurrent())
                        throw new Error("Selected command canonical/local recipient changed.");
                },
            };
        },
    });
}
export function createTelegramBusFollowerClientRuntime(deps) {
    const createRequestId = createTelegramBusRequestIdFactory(deps.instanceId);
    const timeoutMs = deps.timeoutMs ?? TELEGRAM_BUS_FOLLOWER_CLIENT_TIMEOUT_MS;
    const sharedClientDeps = {
        socketPath: deps.socketPath,
        createRequestId,
        timeoutMs,
        waitForRegistrationGeneration: deps.waitForRegistrationGeneration,
    };
    return {
        createRequestId,
        callApi: createTelegramBusFollowerApiCaller({
            ...sharedClientDeps,
            instanceId: deps.instanceId,
            getAuthSecret: deps.getApiAuthSecret,
            getRegistrationGeneration: deps.getRegistrationGeneration,
        }),
        agentMessages: createTelegramBusAgentMessageClient({
            ...sharedClientDeps,
            instanceId: deps.instanceId,
            getAuthSecret: deps.getApiAuthSecret,
            getRegistrationGeneration: deps.getRegistrationGeneration,
        }),
        foreignOwnedUpdateForwarder: createTelegramBusForeignOwnedUpdateForwarder({
            ...sharedClientDeps,
            getAuthSecret: deps.getForwardingAuthSecret,
            getForwardCommentBatchPosition: deps.getForwardCommentBatchPosition,
            validateForwardOwnership: deps.validateForwardOwnership,
            recordRuntimeEvent: deps.recordRuntimeEvent,
        }),
        queueHandoff: createTelegramBusFollowerQueueHandoffClient({
            ...sharedClientDeps,
            instanceId: deps.instanceId,
            getAuthSecret: deps.getApiAuthSecret,
            getRegistrationGeneration: deps.getRegistrationGeneration,
        }),
    };
}
export function createTelegramBusFollowerQueueHandoffClient(deps) {
    const getNowMs = deps.getNowMs ?? Date.now;
    const timeoutMs = deps.timeoutMs ?? TELEGRAM_BUS_FOLLOWER_CLIENT_TIMEOUT_MS;
    return async (input) => {
        const registration = await resolveTelegramBusFollowerRegistration(deps, timeoutMs);
        const socketPath = resolveTelegramBusSocketPath(deps.socketPath);
        const response = await sendTelegramBusOperation(socketPath, registration.remainingTimeoutMs, {
            kind: "follower.offerQueueHandoff",
            requestId: deps.createRequestId(),
            auth: deps.getAuthSecret?.(),
            instanceId: deps.instanceId,
            registrationGeneration: registration.generation,
            ...input,
            sentAtMs: getNowMs(),
        });
        const queueOwner = response?.kind === "bus.ack" && isRecord(response.result)
            ? parseTelegramUpdateJournalQueueOwner(response.result.queueOwner)
            : undefined;
        if (response?.kind === "bus.ack" &&
            response.ok &&
            isRecord(response.result) &&
            response.result.status === "staged" &&
            typeof response.result.receiptId === "string" &&
            Array.isArray(response.result.sourceUpdateIds) &&
            response.result.sourceUpdateIds.every(Number.isSafeInteger) &&
            input.payload.admissionReceipts.length === 1 &&
            response.result.receiptId ===
                input.payload.admissionReceipts[0]?.receiptId &&
            response.result.sourceUpdateIds.length ===
                input.payload.admissionReceipts[0].sourceUpdateIds.length &&
            response.result.sourceUpdateIds.every((updateId, index) => updateId ===
                input.payload.admissionReceipts[0].sourceUpdateIds[index]) &&
            queueOwner) {
            return {
                status: "staged",
                receiptId: response.result.receiptId,
                sourceUpdateIds: response.result.sourceUpdateIds,
                queueOwner,
            };
        }
        throw new Error(response?.kind === "bus.ack"
            ? (response.message ?? "Telegram queue handoff was rejected.")
            : "Telegram queue handoff did not return an acknowledgement.");
    };
}
function createTelegramBusAgentMessageClient(deps) {
    const getNowMs = deps.getNowMs ?? Date.now;
    const timeoutMs = deps.timeoutMs ?? TELEGRAM_BUS_FOLLOWER_CLIENT_TIMEOUT_MS;
    const request = async (envelope, requestTimeoutMs = timeoutMs) => {
        const socketPath = resolveTelegramBusSocketPath(deps.socketPath);
        const response = await sendTelegramBusOperation(socketPath, requestTimeoutMs, envelope);
        if (response?.kind === "bus.ack" && response.ok)
            return response.result;
        throw new Error(response?.kind === "bus.ack"
            ? (response.message ?? "Telegram bus agent message failed.")
            : "Telegram bus agent message did not return an acknowledgement.");
    };
    const registrationFields = async () => {
        const registration = await resolveTelegramBusFollowerRegistration(deps, timeoutMs);
        return {
            fields: {
                auth: deps.getAuthSecret?.(),
                instanceId: deps.instanceId,
                registrationGeneration: registration.generation,
            },
            remainingTimeoutMs: registration.remainingTimeoutMs,
        };
    };
    return {
        async resolveTarget(selector) {
            const registration = await registrationFields();
            const result = await request({
                kind: "follower.resolveAgentTarget",
                requestId: deps.createRequestId(),
                ...registration.fields,
                selector,
                sentAtMs: getNowMs(),
            }, registration.remainingTimeoutMs);
            if (!result || typeof result !== "object" || Array.isArray(result)) {
                throw new Error("Telegram bus returned an invalid agent target.");
            }
            const target = result;
            if (typeof target.chatId !== "number" ||
                typeof target.threadId !== "number") {
                throw new Error("Telegram bus returned an invalid agent target.");
            }
            return { chatId: target.chatId, threadId: target.threadId };
        },
        async routeMessage(message) {
            const registration = await registrationFields();
            await request({
                kind: "follower.routeAgentMessage",
                requestId: deps.createRequestId(),
                ...registration.fields,
                message,
                sentAtMs: getNowMs(),
            }, registration.remainingTimeoutMs);
        },
    };
}
/** Private selected text-effect caller; not a guarded ordinary API adapter or follower command admission grant. */
export function createTelegramBusFollowerSelectedMenuCaller(deps) {
    return async (input) => {
        let dispatched = false, method = "sendMessage";
        try {
            const client = { ...deps.client }, recipient = deps.recipient, ctx = input.ctx, assertAuthority = input.assertAuthority;
            const getAuthority = recipient.getContextAuthority, getJournal = recipient.getJournalBindingKey, getProcess = recipient.getProcessIdentity;
            const state = recipient.registrationState, statePorts = {
                getTarget: state.getTarget,
                getSlot: state.getSlot,
                getGeneration: state.getGeneration,
                getLeaderProtocol: state.getLeaderProtocol,
            };
            const authority = structuredClone(getAuthority.call(recipient, ctx));
            const journalBindingKey = getJournal.call(recipient, ctx), process = structuredClone(getProcess.call(recipient)), target = structuredClone(state.getTarget()), slot = state.getSlot();
            const protocol = structuredClone(deps.protocolIdentity), leaderProtocol = structuredClone(state.getLeaderProtocol()), auth = client.getAuthSecret?.();
            const endpoint = resolveTelegramBusSocketPath(client.socketPath), timeoutMs = client.timeoutMs ?? TELEGRAM_BUS_FOLLOWER_CLIENT_TIMEOUT_MS;
            if (!authority ||
                typeof authority.generation !== "number" ||
                typeof assertAuthority !== "function" ||
                !slot ||
                !auth ||
                !Number.isFinite(timeoutMs) ||
                timeoutMs <= 0)
                throw new Error("Selected-menu local owner is unavailable.");
            const decoded = parseTelegramBusEnvelope(JSON.stringify({
                kind: "follower.deliverSelectedMenu",
                requestId: client.createRequestId(),
                auth,
                instanceId: client.instanceId,
                operationId: input.operationId,
                registrationGeneration: input.registrationGeneration,
                recipient: {
                    sessionId: authority.sessionId,
                    sessionGeneration: authority.generation,
                    ...process,
                    profileKey: authority.profileBindingKey,
                    journalBindingKey,
                    target,
                },
                executor: authority.executor,
                operatorUserId: authority.operatorUserId,
                effect: structuredClone(input.effect),
                sentAtMs: (client.getNowMs ?? Date.now)(),
            }));
            if (decoded?.kind !== "follower.deliverSelectedMenu")
                throw new Error("Selected-menu local description is invalid.");
            const expected = decoded;
            method =
                expected.effect.kind === "send-text"
                    ? "sendMessage"
                    : "editMessageText";
            const assertCurrent = (allowPending = false) => {
                assertAuthority();
                const local = getAuthority.call(recipient, ctx), currentGeneration = client.getRegistrationGeneration();
                if (!isDeepStrictEqual(deps.client, client) ||
                    deps.recipient !== recipient ||
                    recipient.getContextAuthority !== getAuthority ||
                    recipient.getJournalBindingKey !== getJournal ||
                    recipient.getProcessIdentity !== getProcess ||
                    recipient.registrationState !== state ||
                    !Object.entries(statePorts).every(([key, value]) => state[key] === value) ||
                    !isDeepStrictEqual(local, authority) ||
                    getJournal.call(recipient, ctx) !== journalBindingKey ||
                    !isDeepStrictEqual(getProcess.call(recipient), process) ||
                    !isDeepStrictEqual(state.getTarget(), target) ||
                    state.getSlot() !== slot ||
                    state.getGeneration() !== expected.registrationGeneration ||
                    (!allowPending &&
                        currentGeneration !== expected.registrationGeneration) ||
                    (currentGeneration !== undefined &&
                        currentGeneration !== expected.registrationGeneration) ||
                    client.getAuthSecret?.() !== auth ||
                    resolveTelegramBusSocketPath(client.socketPath) !== endpoint ||
                    !isDeepStrictEqual(deps.protocolIdentity, protocol) ||
                    !isDeepStrictEqual(state.getLeaderProtocol(), leaderProtocol) ||
                    !getTelegramBusProtocolCompatibility({
                        local: protocol,
                        remote: leaderProtocol,
                    }).compatible ||
                    !hasTelegramBusCapability(protocol, TELEGRAM_BUS_CAPABILITY_SELECTED_MENU_DELIVERY) ||
                    !hasTelegramBusCapability(leaderProtocol, TELEGRAM_BUS_CAPABILITY_SELECTED_MENU_DELIVERY))
                    throw new Error("Selected-menu local authority changed.");
                if (!isDeepStrictEqual(getAuthority.call(recipient, ctx), authority))
                    throw new Error("Selected-menu context changed after local observations.");
                assertAuthority();
            };
            assertCurrent(true);
            const registration = await resolveTelegramBusFollowerRegistration(client, timeoutMs);
            if (registration.generation !== expected.registrationGeneration)
                throw new Error("Selected-menu registration changed.");
            assertCurrent();
            dispatched = true;
            const response = await sendTelegramBusLocalEnvelope({
                socketPath: endpoint,
                envelope: expected,
                timeoutMs: registration.remainingTimeoutMs,
                retry: { attempts: 1, delayMs: 0 },
                assertAuthority: () => assertCurrent(),
            });
            assertCurrent();
            if (response?.kind !== "bus.ack" ||
                response.requestId !== expected.requestId ||
                !response.ok ||
                response.error !== undefined ||
                (response.protocol !== undefined &&
                    !isDeepStrictEqual(response.protocol, leaderProtocol)) ||
                !isRecord(response.result))
                throw new Error("Selected-menu acknowledgement is unconfirmed.");
            const result = response.result;
            if (!isDeepStrictEqual(Object.keys(result).sort(), [
                "effect",
                "messageId",
                "operationId",
                "recipient",
                "registrationGeneration",
            ]) ||
                result.operationId !== expected.operationId ||
                result.registrationGeneration !== expected.registrationGeneration ||
                result.effect !== expected.effect.kind ||
                !isDeepStrictEqual(result.recipient, expected.recipient) ||
                typeof result.messageId !== "number" ||
                !Number.isSafeInteger(result.messageId) ||
                result.messageId <= 0 ||
                (expected.effect.kind === "edit-text" &&
                    result.messageId !== expected.effect.messageId))
                throw new Error("Selected-menu recipient/result is unconfirmed.");
            assertCurrent();
            return {
                operationId: expected.operationId,
                recipient: structuredClone(expected.recipient),
                registrationGeneration: expected.registrationGeneration,
                effect: expected.effect.kind,
                messageId: result.messageId,
            };
        }
        catch (error) {
            if (!dispatched ||
                (error instanceof TelegramBusLocalAuthorityError &&
                    !error.requestIssued))
                throw new TelegramApiAuthorityError(false);
            throw new TelegramApiCommitUnknownError(method, error);
        }
    };
}
export function createTelegramBusFollowerApiCaller(deps) {
    const getNowMs = deps.getNowMs ?? Date.now;
    const timeoutMs = deps.timeoutMs ?? TELEGRAM_BUS_FOLLOWER_CLIENT_TIMEOUT_MS;
    return async (method, args) => {
        const registration = await resolveTelegramBusFollowerRegistration(deps, timeoutMs);
        const socketPath = resolveTelegramBusSocketPath(deps.socketPath);
        let response;
        try {
            response = await sendTelegramBusOperation(socketPath, registration.remainingTimeoutMs, {
                kind: "follower.callApi",
                requestId: deps.createRequestId(),
                auth: deps.getAuthSecret?.(),
                instanceId: deps.instanceId,
                registrationGeneration: registration.generation,
                method,
                args,
                sentAtMs: getNowMs(),
            });
        }
        catch (error) {
            const apiMethod = (method === "call" || method === "callMultipart") &&
                typeof args[0] === "string"
                ? args[0]
                : method;
            if (!isTelegramApiMethodRetrySafe(apiMethod)) {
                throw new TelegramApiCommitUnknownError(apiMethod, error);
            }
            throw error;
        }
        if (response?.kind === "bus.ack" && response.ok)
            return response.result;
        const message = response?.kind === "bus.ack"
            ? response.message
            : "Telegram bus API call did not return an acknowledgement.";
        if (response?.kind === "bus.ack" &&
            response.error?.code === "stale-target" &&
            response.error.chatId !== undefined &&
            response.error.threadId !== undefined) {
            throw new TelegramApiStaleTargetError(message ?? "Telegram thread target is stale.", {
                chatId: response.error.chatId,
                threadId: response.error.threadId,
            });
        }
        if (response?.kind === "bus.ack" &&
            response.error?.code === "commit-unknown") {
            throw new TelegramApiCommitUnknownError(response.error.method ?? method, new Error(message ?? "Telegram bus API call result is ambiguous."));
        }
        throw new Error(message ?? "Telegram bus API call failed.");
    };
}
async function resolveTelegramBusFollowerRegistration(deps, timeoutMs) {
    const current = deps.getRegistrationGeneration();
    if (current)
        return { generation: current, remainingTimeoutMs: timeoutMs };
    const getNowMs = deps.getNowMs ?? Date.now;
    const startedAtMs = getNowMs();
    const restored = await deps.waitForRegistrationGeneration?.(timeoutMs);
    const remainingTimeoutMs = Math.max(0, timeoutMs - (getNowMs() - startedAtMs));
    if (restored && remainingTimeoutMs > 0) {
        return { generation: restored, remainingTimeoutMs };
    }
    throw new Error("Telegram bus follower is not registered.");
}
export function createTelegramBusFollowerSessionReplacementSuspender(deps) {
    const getNowMs = deps.getNowMs ?? Date.now;
    const getPid = deps.getPid ?? (() => process.pid);
    return async (preserveTarget = true) => {
        if (!preserveTarget) {
            setTelegramFollowerSessionHandoff(undefined);
            Threads.setTelegramLeaderSessionHandoff(undefined);
            await deps.suspendPolling();
            return;
        }
        const target = deps.registrationState.getTarget();
        if (deps.registrationState.isRegistered() && target) {
            setTelegramFollowerSessionHandoff({
                pid: getPid(),
                instanceId: deps.instanceId,
                createdAtMs: getNowMs(),
                target,
                slot: deps.registrationState.getSlot(),
                threadName: deps.registrationState.getThreadName(),
            });
            deps.recordRuntimeEvent("bus", "Telegram follower registration suspended for session replacement", {
                phase: "follower-session-handoff",
                instanceId: deps.instanceId,
                chatId: target.chatId,
                threadId: target.threadId,
            });
        }
        else if (deps.isLeader?.()) {
            const leaderBinding = deps.getLeaderBinding?.();
            if (typeof leaderBinding?.target?.threadId === "number") {
                const activeContext = deps.getActiveContext?.();
                const profileKey = Threads.getTelegramThreadOwnerKey({
                    kind: "leader",
                    cwd: activeContext?.cwd,
                    instanceId: deps.instanceId,
                    telegramProfile: deps.getActiveProfileName?.(),
                });
                Threads.setTelegramLeaderSessionHandoff({
                    pid: getPid(),
                    instanceId: deps.instanceId,
                    createdAtMs: getNowMs(),
                    profileKey,
                    target: {
                        chatId: leaderBinding.target.chatId,
                        threadId: leaderBinding.target.threadId,
                    },
                    slot: leaderBinding.slot,
                    threadName: leaderBinding.threadName,
                });
                deps.recordRuntimeEvent("bus", "Telegram leader binding suspended for session replacement", {
                    phase: "leader-session-handoff",
                    instanceId: deps.instanceId,
                    chatId: leaderBinding.target.chatId,
                    threadId: leaderBinding.target.threadId,
                    slot: leaderBinding.slot,
                    threadName: leaderBinding.threadName,
                });
            }
        }
        await deps.suspendPolling();
    };
}
export function createTelegramBusFollowerSessionRefreshHook(deps) {
    return async (_event, ctx) => {
        if (deps.isSessionActive && !deps.isSessionActive(ctx))
            return;
        if (!deps.registrationState.isRegistered()) {
            const handoff = getTelegramFollowerSessionHandoff();
            const lockState = deps.getLeaderState();
            const handoffIsFresh = isTelegramFollowerSessionHandoffFresh(handoff);
            if (handoffIsFresh && lockState.kind === "active-elsewhere") {
                try {
                    const restored = await deps.registrationRuntime.registerWithLeader(ctx, lockState.lock, {
                        target: handoff.target,
                        previousInstanceId: handoff.instanceId,
                    });
                    if (deps.isSessionActive && !deps.isSessionActive(ctx))
                        return;
                    if (restored) {
                        setTelegramFollowerSessionHandoff(undefined);
                        deps.updateStatus(ctx);
                        deps.recordRuntimeEvent("bus", "Telegram follower registration restored after session replacement", {
                            phase: "follower-session-restore",
                            previousInstanceId: handoff.instanceId,
                        });
                    }
                }
                catch (error) {
                    deps.recordRuntimeEvent("bus", error, {
                        phase: "follower-session-restore",
                        previousInstanceId: handoff?.instanceId,
                    });
                }
            }
            else if (handoff) {
                setTelegramFollowerSessionHandoff(undefined);
            }
        }
        if (!deps.registrationState.isRegistered())
            return;
        if (deps.isSessionActive && !deps.isSessionActive(ctx))
            return;
        try {
            await deps.registrationRuntime.setContext(ctx);
        }
        catch (error) {
            deps.recordRuntimeEvent("bus", error, {
                phase: "follower-session-refresh",
            });
            return;
        }
        if (deps.isSessionActive && !deps.isSessionActive(ctx))
            return;
        deps.updateStatus(ctx);
        deps.recordRuntimeEvent("bus", "Telegram follower session context refreshed", { phase: "follower-session-refresh" });
    };
}
export function createTelegramBusFollowerControlState() {
    let activeAuthSecret;
    let lifecyclePhase;
    return {
        getActiveAuthSecret: () => activeAuthSecret,
        setActiveAuthSecret(secret) {
            activeAuthSecret = secret;
        },
        getLifecyclePhase: () => lifecyclePhase,
        setLifecyclePhase(phase) {
            lifecyclePhase = phase;
        },
    };
}
export function createTelegramBusFollowerRegistrationState(options = {}) {
    let registered = false;
    let target;
    let slot;
    let threadName;
    let displayTitle;
    let generation;
    let sessionId;
    let leaderProtocol;
    let eligibleElectionSlots = [];
    let recoveryEpoch = 0;
    let activeRecoveryEpoch;
    const generationWaiters = new Set();
    const settleGenerationWaiters = (value, epoch) => {
        for (const waiter of [...generationWaiters]) {
            if (epoch === undefined || waiter.epoch === epoch)
                waiter.settle(value);
        }
    };
    return {
        isRegistered: () => registered,
        getTarget: () => (target ? { ...target } : undefined),
        getSlot: () => slot,
        getThreadName: () => threadName,
        getDisplayTitle: () => displayTitle,
        setDisplayTitle(title, expectedGeneration) {
            if (!registered ||
                !generation ||
                expectedGeneration !== generation ||
                !title.trim() ||
                title.length > 128 ||
                displayTitle === title)
                return false;
            displayTitle = title;
            return true;
        },
        getGeneration: () => generation,
        getSessionId: (currentSessionId) => registered && generation && currentSessionId === sessionId
            ? sessionId
            : undefined,
        beginRecovery: () => {
            if (activeRecoveryEpoch !== undefined)
                return activeRecoveryEpoch;
            activeRecoveryEpoch = ++recoveryEpoch;
            return activeRecoveryEpoch;
        },
        cancelRecovery: () => {
            const epoch = activeRecoveryEpoch;
            activeRecoveryEpoch = undefined;
            if (epoch !== undefined)
                settleGenerationWaiters(undefined, epoch);
        },
        waitForGeneration: (timeoutMs = TELEGRAM_BUS_FOLLOWER_REGISTRATION_WAIT_MS) => {
            if (generation)
                return Promise.resolve(generation);
            const epoch = activeRecoveryEpoch;
            if (epoch === undefined)
                return Promise.resolve(undefined);
            return new Promise((resolve) => {
                let timer;
                const settle = (value) => {
                    generationWaiters.delete(waiter);
                    if (timer)
                        clearTimeout(timer);
                    resolve(value);
                };
                const waiter = { epoch, settle };
                generationWaiters.add(waiter);
                timer = setTimeout(() => settle(undefined), Math.max(0, timeoutMs));
            });
        },
        getLeaderProtocol: () => leaderProtocol
            ? { ...leaderProtocol, capabilities: [...leaderProtocol.capabilities] }
            : undefined,
        getEligibleElectionSlots: () => [...eligibleElectionSlots],
        setEligibleElectionSlots: (slots) => {
            eligibleElectionSlots = Array.from(new Set(slots.filter((slot) => /^[A-Z]$/.test(slot)))).sort();
        },
        setRegistered: (next, nextTarget, metadata) => {
            const retainedDisplayTitle = next &&
                registered &&
                generation === metadata?.generation &&
                target?.chatId === nextTarget?.chatId &&
                target?.threadId === nextTarget?.threadId
                ? displayTitle
                : undefined;
            const retainedSessionId = next && registered && generation === metadata?.generation
                ? sessionId
                : undefined;
            const availabilityChanged = registered !== next;
            registered = next;
            target = next ? (nextTarget ? { ...nextTarget } : undefined) : undefined;
            slot = next ? metadata?.slot : undefined;
            threadName = next ? metadata?.threadName : undefined;
            displayTitle =
                next &&
                    nextTarget &&
                    metadata?.generation &&
                    metadata.displayTitle?.trim() &&
                    metadata.displayTitle.length <= 128
                    ? metadata.displayTitle
                    : retainedDisplayTitle;
            generation = next ? metadata?.generation : undefined;
            sessionId = next ? (metadata?.sessionId ?? retainedSessionId) : undefined;
            leaderProtocol =
                next && metadata?.leaderProtocol
                    ? {
                        ...metadata.leaderProtocol,
                        capabilities: [...metadata.leaderProtocol.capabilities],
                    }
                    : undefined;
            if (availabilityChanged)
                options.onAvailabilityChanged?.();
            if (generation) {
                activeRecoveryEpoch = undefined;
                settleGenerationWaiters(generation);
            }
        },
    };
}
export function createTelegramBusFollowerHeartbeatRecoveryHandler(deps) {
    const promotionGraceMs = deps.promotionGraceMs ?? TELEGRAM_BUS_FOLLOWER_PROMOTION_GRACE_MS;
    const sleep = deps.sleep ??
        ((ms) => new Promise((resolve) => setTimeout(resolve, ms)));
    const scheduleRetry = deps.scheduleRetry ??
        ((retry, delayMs) => {
            const timer = setTimeout(retry, delayMs);
            timer.unref?.();
        });
    let promotionPending = false;
    const safeUpdateStatus = (ctx) => {
        try {
            deps.updateStatus(ctx);
        }
        catch (error) {
            if (!isPiStaleContextError(error))
                throw error;
            deps.recordRuntimeEvent("bus", error, {
                phase: "follower-stale-context-status",
            });
        }
    };
    const clearRegisteredState = (ctx) => {
        deps.registrationState.setRegistered(false);
        safeUpdateStatus(ctx);
    };
    const tryRegisterWithLeader = async (ctx, leader, phase, binding) => {
        try {
            const restored = await deps
                .getRegistrationRuntime()
                .registerWithLeader(ctx, leader, binding?.target ? { target: binding.target } : undefined);
            if (!restored)
                return false;
            deps.setLifecyclePhase(undefined);
            safeUpdateStatus(ctx);
            deps.recordRuntimeEvent("bus", "Telegram follower registration restored", {
                phase,
            });
            return true;
        }
        catch (error) {
            clearRegisteredState(ctx);
            deps.recordRuntimeEvent("bus", error, { phase });
            return false;
        }
    };
    const snapshotBinding = () => ({
        target: deps.registrationState.getTarget(),
        slot: deps.registrationState.getSlot(),
        threadName: deps.registrationState.getThreadName(),
    });
    const scheduleRecovery = (reason, fallbackCtx, binding) => {
        const retry = () => {
            const activeCtx = deps.getActiveContext
                ? deps.getActiveContext()
                : fallbackCtx;
            if (!activeCtx) {
                scheduleRetry(retry, promotionGraceMs);
                return;
            }
            void recover(reason, activeCtx, binding);
        };
        scheduleRetry(retry, promotionGraceMs);
    };
    const promoteToLeader = async (reason, ctx, binding, election) => {
        const activeCtx = deps.getActiveContext?.();
        if (deps.getActiveContext && activeCtx !== ctx) {
            scheduleRecovery(reason, ctx, binding);
            return;
        }
        deps.setLifecyclePhase("electing");
        safeUpdateStatus(ctx);
        deps.recordRuntimeEvent("bus", reason, {
            phase: "follower-promotion-electing",
        });
        deps.getRegistrationRuntime().stop();
        deps.setLifecyclePhase("electing");
        safeUpdateStatus(ctx);
        deps.recordRuntimeEvent("bus", "Telegram follower attempting promotion", {
            phase: "follower-promotion-electing",
        });
        const promoted = await deps.promoteToLeader(ctx, binding, election);
        deps.setLifecyclePhase(undefined);
        safeUpdateStatus(ctx);
        deps.recordRuntimeEvent("bus", promoted
            ? "Telegram follower promotion completed"
            : "Telegram follower promotion lost election", {
            phase: promoted
                ? "follower-promotion-complete"
                : "follower-promotion-lost",
        });
        if (!promoted)
            scheduleRecovery(reason, ctx, binding);
    };
    const attemptPreferredPromotion = async (reason, ctx, binding, candidateState) => {
        const slot = binding.slot;
        const lowerEligibleSlot = slot
            ? deps.registrationState
                .getEligibleElectionSlots()
                .find((candidate) => candidate < slot)
            : undefined;
        if (lowerEligibleSlot) {
            deps.recordRuntimeEvent("bus", "Telegram follower deferring to a lower-slot election candidate", {
                phase: "follower-promotion-slot-priority",
                slot,
                lowerEligibleSlot,
            });
            await sleep(promotionGraceMs);
            candidateState = deps.getLeaderState();
            if (candidateState.kind === "active-elsewhere") {
                if (!(await tryRegisterWithLeader(ctx, candidateState.lock, "follower-register-preferred-successor", binding))) {
                    scheduleRecovery(reason, ctx, binding);
                }
                return;
            }
        }
        if (candidateState.kind !== "stale" && candidateState.kind !== "inactive")
            return;
        await promoteToLeader(reason, ctx, binding, {
            expectedOwner: candidateState.kind === "stale" ? candidateState.lock : undefined,
        });
    };
    const recover = async (error, ctx, carriedBinding) => {
        if (promotionPending)
            return;
        promotionPending = true;
        deps.registrationState.beginRecovery();
        const initialBinding = carriedBinding ?? snapshotBinding();
        try {
            const state = deps.getLeaderState();
            if (state.kind === "active-elsewhere") {
                clearRegisteredState(ctx);
                if (await tryRegisterWithLeader(ctx, state.lock, "follower-register-restore", initialBinding)) {
                    return;
                }
                deps.setLifecyclePhase("electing");
                safeUpdateStatus(ctx);
                deps.recordRuntimeEvent("bus", "Telegram follower waiting for leader reload recovery", { phase: "follower-promotion-grace" });
                await sleep(promotionGraceMs);
                const graceState = deps.getLeaderState();
                if (graceState.kind === "active-elsewhere") {
                    if (await tryRegisterWithLeader(ctx, graceState.lock, "follower-register-restore-grace", initialBinding)) {
                        return;
                    }
                    if (await deps.proveLeaderUnresponsive?.(graceState.lock)) {
                        await promoteToLeader(error, ctx, initialBinding, {
                            expectedOwner: graceState.lock,
                            unresponsive: true,
                        });
                        return;
                    }
                    deps.setLifecyclePhase(undefined);
                    safeUpdateStatus(ctx);
                    deps.recordRuntimeEvent("bus", "Telegram follower promotion blocked by live leader lease", {
                        phase: "follower-promotion-live-owner",
                        leaderInstanceId: graceState.lock.instanceId,
                        leaderEpoch: graceState.lock.leaderEpoch,
                    });
                    scheduleRecovery(error, ctx, initialBinding);
                    return;
                }
                await attemptPreferredPromotion(error, ctx, initialBinding, graceState);
                return;
            }
            await attemptPreferredPromotion(error, ctx, initialBinding, state);
        }
        catch (promotionError) {
            deps.setLifecyclePhase(undefined);
            safeUpdateStatus(ctx);
            if (isPiStaleContextError(promotionError)) {
                deps.recordRuntimeEvent("bus", promotionError, {
                    phase: "follower-heartbeat-stale-context",
                });
                return;
            }
            deps.recordRuntimeEvent("bus", promotionError, {
                phase: "follower-promotion-failed",
            });
            scheduleRecovery(promotionError, ctx, initialBinding);
        }
        finally {
            promotionPending = false;
        }
    };
    return recover;
}
export function createTelegramBusFollowerRegistrationRuntime(deps) {
    const getNowMs = deps.getNowMs ?? Date.now;
    const getPid = deps.getPid ?? (() => process.pid);
    const heartbeatMs = deps.heartbeatMs ?? 1000;
    const heartbeatTimeoutMs = deps.heartbeatTimeoutMs ??
        deps.timeoutMs ??
        TELEGRAM_BUS_FOLLOWER_HEARTBEAT_TIMEOUT_MS;
    const registrationTimeoutMs = deps.registrationTimeoutMs ?? deps.timeoutMs ?? 30000;
    const registrationRetryAttempts = deps.registrationRetryAttempts ??
        TELEGRAM_BUS_FOLLOWER_REGISTRATION_RETRY_ATTEMPTS;
    const registrationRetryDelayMs = deps.registrationRetryDelayMs ??
        TELEGRAM_BUS_FOLLOWER_REGISTRATION_RETRY_DELAY_MS;
    let heartbeatInterval;
    let heartbeatPromise;
    let heartbeatPromiseGeneration;
    let activeLeaderSocketPath;
    let activeAuthSecret;
    let activeRegistrationGeneration;
    let registrationAttempt;
    let activeContext;
    let lastKnownTarget;
    let lastKnownSlot;
    let lastKnownThreadName;
    // A leader result is accepted only for the same endpoint and registration generation that issued the request.
    const isLeaderSessionCurrent = (socketPath, generation) => activeLeaderSocketPath === socketPath &&
        activeRegistrationGeneration === generation &&
        (!deps.registrationState ||
            deps.registrationState.getGeneration() === generation);
    const stopHeartbeat = () => {
        if (!heartbeatInterval)
            return;
        clearInterval(heartbeatInterval);
        heartbeatInterval = undefined;
    };
    const stop = () => {
        registrationAttempt = undefined;
        stopHeartbeat();
        activeAuthSecret = undefined;
        activeRegistrationGeneration = undefined;
        heartbeatPromise = undefined;
        heartbeatPromiseGeneration = undefined;
        deps.setActiveAuthSecret?.(undefined);
        deps.registrationState?.cancelRecovery();
        deps.registrationState?.setRegistered(false);
        lastKnownTarget = undefined;
        lastKnownSlot = undefined;
        lastKnownThreadName = undefined;
        activeContext = undefined;
        void Promise.resolve(deps.stopReceiving?.()).catch((error) => {
            try {
                deps.recordRuntimeEvent?.("bus", error, {
                    phase: "follower-receiver-stop",
                });
            }
            catch {
                // Stop diagnostics cannot create an unhandled Promise.
            }
        });
    };
    const sendHeartbeat = async () => {
        const leaderSocketPath = activeLeaderSocketPath;
        const registrationGeneration = activeRegistrationGeneration;
        const heartbeatContext = activeContext;
        if (!leaderSocketPath || !registrationGeneration)
            return;
        const isCurrentHeartbeat = () => activeLeaderSocketPath === leaderSocketPath &&
            activeRegistrationGeneration === registrationGeneration &&
            activeContext === heartbeatContext;
        try {
            const response = await sendTelegramBusOperation(leaderSocketPath, heartbeatTimeoutMs, {
                kind: "follower.heartbeat",
                requestId: deps.createRequestId(),
                auth: activeAuthSecret,
                instanceId: deps.instanceId,
                registrationGeneration,
                sentAtMs: getNowMs(),
            });
            if (!isCurrentHeartbeat())
                return;
            if (response?.kind === "bus.ack" && response.ok) {
                const heartbeatResult = isRecord(response.result)
                    ? response.result
                    : undefined;
                const slots = Array.isArray(heartbeatResult?.eligibleElectionSlots)
                    ? heartbeatResult.eligibleElectionSlots.filter((slot) => typeof slot === "string")
                    : [];
                deps.registrationState?.setEligibleElectionSlots(slots);
                if (typeof heartbeatResult?.displayTitle === "string" &&
                    deps.registrationState?.setDisplayTitle(heartbeatResult.displayTitle, registrationGeneration) &&
                    heartbeatContext) {
                    try {
                        deps.onDisplayTitleChanged?.(heartbeatContext);
                    }
                    catch {
                        // Display refresh failure must not invalidate a successful heartbeat.
                    }
                }
            }
            if (response?.kind === "bus.ack" && !response.ok) {
                throw new Error(response.message ?? "Telegram bus follower heartbeat was rejected.");
            }
        }
        catch (error) {
            if (!isCurrentHeartbeat())
                return;
            try {
                deps.recordRuntimeEvent?.("bus", error, {
                    phase: "follower-heartbeat",
                });
            }
            catch {
                // Diagnostics cannot replace heartbeat recovery.
            }
            if (!heartbeatContext)
                return;
            try {
                await deps.onHeartbeatFailure?.(error, heartbeatContext);
            }
            catch (recoveryError) {
                try {
                    deps.recordRuntimeEvent?.("bus", recoveryError, {
                        phase: "follower-heartbeat-recovery",
                    });
                }
                catch {
                    // A diagnostic sink cannot create an unhandled interval rejection.
                }
            }
        }
    };
    const requestHeartbeat = () => {
        const generation = activeRegistrationGeneration;
        if (heartbeatPromise && heartbeatPromiseGeneration === generation) {
            return heartbeatPromise;
        }
        let tracked;
        tracked = sendHeartbeat().finally(() => {
            if (heartbeatPromise === tracked) {
                heartbeatPromise = undefined;
                heartbeatPromiseGeneration = undefined;
            }
        });
        heartbeatPromise = tracked;
        heartbeatPromiseGeneration = generation;
        return tracked;
    };
    const startHeartbeat = (socketPath) => {
        stopHeartbeat();
        activeLeaderSocketPath = socketPath;
        heartbeatInterval = setInterval(() => {
            void requestHeartbeat();
        }, heartbeatMs);
        heartbeatInterval.unref?.();
    };
    return {
        registerWithLeader: async (ctx, leader, options) => {
            if (deps.isContextActive?.(ctx) === false)
                return false;
            const sessionGeneration = deps.getSessionGeneration?.();
            const sessionId = deps.getSessionId?.(ctx);
            const attempt = { ctx, sessionGeneration, sessionId };
            registrationAttempt = attempt;
            const isCurrentRequest = () => registrationAttempt === attempt &&
                deps.getSessionGeneration?.() === sessionGeneration &&
                deps.getSessionId?.(ctx) === sessionId &&
                deps.isContextActive?.(ctx) !== false;
            const abandonOwnedRequest = async () => {
                if (registrationAttempt !== attempt)
                    return;
                registrationAttempt = undefined;
                stopHeartbeat();
                activeLeaderSocketPath = undefined;
                activeAuthSecret = undefined;
                activeRegistrationGeneration = undefined;
                activeContext = undefined;
                deps.registrationState?.setRegistered(false);
                deps.setActiveAuthSecret?.(undefined);
                await deps.stopReceiving?.();
            };
            // Failed registration releases its leader session but keeps the attempt owner for caller-visible errors.
            const failRegistration = async (clearGeneration = false) => {
                stopHeartbeat();
                activeLeaderSocketPath = undefined;
                activeAuthSecret = undefined;
                if (clearGeneration)
                    activeRegistrationGeneration = undefined;
                deps.registrationState?.setRegistered(false);
                deps.setActiveAuthSecret?.(undefined);
                await deps.stopReceiving?.();
            };
            const pendingHandoff = options
                ? undefined
                : getTelegramFollowerSessionHandoff();
            const pendingHandoffOptions = isTelegramFollowerSessionHandoffFresh(pendingHandoff)
                ? {
                    target: pendingHandoff.target,
                    previousInstanceId: pendingHandoff.instanceId,
                }
                : undefined;
            const registrationOptions = options ?? pendingHandoffOptions;
            const leaderSocketPath = leader.busSocketPath ??
                deps.getLeaderSocketPath?.() ??
                getTelegramBusSocketPath();
            await deps.startReceiving?.();
            if (!isCurrentRequest()) {
                await abandonOwnedRequest();
                return false;
            }
            activeAuthSecret = deps.getLeaderAuthSecret
                ? deps.getLeaderAuthSecret(leader)
                : leader?.busSecret;
            deps.setActiveAuthSecret?.(activeAuthSecret);
            const registrationGeneration = deps.createRequestId();
            const registrationEnvelope = {
                kind: registrationOptions?.restoreWorkspace
                    ? "follower.restoreWorkspace"
                    : "follower.register",
                requestId: registrationGeneration,
                auth: activeAuthSecret,
                registration: {
                    instanceId: deps.instanceId,
                    ...(registrationOptions?.previousInstanceId
                        ? { previousInstanceId: registrationOptions.previousInstanceId }
                        : {}),
                    profileKey: deps.getProfileKey?.(ctx) ??
                        (ctx.cwd ? `cwd:${ctx.cwd}` : undefined),
                    threadName: deps.registrationState?.getThreadName() ??
                        lastKnownThreadName ??
                        deps.getThreadName?.(ctx) ??
                        (ctx.cwd ? basename(ctx.cwd) : undefined),
                    ...((deps.registrationState?.getSlot() ?? lastKnownSlot)
                        ? { slot: deps.registrationState?.getSlot() ?? lastKnownSlot }
                        : {}),
                    cwd: ctx.cwd,
                    sessionId,
                    pid: getPid(),
                    processBirthId: deps.getProcessBirthId?.(),
                    sessionGeneration,
                    target: registrationOptions?.target ??
                        deps.registrationState?.getTarget() ??
                        lastKnownTarget,
                    busSocketPath: deps.getFollowerBusSocketPath?.() ?? deps.followerBusSocketPath,
                    registrationGeneration,
                    protocol: deps.protocolIdentity,
                    connectedAtMs: getNowMs(),
                },
            };
            let response;
            try {
                response = await sendTelegramBusLocalEnvelope({
                    socketPath: leaderSocketPath,
                    timeoutMs: registrationTimeoutMs,
                    envelope: registrationEnvelope,
                    retry: getTelegramBusTransportRetryPolicy({
                        endpoint: leaderSocketPath,
                        operation: "registration",
                        overrides: {
                            attempts: registrationRetryAttempts,
                            delayMs: registrationRetryDelayMs,
                        },
                    }),
                    recordTransportEvent(phase, details) {
                        deps.recordRuntimeEvent?.("bus", `Telegram bus ${phase}`, {
                            phase: `follower-register-${phase}`,
                            ...details,
                        });
                    },
                });
            }
            catch (error) {
                if (!isCurrentRequest()) {
                    await abandonOwnedRequest();
                    return false;
                }
                await failRegistration();
                throw error;
            }
            if (!isCurrentRequest()) {
                await abandonOwnedRequest();
                return false;
            }
            const compatibility = getTelegramBusProtocolCompatibility({
                local: deps.protocolIdentity,
                remote: response?.kind === "bus.ack" ? response.protocol : undefined,
            });
            // An identity-less error ACK is a rejection/transport failure, not evidence of an incompatible leader.
            const unidentifiedRejection = response?.kind === "bus.ack" &&
                !response.ok &&
                response.protocol === undefined;
            if (!compatibility.compatible && !unidentifiedRejection) {
                await failRegistration();
                throw new Error(`Incompatible Telegram bus leader protocol: ${compatibility.reason}.`);
            }
            if (response?.kind === "bus.ack" && !response.ok) {
                await failRegistration();
                if (registrationOptions?.restoreWorkspace &&
                    response.error?.code === "workspace-binding-unavailable") {
                    deps.recordRuntimeEvent?.("bus", "Telegram follower auto-connect refused by leader: Workspace binding unavailable.", {
                        phase: "follower-auto-connect-skip",
                        reason: "leader-binding-unavailable",
                    });
                    return false;
                }
                throw new Error(response.message ??
                    "Telegram bus follower registration was rejected.");
            }
            if (response?.kind === "bus.ack" && response.ok) {
                const registrationResult = parseRegistrationResult(response.result);
                deps.registrationState?.setRegistered(true, registrationResult.target, {
                    ...registrationResult,
                    generation: registrationGeneration,
                    sessionId,
                    ...(response.protocol ? { leaderProtocol: response.protocol } : {}),
                });
                lastKnownTarget = registrationResult.target;
                lastKnownSlot = registrationResult.slot;
                lastKnownThreadName = registrationResult.threadName;
                activeLeaderSocketPath = leaderSocketPath;
                activeRegistrationGeneration = registrationGeneration;
                activeContext = ctx;
                try {
                    await deps.onRegistered?.(ctx);
                }
                catch (error) {
                    if (!isCurrentRequest()) {
                        await abandonOwnedRequest();
                        return false;
                    }
                    await failRegistration(true);
                    throw error;
                }
                if (!isCurrentRequest()) {
                    await abandonOwnedRequest();
                    return false;
                }
                await requestHeartbeat();
                if (!isCurrentRequest()) {
                    await abandonOwnedRequest();
                    return false;
                }
                startHeartbeat(leaderSocketPath);
                if (pendingHandoffOptions &&
                    getTelegramFollowerSessionHandoff()?.instanceId ===
                        pendingHandoffOptions.previousInstanceId) {
                    setTelegramFollowerSessionHandoff(undefined);
                }
                return true;
            }
            await failRegistration();
            return false;
        },
        setContext(ctx) {
            if (registrationAttempt &&
                (registrationAttempt.ctx !== ctx ||
                    registrationAttempt.sessionGeneration !==
                        deps.getSessionGeneration?.() ||
                    registrationAttempt.sessionId !== deps.getSessionId?.(ctx)))
                registrationAttempt = undefined;
            activeContext = ctx;
        },
        async setThreadDisplayMode(mode) {
            const socketPath = activeLeaderSocketPath;
            const generation = activeRegistrationGeneration;
            const auth = activeAuthSecret;
            if (!socketPath ||
                !generation ||
                !deps.registrationState?.isRegistered()) {
                throw new Error("Telegram follower is not registered with the leader.");
            }
            const requiredCapability = mode === "directory-snake" || mode === "directory-title"
                ? TELEGRAM_BUS_CAPABILITY_DIRECTORY_DISPLAY_FORMAT
                : TELEGRAM_BUS_CAPABILITY_THREAD_DISPLAY_MODE;
            if (!hasTelegramBusCapability(deps.protocolIdentity, requiredCapability) ||
                !hasTelegramBusCapability(deps.registrationState.getLeaderProtocol(), requiredCapability)) {
                throw new Error("The Telegram peers do not support this Thread display setting. Update or restart both instances.");
            }
            const requestId = deps.createRequestId();
            const response = await sendTelegramBusLocalEnvelope({
                socketPath,
                timeoutMs: registrationTimeoutMs,
                envelope: {
                    kind: "follower.setThreadDisplayMode",
                    requestId,
                    auth,
                    instanceId: deps.instanceId,
                    registrationGeneration: generation,
                    mode,
                },
            });
            if (activeLeaderSocketPath !== socketPath ||
                activeRegistrationGeneration !== generation ||
                activeAuthSecret !== auth ||
                deps.registrationState.getGeneration() !== generation) {
                throw new Error("Telegram Thread display setting completed for a stale registration.");
            }
            if (response?.kind !== "bus.ack" ||
                !response.ok ||
                response.requestId !== requestId ||
                !isRecord(response.result) ||
                response.result.mode !== mode) {
                throw new Error(response?.kind === "bus.ack"
                    ? (response.message ??
                        "Telegram Thread display setting was rejected.")
                    : "Telegram Thread display setting was not acknowledged.");
            }
        },
        async renameThread(target, threadName) {
            if (!activeLeaderSocketPath || !activeRegistrationGeneration) {
                throw new Error("Telegram follower is not registered with the leader.");
            }
            if (deps.registrationState &&
                !hasTelegramBusCapability(deps.registrationState.getLeaderProtocol(), TELEGRAM_BUS_CAPABILITY_WORKSPACE_THREAD_RENAME)) {
                throw new Error("The active Telegram leader does not support Workspace Thread rename. Update or restart that Pi instance.");
            }
            const expectedLeaderSocketPath = activeLeaderSocketPath;
            const expectedRegistrationGeneration = activeRegistrationGeneration;
            const expectedAuthSecret = activeAuthSecret;
            const response = await sendTelegramBusOperation(expectedLeaderSocketPath, registrationTimeoutMs, {
                kind: "follower.renameThread",
                requestId: deps.createRequestId(),
                auth: expectedAuthSecret,
                instanceId: deps.instanceId,
                registrationGeneration: expectedRegistrationGeneration,
                target,
                threadName,
                sentAtMs: getNowMs(),
            });
            if (response?.kind !== "bus.ack" || !response.ok) {
                throw new Error(response?.kind === "bus.ack"
                    ? (response.message ??
                        "Telegram Workspace Thread rename was rejected.")
                    : "Telegram Workspace Thread rename was not acknowledged.");
            }
            if (!isLeaderSessionCurrent(expectedLeaderSocketPath, expectedRegistrationGeneration)) {
                throw new Error("Telegram Workspace Thread rename completed for a stale follower registration.");
            }
            const renamedThreadName = response.result &&
                typeof response.result === "object" &&
                "threadName" in response.result &&
                typeof response.result.threadName === "string"
                ? response.result.threadName
                : threadName;
            const registrationTarget = deps.registrationState?.getTarget();
            if (deps.registrationState && registrationTarget) {
                deps.registrationState.setRegistered(true, registrationTarget, {
                    slot: deps.registrationState.getSlot(),
                    threadName: renamedThreadName,
                    generation: expectedRegistrationGeneration,
                    leaderProtocol: deps.registrationState.getLeaderProtocol(),
                });
            }
            return renamedThreadName;
        },
        async resetThreadName(target) {
            if (!activeLeaderSocketPath || !activeRegistrationGeneration) {
                throw new Error("Telegram follower is not registered with the leader.");
            }
            if (deps.registrationState &&
                !hasTelegramBusCapability(deps.registrationState.getLeaderProtocol(), TELEGRAM_BUS_CAPABILITY_WORKSPACE_THREAD_RENAME)) {
                throw new Error("The active Telegram leader does not support Workspace Thread reset. Update or restart that Pi instance.");
            }
            const expectedLeaderSocketPath = activeLeaderSocketPath;
            const expectedRegistrationGeneration = activeRegistrationGeneration;
            const response = await sendTelegramBusOperation(expectedLeaderSocketPath, registrationTimeoutMs, {
                kind: "follower.resetThreadName",
                requestId: deps.createRequestId(),
                auth: activeAuthSecret,
                instanceId: deps.instanceId,
                registrationGeneration: expectedRegistrationGeneration,
                target,
                sentAtMs: getNowMs(),
            });
            const resetName = response?.kind === "bus.ack" &&
                response.ok &&
                response.result &&
                typeof response.result === "object" &&
                "threadName" in response.result &&
                typeof response.result.threadName === "string"
                ? response.result.threadName
                : undefined;
            if (!resetName) {
                throw new Error(response?.kind === "bus.ack"
                    ? (response.message ??
                        "Telegram Workspace Thread reset was rejected.")
                    : "Telegram Workspace Thread reset was not acknowledged.");
            }
            if (!isLeaderSessionCurrent(expectedLeaderSocketPath, expectedRegistrationGeneration)) {
                throw new Error("Telegram Workspace Thread reset completed for a stale follower registration.");
            }
            const registrationTarget = deps.registrationState?.getTarget();
            if (deps.registrationState && registrationTarget) {
                deps.registrationState.setRegistered(true, registrationTarget, {
                    slot: deps.registrationState.getSlot(),
                    threadName: resetName,
                    generation: expectedRegistrationGeneration,
                    leaderProtocol: deps.registrationState.getLeaderProtocol(),
                });
            }
            return resetName;
        },
        async requestSessionReplacement(operation, intent) {
            if (!activeLeaderSocketPath || !activeRegistrationGeneration) {
                throw new Error("Telegram follower is not registered with the leader.");
            }
            if (!hasTelegramBusCapability(deps.protocolIdentity, TELEGRAM_BUS_CAPABILITY_SESSION_REPLACEMENT_INTENT) ||
                !hasTelegramBusCapability(deps.registrationState?.getLeaderProtocol(), TELEGRAM_BUS_CAPABILITY_SESSION_REPLACEMENT_INTENT)) {
                throw new Error("The active Telegram leader does not support follower session replacement. Update or restart that Pi instance.");
            }
            const expectedLeaderSocketPath = activeLeaderSocketPath;
            const expectedRegistrationGeneration = activeRegistrationGeneration;
            const expectedAuthSecret = activeAuthSecret;
            const requestId = deps.createRequestId();
            const response = await sendTelegramBusOperation(expectedLeaderSocketPath, registrationTimeoutMs, {
                kind: operation === "publish"
                    ? "follower.publishSessionReplacement"
                    : "follower.settleSessionReplacement",
                requestId,
                auth: expectedAuthSecret,
                instanceId: deps.instanceId,
                registrationGeneration: expectedRegistrationGeneration,
                intent,
                sentAtMs: getNowMs(),
            });
            if (response?.kind !== "bus.ack" ||
                !response.ok ||
                response.requestId !== requestId ||
                !isRecord(response.result) ||
                response.result.committed !== true) {
                throw new Error(response?.kind === "bus.ack"
                    ? (response.message ?? "Telegram session replacement was rejected.")
                    : "Telegram session replacement was not acknowledged.");
            }
            if (!isLeaderSessionCurrent(expectedLeaderSocketPath, expectedRegistrationGeneration) ||
                activeAuthSecret !== expectedAuthSecret) {
                throw new Error("Telegram session replacement completed for a stale follower registration.");
            }
            return true;
        },
        async disconnectFromLeader() {
            if (!activeLeaderSocketPath || !activeRegistrationGeneration) {
                return false;
            }
            const response = await sendTelegramBusOperation(activeLeaderSocketPath, registrationTimeoutMs, {
                kind: "follower.disconnect",
                requestId: deps.createRequestId(),
                auth: activeAuthSecret,
                instanceId: deps.instanceId,
                registrationGeneration: activeRegistrationGeneration,
                sentAtMs: getNowMs(),
            });
            if (response?.kind === "bus.ack" && response.ok)
                return true;
            throw new Error(response?.kind === "bus.ack"
                ? (response.message ?? "Telegram follower disconnect was rejected.")
                : "Telegram follower disconnect was not acknowledged.");
        },
        stop,
    };
}
const TELEGRAM_FOLLOWER_FORWARD_BATCH_POSITION_FIELD = "pi_telegram_forward_comment_batch_position";
/** Bind source-owned sender checks to the journal's already-admitted synchronous v1 hook. */
export function createTelegramBusFollowerPairedAdmission(deps) {
    return (updates, publish) => {
        if (updates.length !== 1)
            return { admitted: false };
        const update = updates[0];
        const keys = Object.keys(update).filter((key) => key !== "update_id" &&
            key !== TELEGRAM_FOLLOWER_FORWARD_BATCH_POSITION_FIELD);
        if (keys.length !== 1)
            return { admitted: false };
        const kind = keys[0];
        if (kind !== "message" &&
            kind !== "edited_message" &&
            kind !== "callback_query" &&
            kind !== "message_reaction")
            return { admitted: false };
        const position = update[TELEGRAM_FOLLOWER_FORWARD_BATCH_POSITION_FIELD];
        if (position !== undefined &&
            (kind !== "message" || (position !== "comment" && position !== "forward")))
            return { admitted: false };
        const carrier = update[kind];
        if (!Number.isSafeInteger(update.update_id) ||
            update.update_id < 0 ||
            !isRecord(carrier) ||
            carrier.pi_telegram_source_update_id !== update.update_id ||
            carrier.sender_chat !== undefined ||
            carrier.actor_chat !== undefined)
            return { admitted: false };
        const sender = kind === "message_reaction" ? carrier.user : carrier.from;
        if (!isRecord(sender) ||
            typeof sender.id !== "number" ||
            !Number.isSafeInteger(sender.id) ||
            sender.id <= 0 ||
            sender.is_bot !== false)
            return { admitted: false };
        return deps.configStore.withPairedUserAdmission(deps.profileName, deps.tokenSha256, sender.id, publish, deps.assertExecutionCurrent);
    };
}
/** One authenticated local operation request with the shared operation retry policy; no replay semantics. */
function sendTelegramBusOperation(socketPath, timeoutMs, envelope) {
    return sendTelegramBusLocalEnvelope({
        socketPath,
        timeoutMs,
        envelope,
        retry: getTelegramBusTransportRetryPolicy({
            endpoint: socketPath,
            operation: "operation",
        }),
    });
}
export function prepareTelegramBusFollowerJournaledUpdateForExecution(update, prepareForwardedMessage) {
    const position = update[TELEGRAM_FOLLOWER_FORWARD_BATCH_POSITION_FIELD];
    if (update.message !== undefined &&
        (position === "comment" || position === "forward")) {
        prepareForwardedMessage(update.message, position);
    }
    if (position === undefined)
        return update;
    const prepared = { ...update };
    delete prepared[TELEGRAM_FOLLOWER_FORWARD_BATCH_POSITION_FIELD];
    return prepared;
}
/** Copy the captured command target for the native owner; ordinary input supplies no target constraint. */
function copyLiveRebindCommandTarget(saved) {
    return saved.selectedCommand
        ? { expectedTarget: { ...saved.selectedCommand.target } }
        : {};
}
/** Exact operation/recipient/source agreement between a retained save and a later request; identity only, never currency. */
function matchesLiveRebindCarrier(saved, envelope) {
    return (saved.operationId === envelope.operationId &&
        saved.recipient.instanceId === envelope.recipientInstanceId &&
        saved.recipient.sessionId === envelope.recipientSessionId &&
        saved.recipient.generation === envelope.recipientRegistrationGeneration &&
        saved.recipient.bindingKey === envelope.recipientBindingKey &&
        isDeepStrictEqual(saved.sourceUpdateIds, [...envelope.sourceUpdateIds].sort((a, b) => a - b)));
}
/** Exact branch agreement: a command record requires its saved proof; ordinary input refuses any proof. */
function matchesLiveRebindBranch(saved, status, envelope) {
    return (isDeepStrictEqual(saved.selectedCommand, envelope.selectedCommand) &&
        (status
            ? isDeepStrictEqual(saved.preparedSource, envelope.preparedSource)
            : envelope.preparedSource === undefined));
}
/** One current warm preparation, not a prompt queue or restart recovery record. */
export function createTelegramBusFollowerLiveRebindRuntime(deps) {
    let retained;
    const runtime = {
        save(envelope, ctx, isCurrent) {
            const captured = structuredClone(envelope), selectedCommand = captured.selectedCommand;
            const heldMarker = selectedCommand, getCommandOwner = heldMarker ? deps.getCommandOwner : undefined, applyOwner = deps.applyTarget;
            if (heldMarker !== undefined &&
                (!getCommandOwner || !applyOwner || captured.updates.length !== 1))
                throw new Error("Selected command native consumption is unavailable.");
            const admission = deps.getAdmission(ctx), prepareInput = admission?.prepareLiveInput, append = admission?.appendBatch;
            let status = retained?.operationId === captured.operationId
                ? retained.status
                : undefined;
            const current = () => isCurrent() &&
                deps.getAdmission(ctx) === admission &&
                admission?.getJournalBindingKey() === captured.recipientBindingKey &&
                (!heldMarker ||
                    (deps.getCommandOwner === getCommandOwner &&
                        deps.applyTarget === applyOwner &&
                        admission?.prepareLiveInput === prepareInput &&
                        admission?.appendBatch === append &&
                        !!status?.current()));
            if (retained?.status &&
                retained.operationId !== captured.operationId &&
                retained.outcome?.status === "released" &&
                (retained.status.result !== "completed" ||
                    !retained.status.ports?.inspectCompletion.call(retained.status.plan)))
                throw new Error("Released held command is unconfirmed; no carrier replacement.");
            if (heldMarker && !status) {
                const { recipientInstanceId, recipientRegistrationGeneration, recipientSessionId, recipientBindingKey, operationId, updates, } = captured;
                const owner = getCommandOwner(structuredClone({
                    recipientInstanceId,
                    recipientRegistrationGeneration,
                    recipientSessionId,
                    recipientBindingKey,
                    operationId,
                    updates,
                    selectedCommand,
                }), ctx);
                if (!owner ||
                    typeof owner.isCurrent !== "function" ||
                    typeof owner.prepare !== "function" ||
                    typeof owner.assertRecipientCurrent !== "function")
                    throw new Error("Selected command preparation owner is unavailable.");
                const ports = {
                    isCurrent: owner.isCurrent,
                    prepare: owner.prepare,
                    assertRecipientCurrent: owner.assertRecipientCurrent,
                };
                status = {
                    owner,
                    current: () => Object.entries(ports).every(([key, value]) => Reflect.get(owner, key) === value) &&
                        (!status?.plan ||
                            Object.entries(status.ports).every(([key, value]) => Reflect.get(status.plan, key) === value)) &&
                        ports.isCurrent.call(owner),
                    prepare: ports.prepare,
                    recipient: ports.assertRecipientCurrent,
                };
            }
            if (!admission?.prepareLiveInput || !current())
                throw new Error("Live-rebind recipient admission is unavailable.");
            const digest = createHash("sha256")
                .update(JSON.stringify(captured.updates))
                .digest("hex");
            if (retained?.status && !retained.current())
                throw new Error("Prepared command authority changed; no replacement adoption.");
            if (retained &&
                (!retained.current() ||
                    (retained.operationId !== envelope.operationId &&
                        (retained.outcome?.status === "released" ||
                            retained.outcome?.status === "discarded"))))
                retained = undefined;
            if (heldMarker &&
                retained &&
                (retained.ctx !== ctx ||
                    retained.observation.recipient.instanceId !==
                        captured.recipientInstanceId ||
                    retained.observation.recipient.sessionId !==
                        captured.recipientSessionId ||
                    retained.observation.recipient.generation !==
                        captured.recipientRegistrationGeneration ||
                    retained.observation.recipient.bindingKey !==
                        captured.recipientBindingKey))
                throw new Error("Selected command saved recipient changed.");
            if (retained &&
                (retained.operationId !== captured.operationId ||
                    retained.digest !== digest ||
                    !isDeepStrictEqual(retained.observation.selectedCommand, selectedCommand)))
                throw new Error("Another live-rebind input or branch is already prepared.");
            if (!retained) {
                const held = admission.prepareLiveInput(ctx, captured.updates.map((update) => update.update_id), current);
                if (!held)
                    throw new Error("Live-rebind recipient input cannot be prepared.");
                if (heldMarker && !held.canPrepareStatusConsumption?.()) {
                    held.cancelEmpty();
                    throw new Error("Selected command native consumption is unavailable.");
                }
                retained = {
                    operationId: captured.operationId,
                    digest,
                    current,
                    held,
                    ctx,
                    ...(status ? { status } : {}),
                    observation: {
                        operationId: captured.operationId,
                        recipient: {
                            instanceId: captured.recipientInstanceId,
                            sessionId: captured.recipientSessionId,
                            generation: captured.recipientRegistrationGeneration,
                            bindingKey: captured.recipientBindingKey,
                        },
                        sourceUpdateIds: [...held.sourceUpdateIds].sort((a, b) => a - b),
                        ...(selectedCommand
                            ? { selectedCommand: structuredClone(selectedCommand) }
                            : {}),
                        status: "saved",
                    },
                };
                try {
                    admission.appendBatch(captured.updates);
                }
                catch (error) {
                    if (!held.confirmSaved())
                        throw error;
                }
            }
            if (!current() || !retained.held.confirmSaved())
                throw new Error("Live-rebind recipient save is unconfirmed.");
            if (status && !status.plan) {
                if (status.attempted)
                    throw new Error("Selected command preparation is unconfirmed; no replay.");
                status.attempted = true;
                const record = retained, readiness = record.held.prepareSourceCompletion?.();
                const assertSourceCurrent = () => {
                    if (retained !== record ||
                        !record.current() ||
                        !record.held.isOwnerCurrent())
                        throw new Error("Selected command source owner changed.");
                };
                const plan = readiness &&
                    status.prepare.call(status.owner, readiness, ctx, {
                        target: { ...heldMarker.target },
                        assertSourceCurrent,
                        assertRecipientCurrent: status.recipient,
                    });
                if (!plan ||
                    !current() ||
                    !record.held.confirmSaved() ||
                    !isDeepStrictEqual(plan.source, readiness.snapshot.source) ||
                    [plan.bindCarrier, plan.execute, plan.inspectCompletion].some((port) => typeof port !== "function"))
                    throw new Error("Selected command plan is unavailable.");
                status.plan = plan;
                status.ports = {
                    bindCarrier: plan.bindCarrier,
                    execute: plan.execute,
                    inspectCompletion: plan.inspectCompletion,
                };
                const continuation = {
                    source: plan.source,
                    assertCurrent() {
                        assertSourceCurrent();
                        if (record.outcome?.status !== "released")
                            throw new Error("Selected command has no confirmed peer release.");
                        status.recipient.call(status.owner);
                    },
                    bindCarrier: status.ports.bindCarrier.bind(plan),
                    async execute() {
                        status.result = "unknown";
                        const completed = await status.ports.execute.call(plan);
                        if (completed && record.current() && record.held.isOwnerCurrent())
                            status.result = "completed";
                        return completed;
                    },
                };
                if (!record.held.prepareStatusConsumption?.(continuation) ||
                    !current()) {
                    delete status.plan;
                    delete status.ports;
                    throw new Error("Selected command consumption is unconfirmed.");
                }
                record.observation.preparedSource = { ...plan.source };
            }
            return structuredClone(retained.observation);
        },
        async apply(envelope, ctx, isCurrent) {
            const record = retained;
            if (envelope.selectedCommand !== undefined && !record?.status?.plan)
                throw new Error("Selected command native consumption is unavailable.");
            const current = () => retained === record &&
                !!record &&
                record.ctx === ctx &&
                record.current() &&
                isCurrent() &&
                record.held.isCurrent() &&
                matchesLiveRebindBranch(record.observation, !!record.status, envelope);
            if (!record ||
                !current() ||
                !record.held.confirmSaved() ||
                !deps.applyTarget ||
                record.applying ||
                record.settling ||
                !matchesLiveRebindCarrier(record.observation, envelope))
                throw new Error("Live-rebind apply has no exact current saved carrier.");
            const mode = envelope.mode === "apply" && !record.applyIssued ? "apply" : "inspect";
            if (mode === "apply")
                record.applyIssued = true;
            record.applying = true;
            try {
                const observation = await deps.applyTarget({
                    operationId: record.operationId,
                    registrationGeneration: envelope.recipientRegistrationGeneration,
                    mode,
                    liveRebind: {
                        sourceUpdateIds: record.observation.sourceUpdateIds,
                        isCurrent: current,
                        ...copyLiveRebindCommandTarget(record.observation),
                    },
                }, ctx);
                if (!current() ||
                    !record.held.confirmSaved() ||
                    observation.operationId !== record.operationId ||
                    observation.recipient.instanceId !== envelope.recipientInstanceId ||
                    observation.recipient.sessionId !== envelope.recipientSessionId ||
                    observation.recipient.generation !==
                        envelope.recipientRegistrationGeneration)
                    throw new Error("Live-rebind apply observation changed.");
                return {
                    ...structuredClone(record.observation),
                    target: { ...observation.target },
                    slot: observation.slot,
                    status: observation.ready ? "applied" : "saved",
                };
            }
            finally {
                delete record.applying;
            }
        },
        async settle(envelope, ctx, isCurrent) {
            if (envelope.mode === "observe-command")
                return inspectCommandCompletion(envelope, ctx, isCurrent);
            const record = retained, status = record?.status, plan = status?.plan;
            if (envelope.selectedCommand !== undefined && !plan)
                throw new Error("Selected command native consumption is unavailable.");
            // Only the marked read-only held work sample may outlive the source worker, through the captured recipient owner.
            const commandWork = envelope.mode === "observe" && envelope.selectedCommand !== undefined;
            const recipientCurrent = () => {
                if (record?.status !== status ||
                    status?.plan !== plan ||
                    !status?.current())
                    return false;
                try {
                    status.recipient.call(status.owner);
                    return true;
                }
                catch {
                    return false;
                }
            };
            const current = () => retained === record &&
                !!record &&
                record.ctx === ctx &&
                record.current() &&
                isCurrent() &&
                (commandWork ? recipientCurrent() : record.held.isOwnerCurrent()) &&
                matchesLiveRebindBranch(record.observation, !!record.status, envelope);
            if (!record ||
                !current() ||
                record.applying ||
                record.settling ||
                !matchesLiveRebindCarrier(record.observation, envelope))
                throw new Error("Live-rebind settlement has no exact current carrier.");
            if (envelope.mode === "observe") {
                if (record.outcome?.status !== "released" ||
                    !deps.applyTarget ||
                    !deps.observeWork ||
                    !envelope.oldTarget)
                    throw new Error("Live-rebind work observation has no released carrier or observer.");
                const oldTarget = { ...envelope.oldTarget };
                let work;
                record.settling = true;
                try {
                    await deps.applyTarget({
                        operationId: record.operationId,
                        registrationGeneration: envelope.recipientRegistrationGeneration,
                        mode: "inspect",
                        liveRebind: {
                            sourceUpdateIds: record.observation.sourceUpdateIds,
                            isCurrent: current,
                            ...copyLiveRebindCommandTarget(record.observation),
                            observeWork(canonicalOldTarget) {
                                if (!current() ||
                                    !isDeepStrictEqual(canonicalOldTarget, oldTarget))
                                    throw new Error("Live-rebind old work target changed.");
                                work = structuredClone(deps.observeWork(canonicalOldTarget, ctx));
                                if (!current() ||
                                    !isDeepStrictEqual(canonicalOldTarget, oldTarget))
                                    throw new Error("Live-rebind work observer authority or target changed.");
                            },
                        },
                    }, ctx);
                    if (!current() || !work)
                        throw new Error("Live-rebind work observation is unconfirmed.");
                    // Unfinished or unconfirmed selected execution stays unknown; later settled delivery cannot clear it.
                    if (commandWork && status.result !== "completed")
                        work.unknown = true;
                    if (!current())
                        throw new Error("Live-rebind work observer authority changed.");
                    return {
                        ...structuredClone(record.observation),
                        status: "observed",
                        oldTarget,
                        work,
                    };
                }
                finally {
                    delete record.settling;
                }
            }
            const action = envelope.mode;
            if (record.outcome) {
                if (record.outcome.action !== envelope.mode)
                    throw new Error("Live-rebind input already has another disposition.");
                return structuredClone(record.outcome);
            }
            record.settling = true;
            const observation = (status) => ({
                ...structuredClone(record.observation),
                action,
                status,
            });
            try {
                if (envelope.mode === "discard") {
                    record.outcome = observation("unknown");
                    const status = record.held.discardSaved(current);
                    record.outcome =
                        status === "protected" ? undefined : observation(status);
                    if (!current())
                        throw new Error("Live-rebind disposal authority changed.");
                    return observation(status);
                }
                if (!record.held.isCurrent() ||
                    !record.held.confirmSaved() ||
                    !deps.applyTarget)
                    throw new Error("Live-rebind input cannot be released.");
                await deps.applyTarget({
                    operationId: record.operationId,
                    registrationGeneration: envelope.recipientRegistrationGeneration,
                    mode: "inspect",
                    liveRebind: {
                        sourceUpdateIds: record.observation.sourceUpdateIds,
                        isCurrent: current,
                        ...copyLiveRebindCommandTarget(record.observation),
                        release(canRelease) {
                            record.outcome = observation("unknown");
                            record.outcome = record.held.release(() => current() && canRelease())
                                ? observation("released")
                                : undefined;
                        },
                    },
                }, ctx);
                if (!current())
                    throw new Error("Live-rebind release authority changed.");
                return structuredClone(record.outcome ?? observation("protected"));
            }
            finally {
                delete record.settling;
            }
        },
    };
    async function inspectCommandCompletion(envelope, ctx, isCurrent) {
        const record = retained;
        const current = () => retained === record &&
            !!record?.status?.plan &&
            record.ctx === ctx &&
            record.current() &&
            record.held.isOwnerCurrent() &&
            isCurrent() &&
            matchesLiveRebindBranch(record.observation, true, envelope);
        if (!record?.status?.plan ||
            !deps.applyTarget ||
            !current() ||
            record.outcome?.status !== "released" ||
            record.applying ||
            record.settling ||
            !matchesLiveRebindCarrier(record.observation, envelope))
            throw new Error("Selected command completion has no exact released carrier.");
        record.settling = true;
        try {
            const assertRecipient = () => {
                record.status.recipient.call(record.status.owner);
                if (!current())
                    throw new Error("Selected command completion recipient changed.");
            };
            assertRecipient();
            const observed = await deps.applyTarget({
                operationId: record.operationId,
                registrationGeneration: record.observation.recipient.generation,
                mode: "inspect",
                liveRebind: {
                    sourceUpdateIds: record.observation.sourceUpdateIds,
                    expectedTarget: { ...record.observation.selectedCommand.target },
                    isCurrent: current,
                },
            }, ctx);
            if (!observed.ready || !current())
                throw new Error("Selected command completion target is unconfirmed.");
            assertRecipient();
            const sourceAck = record.status.ports.inspectCompletion.call(record.status.plan);
            assertRecipient();
            if (sourceAck &&
                !isDeepStrictEqual(sourceAck, record.observation.preparedSource))
                throw new Error("Selected command completion exact ACK changed.");
            const settled = {
                preparedSource: { ...record.observation.preparedSource },
                command: record.status.result ?? "not-issued",
                ...(sourceAck ? { sourceAck: { ...sourceAck } } : {}),
            };
            const { selectedCommand } = record.observation;
            if (envelope.mode !== "observe-command" || !selectedCommand)
                throw new Error("Held completion mode does not match its marker.");
            return {
                ...structuredClone(record.observation),
                status: "command-observed",
                selectedCommand: structuredClone(selectedCommand),
                ...settled,
            };
        }
        finally {
            delete record.settling;
        }
    }
    return runtime;
}
export function createTelegramBusFollowerDurableAdmissionRuntime(deps) {
    // The journal forgets a delivery once its worker completes; this window absorbs a leader retry after a lost ACK.
    const recentDeliveries = new Map();
    const getNowMs = deps.getNowMs ?? Date.now;
    const limit = deps.recentDeliveryLimit ?? {
        maxAgeMs: 24 * 60 * 60 * 1000,
        maxEntries: 4096,
    };
    const pruneRecentDeliveries = (nowMs) => {
        for (const [deliveryId, admittedAtMs] of recentDeliveries) {
            if (recentDeliveries.size <= limit.maxEntries &&
                nowMs - admittedAtMs < limit.maxAgeMs)
                break;
            recentDeliveries.delete(deliveryId);
        }
    };
    return {
        async admit(envelope, ctx) {
            const delivery = envelope.delivery;
            if (!delivery) {
                throw new Error("Telegram follower durable admission requires delivery identity.");
            }
            if (envelope.kind === "leader.wakeInputCustody")
                throw new Error("Telegram follower custody wake cannot use executable-copy admission.");
            const expected = createTelegramBusFollowerDeliveryIdentity({
                kind: envelope.kind,
                recipientBindingKey: delivery.recipientBindingKey,
                sourceUpdateId: delivery.sourceUpdateId,
            });
            if (expected.deliveryId !== delivery.deliveryId) {
                throw new Error("Invalid Telegram follower delivery id.");
            }
            const carrier = envelope.kind === "leader.forwardCallback"
                ? envelope.query
                : envelope.kind === "leader.forwardReaction"
                    ? envelope.reactionUpdate
                    : envelope.message;
            if (!isRecord(carrier) ||
                carrier.pi_telegram_source_update_id !== delivery.sourceUpdateId) {
                throw new Error("Telegram follower delivery source update id mismatch.");
            }
            const update = {
                update_id: delivery.sourceUpdateId,
                ...(envelope.kind === "leader.forwardMessage" &&
                    envelope.forwardCommentBatchPosition
                    ? {
                        [TELEGRAM_FOLLOWER_FORWARD_BATCH_POSITION_FIELD]: envelope.forwardCommentBatchPosition,
                    }
                    : {}),
                ...(envelope.kind === "leader.forwardCallback"
                    ? { callback_query: carrier }
                    : envelope.kind === "leader.forwardReaction"
                        ? { message_reaction: carrier }
                        : envelope.kind === "leader.forwardMessage"
                            ? { message: carrier }
                            : { edited_message: carrier }),
            };
            const result = {
                deliveryId: delivery.deliveryId,
                sourceUpdateId: delivery.sourceUpdateId,
            };
            pruneRecentDeliveries(getNowMs());
            if (recentDeliveries.has(delivery.deliveryId))
                return result;
            deps.journal.appendBatch([update]);
            recentDeliveries.set(delivery.deliveryId, getNowMs());
            pruneRecentDeliveries(getNowMs());
            deps.signalWorker(ctx);
            return result;
        },
    };
}
export function createTelegramBusFollowerInputCustodyPorts(deps) {
    const getRequired = () => {
        const bundle = deps.getInputCustodyBus();
        if (!bundle)
            throw new Error("Telegram input custody bus binding is unavailable.");
        return bundle;
    };
    return {
        isSourceReferenceAdmissionEnabled: () => Boolean(deps.getInputCustodyBus()),
        handleInputCustodyHandoff(envelope, ctx) {
            return getRequired().acceptHandoff({
                sourceRecoveryKey: envelope.sourceRecoveryKey,
                recipientBindingKey: envelope.recipientBindingKey,
                source: envelope.source,
                handoffId: envelope.handoffId,
            }, ctx);
        },
        sourceReferenceAdmission: createTelegramBusFollowerSourceReferenceAdmissionRuntime({
            wakeSource(input, ctx) {
                getRequired().wakeSource(input, ctx);
            },
        }),
        resolveInputCustodyReference(input) {
            return deps.getInputCustodyBus()?.resolveForwardReference(input);
        },
    };
}
export function createTelegramBusFollowerSourceReferenceAdmissionRuntime(deps) {
    return {
        async admit(envelope, ctx) {
            const delivery = envelope.delivery;
            if (!delivery)
                throw new Error("Telegram follower source-reference admission requires delivery identity.");
            if (!delivery.sourceRecoveryKey)
                throw new Error("Telegram follower source-reference admission requires a recovery key.");
            if (!delivery.sourceClaim)
                throw new Error("Telegram follower source-reference admission requires exact claim evidence.");
            const expected = createTelegramBusFollowerDeliveryIdentity({
                kind: envelope.kind,
                recipientBindingKey: delivery.recipientBindingKey,
                sourceUpdateId: delivery.sourceUpdateId,
            });
            if (expected.deliveryId !== delivery.deliveryId)
                throw new Error("Invalid Telegram follower source-reference delivery id.");
            const carrier = envelope.kind === "leader.wakeInputCustody"
                ? undefined
                : envelope.kind === "leader.forwardCallback"
                    ? envelope.query
                    : envelope.kind === "leader.forwardReaction"
                        ? envelope.reactionUpdate
                        : envelope.message;
            if (envelope.kind !== "leader.wakeInputCustody" &&
                (!isRecord(carrier) ||
                    carrier.pi_telegram_source_update_id !== delivery.sourceUpdateId))
                throw new Error("Telegram follower source-reference update id mismatch.");
            await deps.wakeSource({
                deliveryId: delivery.deliveryId,
                sourceUpdateId: delivery.sourceUpdateId,
                recipientBindingKey: delivery.recipientBindingKey,
                sourceRecoveryKey: delivery.sourceRecoveryKey,
                sourceClaim: { ...delivery.sourceClaim },
            }, ctx);
            return {
                deliveryId: delivery.deliveryId,
                sourceUpdateId: delivery.sourceUpdateId,
            };
        },
    };
}
export function createTelegramBusForwardedUpdateReceiverRuntime(deps) {
    const server = createTelegramBusLocalServer({
        socketPath: deps.socketPath,
        recordTransportEvent(phase, details) {
            deps.recordRuntimeEvent?.("bus", `Telegram bus ${phase}`, {
                phase: `follower-receiver-${phase}`,
                ...details,
            });
        },
        async handleEnvelope(envelope) {
            const authSecret = deps.getAuthSecret?.();
            if (deps.getAuthSecret &&
                (!authSecret || !isTelegramBusEnvelopeAuthorized(envelope, authSecret))) {
                return createUnauthorizedBusAck(envelope.requestId);
            }
            if ((envelope.kind !== "leader.forwardCallback" &&
                envelope.kind !== "leader.forwardReaction" &&
                envelope.kind !== "leader.forwardMessage" &&
                envelope.kind !== "leader.forwardEditedMessage" &&
                envelope.kind !== "leader.wakeInputCustody" &&
                envelope.kind !== "leader.offerInputCustodyHandoff" &&
                envelope.kind !== "leader.workspaceRestore" &&
                envelope.kind !== "leader.prepareLiveRebind" &&
                envelope.kind !== "leader.applyLiveRebind" &&
                envelope.kind !== "leader.settleLiveRebind" &&
                envelope.kind !== "leader.offerQueueHandoff") ||
                envelope.recipientInstanceId !== deps.instanceId) {
                return rejectTelegramBusRequest(envelope.requestId, "Telegram bus receiver cannot handle this envelope.");
            }
            const registrationGeneration = deps.getRegistrationGeneration();
            if (!registrationGeneration ||
                envelope.recipientRegistrationGeneration !== registrationGeneration) {
                return rejectTelegramBusRequest(envelope.requestId, "Stale Telegram bus follower registration generation.");
            }
            if (envelope.kind === "leader.prepareLiveRebind" ||
                envelope.kind === "leader.applyLiveRebind" ||
                envelope.kind === "leader.settleLiveRebind") {
                const apply = envelope.kind !== "leader.prepareLiveRebind", settle = envelope.kind === "leader.settleLiveRebind";
                const commandInput = envelope.selectedCommand, commandEnabled = deps.isLiveRebindCommandSetEnabled;
                const required = [
                    TELEGRAM_BUS_CAPABILITY_LIVE_REBIND_SAVE,
                    ...(apply ? [TELEGRAM_BUS_CAPABILITY_LIVE_REBIND_APPLY] : []),
                    ...(settle ? [TELEGRAM_BUS_CAPABILITY_LIVE_REBIND_SETTLE] : []),
                    ...(commandInput
                        ? [
                            TELEGRAM_BUS_CAPABILITY_LIVE_REBIND_APPLY,
                            TELEGRAM_BUS_CAPABILITY_LIVE_REBIND_SETTLE,
                            TELEGRAM_BUS_CAPABILITY_LIVE_REBIND_COMMAND_SET,
                            TELEGRAM_BUS_CAPABILITY_SELECTED_MENU_DELIVERY,
                        ]
                        : []),
                ];
                const ctx = deps.getContext(), sessionId = deps.getSessionId?.(), bindingKey = deps.getLiveRebindJournalBindingKey?.();
                const protocol = structuredClone(deps.getLeaderProtocol?.()), localProtocol = structuredClone(deps.getLocalProtocol?.());
                const current = () => !!authSecret &&
                    deps.getAuthSecret?.() === authSecret &&
                    deps.getContext() === ctx &&
                    deps.getRegistrationGeneration() === registrationGeneration &&
                    deps.getSessionId?.() === sessionId &&
                    deps.getLiveRebindJournalBindingKey?.() === bindingKey &&
                    deps.isLiveRebindSaveEnabled?.() === true &&
                    (!apply || deps.isLiveRebindApplyEnabled?.() === true) &&
                    (!settle || deps.isLiveRebindSettleEnabled?.() === true) &&
                    (!commandInput ||
                        (typeof commandEnabled === "function" &&
                            deps.isLiveRebindCommandSetEnabled === commandEnabled &&
                            commandEnabled())) &&
                    isDeepStrictEqual(deps.getLeaderProtocol?.(), protocol) &&
                    isDeepStrictEqual(deps.getLocalProtocol?.(), localProtocol);
                if (!authSecret ||
                    !ctx ||
                    !bindingKey ||
                    sessionId !== envelope.recipientSessionId ||
                    bindingKey !== envelope.recipientBindingKey ||
                    !required.every((cap) => hasTelegramBusCapability(protocol, cap) &&
                        hasTelegramBusCapability(localProtocol, cap)) ||
                    !localProtocol ||
                    !getTelegramBusProtocolCompatibility({
                        local: localProtocol,
                        remote: protocol,
                    }).compatible ||
                    !current() ||
                    (settle
                        ? !deps.handleLiveRebindSettle
                        : apply
                            ? !deps.handleLiveRebindApply
                            : !deps.handleLiveRebindSave))
                    return rejectTelegramBusRequest(envelope.requestId, "Live-rebind capability or recipient authority is unavailable.");
                try {
                    const result = envelope.kind === "leader.settleLiveRebind"
                        ? await deps.handleLiveRebindSettle(envelope, ctx, current)
                        : envelope.kind === "leader.applyLiveRebind"
                            ? await deps.handleLiveRebindApply(envelope, ctx, current)
                            : await deps.handleLiveRebindSave(envelope, ctx, current);
                    if (!current())
                        throw new Error("Live-rebind receiver authority changed.");
                    return {
                        kind: "bus.ack",
                        requestId: envelope.requestId,
                        ok: true,
                        result,
                    };
                }
                catch (error) {
                    deps.recordRuntimeEvent?.("bus", error, {
                        phase: "follower-live-rebind-control",
                    });
                    return {
                        kind: "bus.ack",
                        requestId: envelope.requestId,
                        ok: false,
                        message: error instanceof Error
                            ? error.message
                            : "Live-rebind request failed.",
                    };
                }
            }
            if (envelope.kind === "leader.workspaceRestore") {
                const ctx = deps.getContext();
                if (!authSecret ||
                    !ctx ||
                    !deps.isWorkspaceRestoreEnabled?.() ||
                    !deps.handleWorkspaceRestore)
                    return rejectTelegramBusRequest(envelope.requestId, "Workspace Restore capability is unavailable.");
                try {
                    const result = await deps.handleWorkspaceRestore({
                        operationId: envelope.operationId,
                        registrationGeneration,
                        mode: envelope.mode,
                    }, ctx);
                    if (deps.getAuthSecret?.() !== authSecret ||
                        deps.getRegistrationGeneration() !== registrationGeneration ||
                        deps.getContext() !== ctx ||
                        !deps.isWorkspaceRestoreEnabled())
                        throw new Error("Workspace Restore receiver authority changed.");
                    return {
                        kind: "bus.ack",
                        requestId: envelope.requestId,
                        ok: true,
                        result,
                    };
                }
                catch (error) {
                    deps.recordRuntimeEvent?.("bus", error, {
                        phase: "follower-workspace-restore",
                    });
                    return {
                        kind: "bus.ack",
                        requestId: envelope.requestId,
                        ok: false,
                        message: error instanceof Error
                            ? error.message
                            : "Workspace Restore failed.",
                    };
                }
            }
            if (envelope.kind === "leader.offerInputCustodyHandoff" &&
                envelope.recipientBindingKey !== deps.getRecipientBindingKey())
                return rejectTelegramBusRequest(envelope.requestId, "Mismatched Telegram follower handoff recipient identity.");
            if (envelope.kind !== "leader.offerQueueHandoff" &&
                envelope.kind !== "leader.offerInputCustodyHandoff" &&
                (!envelope.delivery ||
                    envelope.delivery.recipientBindingKey !==
                        deps.getRecipientBindingKey())) {
                return rejectTelegramBusRequest(envelope.requestId, "Mismatched Telegram follower delivery identity.");
            }
            const ctx = deps.getContext();
            if (!ctx) {
                return rejectTelegramBusRequest(envelope.requestId, "Telegram bus follower has no active context.");
            }
            try {
                if (envelope.kind === "leader.offerInputCustodyHandoff") {
                    if (!(deps.isSourceReferenceAdmissionEnabled?.() ?? false))
                        throw new Error("Telegram input custody handoff capability is not enabled.");
                    if (!deps.hasAuthenticatedSourceReferenceTransport?.())
                        throw new Error("Telegram input custody handoff requires authenticated transport.");
                    if (!deps.handleInputCustodyHandoff)
                        throw new Error("Telegram input custody handoff acceptance is unavailable.");
                    const result = await deps.handleInputCustodyHandoff(envelope, ctx);
                    return {
                        kind: "bus.ack",
                        requestId: envelope.requestId,
                        ok: true,
                        result,
                    };
                }
                if (envelope.kind !== "leader.offerQueueHandoff") {
                    const sourceReferenceEnabled = envelope.kind === "leader.wakeInputCustody" ||
                        (deps.isSourceReferenceAdmissionEnabled?.() ?? false);
                    if (sourceReferenceEnabled &&
                        !deps.hasAuthenticatedSourceReferenceTransport?.())
                        throw new Error("Telegram follower source-reference admission requires authenticated transport.");
                    const admission = sourceReferenceEnabled
                        ? deps.sourceReferenceAdmission
                        : deps.durableAdmission;
                    if (!admission)
                        throw new Error("Telegram follower source-reference admission is enabled without a wake authority.");
                    const receipt = await admission.admit(envelope, ctx);
                    return {
                        kind: "bus.ack",
                        requestId: envelope.requestId,
                        ok: true,
                        result: receipt,
                    };
                }
                if (!deps.handleQueueHandoff) {
                    throw new Error("Telegram bus receiver cannot accept queue handoff payloads.");
                }
                const result = await deps.handleQueueHandoff(envelope, ctx);
                const receipt = envelope.payload.admissionReceipts[0];
                if (envelope.payload.admissionReceipts.length !== 1 ||
                    !receipt ||
                    result.status !== "staged" ||
                    result.receiptId !== receipt.receiptId ||
                    result.sourceUpdateIds.length !== receipt.sourceUpdateIds.length ||
                    result.sourceUpdateIds.some((updateId, index) => updateId !== receipt.sourceUpdateIds[index])) {
                    throw new Error("Telegram queue handoff staging returned a mismatched receipt.");
                }
                return {
                    kind: "bus.ack",
                    requestId: envelope.requestId,
                    ok: true,
                    result,
                };
            }
            catch (error) {
                deps.recordRuntimeEvent?.("bus", error, { phase: "follower-forward" });
                return {
                    kind: "bus.ack",
                    requestId: envelope.requestId,
                    ok: false,
                    message: error instanceof Error
                        ? error.message
                        : "Telegram bus follower dispatch failed.",
                };
            }
        },
    });
    return server;
}
function parseRegistrationResult(value) {
    if (!isRecord(value))
        return {};
    const target = parseTarget(isRecord(value.target) ? value.target : value);
    return {
        ...(target ? { target } : {}),
        ...(typeof value.slot === "string" ? { slot: value.slot } : {}),
        ...(typeof value.threadName === "string"
            ? { threadName: value.threadName }
            : {}),
        ...(typeof value.displayTitle === "string"
            ? { displayTitle: value.displayTitle }
            : {}),
    };
}
