/**
 * Telegram Thread naming rules and manual-name interaction
 * Zones: thread identity names, palettes, title formatting, runtime controls
 * Owns name/title value policy and one expiring exact-target dialog per session scope.
 * Excludes occupancy/slot allocation, display-mode projection, transport and API/store effects.
 */

import {
  areTelegramTargetsEqual as sameTarget,
  type TelegramTarget,
} from "./target.ts";

export const TELEGRAM_THREAD_NAME_DIALOG_TTL_MS = 5 * 60_000;

export type TelegramThreadNameDialogAction = "reset" | "cancel";

export interface TelegramThreadNameDialogCandidate {
  scope: string;
  target: TelegramTarget;
  dialogMessageId: number;
  phase: "input";
  expiresAtMs: number;
}

function targetKey(target: TelegramTarget): string {
  return `${target.chatId}:${target.threadId ?? "chat"}`;
}

function cloneCandidate(
  candidate: TelegramThreadNameDialogCandidate,
): TelegramThreadNameDialogCandidate {
  return { ...candidate, target: { ...candidate.target } };
}

export interface TelegramThreadNameDialogLifetime {
  /** Independent recipient effect authority; never the input handle's deadline or ownership. */
  readonly assertAuthority?: () => void;
  isCurrent(): boolean;
  publish(
    dialogMessageId: number,
    assertPublicationCurrent?: () => void,
  ): TelegramThreadNameDialogCandidate | undefined;
  select(action: TelegramThreadNameDialogAction): {
    kind: "reset" | "cancel" | "expired";
  };
  consumeName(
    text: string,
  ): { kind: "name"; name: string } | { kind: "none" | "empty" };
  reopen(): TelegramThreadNameDialogCandidate | undefined;
  finish(): void;
}

export function createTelegramThreadNameDialogRuntime(options?: {
  ttlMs?: number;
  nowMs?: () => number;
}): {
  prepare(input: {
    scope: string;
    target: TelegramTarget;
    isCurrent?: () => boolean;
    assertAuthority?: () => void;
  }): TelegramThreadNameDialogLifetime | undefined;
  capture(input: {
    scope: string;
    target: TelegramTarget;
    dialogMessageId: number;
  }): TelegramThreadNameDialogLifetime | undefined;
  open(input: {
    scope: string;
    target: TelegramTarget;
    dialogMessageId: number;
  }): TelegramThreadNameDialogCandidate;
  select(input: {
    scope: string;
    target: TelegramTarget;
    dialogMessageId: number;
    action: TelegramThreadNameDialogAction;
  }): { kind: "reset" | "cancel" | "expired" };
  consumeName(input: {
    scope: string;
    target: TelegramTarget;
    text: string;
  }): { kind: "name"; name: string } | { kind: "none" | "empty" };
  clearScope(scope: string): void;
  inspect(
    target: TelegramTarget,
  ): TelegramThreadNameDialogCandidate | undefined;
} {
  const ttlMs = options?.ttlMs ?? TELEGRAM_THREAD_NAME_DIALOG_TTL_MS;
  const nowMs = options?.nowMs ?? Date.now;
  interface Entry {
    scope: string;
    target: TelegramTarget;
    dialogMessageId?: number;
    phase: "delivery" | "input" | "consumed";
    expiresAtMs: number;
    isCurrent?: () => boolean;
    assertAuthority?: () => void;
    consumedBy?: object;
  }
  const candidates = new Map<string, Entry>();
  const recipientCurrent = (entry: Entry): boolean => {
    try {
      return entry.isCurrent ? entry.isCurrent() === true : true;
    } catch {
      return false;
    }
  };
  const view = (entry: Entry): TelegramThreadNameDialogCandidate =>
    cloneCandidate({
      scope: entry.scope,
      target: entry.target,
      dialogMessageId: entry.dialogMessageId!,
      phase: "input",
      expiresAtMs: entry.expiresAtMs,
    });
  const createLifetime = (entry: Entry): TelegramThreadNameDialogLifetime => {
    const key = targetKey(entry.target);
    const claim = {};
    let ended = false;
    const finish = () => {
      ended = true;
      if (
        candidates.get(key) === entry &&
        (!entry.consumedBy || entry.consumedBy === claim)
      )
        candidates.delete(key);
    };
    const isCurrent = () => {
      if (
        ended ||
        candidates.get(key) !== entry ||
        (entry.consumedBy && entry.consumedBy !== claim)
      ) {
        ended = true;
        return false;
      }
      const current = entry.expiresAtMs > nowMs() && recipientCurrent(entry);
      // Supplied authority/clock observations may synchronously replace this dialog.
      if (
        !current ||
        entry.expiresAtMs <= nowMs() ||
        candidates.get(key) !== entry ||
        (entry.consumedBy && entry.consumedBy !== claim)
      ) {
        finish();
        return false;
      }
      return true;
    };
    return {
      assertAuthority: entry.assertAuthority,
      isCurrent,
      publish(dialogMessageId, assertPublicationCurrent) {
        if (!isCurrent() || entry.phase !== "delivery") return undefined;
        // Source authority is needed for this publication only, never retained by future input/effects.
        if (assertPublicationCurrent) {
          assertPublicationCurrent();
          if (entry.expiresAtMs <= nowMs()) return undefined;
          assertPublicationCurrent();
          if (
            ended ||
            candidates.get(key) !== entry ||
            entry.phase !== "delivery"
          )
            return undefined;
        }
        if (!Number.isSafeInteger(dialogMessageId) || dialogMessageId <= 0) {
          finish();
          return undefined;
        }
        entry.dialogMessageId = dialogMessageId;
        entry.phase = "input";
        return view(entry);
      },
      select(action) {
        if (!isCurrent() || entry.phase !== "input") return { kind: "expired" };
        if (action === "cancel") finish();
        else {
          entry.phase = "consumed";
          entry.consumedBy = claim;
        }
        return { kind: action };
      },
      consumeName(text) {
        if (!isCurrent() || entry.phase !== "input") return { kind: "none" };
        const name = text.trim();
        if (!name) return { kind: "empty" };
        entry.phase = "consumed";
        entry.consumedBy = claim;
        return { kind: "name", name };
      },
      reopen() {
        if (
          !isCurrent() ||
          entry.phase !== "consumed" ||
          entry.consumedBy !== claim
        ) {
          return undefined;
        }
        // A failed effect may restore input, never renew its deadline or issued handle.
        const replacement: Entry = {
          ...entry,
          phase: "input",
          consumedBy: undefined,
        };
        candidates.set(key, replacement);
        ended = true;
        return view(replacement);
      },
      finish,
    };
  };
  const current = (
    scope: string,
    target: TelegramTarget,
  ): Entry | undefined => {
    const entry = candidates.get(targetKey(target));
    if (
      !entry ||
      entry.scope !== scope ||
      entry.phase !== "input" ||
      !sameTarget(entry.target, target) ||
      !createLifetime(entry).isCurrent()
    )
      return undefined;
    return entry;
  };
  const capture = (input: {
    scope: string;
    target: TelegramTarget;
    dialogMessageId: number;
  }): TelegramThreadNameDialogLifetime | undefined => {
    const { scope, dialogMessageId } = input;
    const target = { ...input.target };
    const entry = current(scope, target);
    if (!entry || entry.dialogMessageId !== dialogMessageId) return undefined;
    return createLifetime(entry);
  };
  return {
    prepare(input) {
      const scope = input.scope;
      const target = { ...input.target };
      const isCurrent = input.isCurrent,
        assertAuthority = input.assertAuthority;
      const key = targetKey(target);
      const previous = candidates.get(key);
      const entry: Entry = {
        scope,
        target,
        phase: "delivery",
        expiresAtMs: nowMs() + ttlMs,
        isCurrent,
        assertAuthority,
      };
      if (
        !recipientCurrent(entry) ||
        entry.expiresAtMs <= nowMs() ||
        candidates.get(key) !== previous
      )
        return undefined;
      candidates.set(key, entry);
      return createLifetime(entry);
    },
    capture,
    open(input) {
      const entry: Entry = {
        scope: input.scope,
        target: { ...input.target },
        phase: "input",
        dialogMessageId: input.dialogMessageId,
        expiresAtMs: nowMs() + ttlMs,
      };
      candidates.set(targetKey(entry.target), entry);
      return view(entry);
    },
    select(input) {
      const action = input.action;
      const lifetime = capture(input);
      if (!lifetime) return { kind: "expired" };
      const result = lifetime.select(action);
      lifetime.finish();
      return result;
    },
    consumeName(input) {
      const text = input.text;
      const entry = current(input.scope, input.target);
      if (!entry) return { kind: "none" };
      const lifetime = createLifetime(entry);
      const result = lifetime.consumeName(text);
      if (result.kind === "name") lifetime.finish();
      return result;
    },
    clearScope(scope) {
      for (const [key, entry] of candidates) {
        if (entry.scope === scope) candidates.delete(key);
      }
    },
    inspect(target) {
      const key = targetKey(target);
      const entry = candidates.get(key);
      if (!entry) return undefined;
      if (entry.expiresAtMs <= nowMs()) {
        if (candidates.get(key) === entry) candidates.delete(key);
        return undefined;
      }
      if (entry.phase !== "input" || !createLifetime(entry).isCurrent())
        return undefined;
      return view(entry);
    },
  };
}

export interface TelegramThreadTitleInput {
  instanceId: string;
  profileKey: string;
  threadName?: string;
}

export function normalizeTelegramTopicTargetThreadName(
  threadName: string,
): string {
  return threadName.replace(/\s+/g, " ").trim().slice(0, 96);
}

function getGraphemeSegments(value: string): string[] {
  const segmenter = (
    Intl as unknown as {
      Segmenter?: new (
        locale?: string,
        options?: { granularity: "grapheme" },
      ) => { segment(input: string): Iterable<{ segment: string }> };
    }
  ).Segmenter;
  if (!segmenter) return Array.from(value);
  return Array.from(
    new segmenter(undefined, { granularity: "grapheme" }).segment(value),
    (part) => part.segment,
  );
}

export function getTelegramTopicIdentityName(threadName: string): string {
  return getGraphemeSegments(normalizeTelegramTopicTargetThreadName(threadName))
    .join("")
    .trim();
}

const TELEGRAM_THREAD_NAME_PALETTE: Record<string, readonly string[]> = {
  A: ["Atlas", "Aster", "Aurora", "Anchor", "Ashen"],
  B: ["Beacon", "Briar", "Boreal", "Birch", "Bison"],
  C: ["Cedar", "Comet", "Cipher", "Coral", "Cinder"],
  D: ["Delta", "Dawn", "Drift", "Dune", "Dagger"],
  E: ["Ember", "Echo", "Eagle", "Eden", "Elder"],
  F: ["Falcon", "Fjord", "Flint", "Forest", "Fable"],
  G: ["Grove", "Glade", "Glyph", "Garnet", "Gale"],
  H: ["Harbor", "Hawk", "Hazel", "Helix", "Haven"],
  I: ["Iris", "Ivory", "Iron", "Isle", "Idea"],
  J: ["Jade", "Juno", "Jolt", "Jewel", "Jasper"],
  K: ["Kite", "Karma", "Kernel", "Kodiak", "Kelp"],
  L: ["Lumen", "Laurel", "Lynx", "Lotus", "Lagoon"],
  M: ["Maple", "Meteor", "Meadow", "Marble", "Moss"],
  N: ["Nimbus", "Nova", "Nectar", "North", "Noble"],
  O: ["Orion", "Onyx", "Opal", "Orbit", "Olive"],
  P: ["Pine", "Pulse", "Praxis", "Pebble", "Prism"],
  Q: ["Quartz", "Quill", "Quasar", "Quest", "Quiver"],
  R: ["River", "Raven", "Rune", "Reef", "Ridge"],
  S: ["Spruce", "Solar", "Signal", "Stone", "Sable"],
  T: ["Timber", "Talon", "Terra", "Torch", "Tide"],
  U: ["Umber", "Unity", "Ursa", "Uplink", "Ulmus"],
  V: ["Violet", "Vector", "Vista", "Vale", "Vortex"],
  W: ["Willow", "Warden", "Wave", "Winter", "Wisp"],
  X: ["Xenon", "Xylem", "Xavier", "Xylo", "Xerus"],
  Y: ["Yarrow", "Yonder", "Yukon", "Yale", "Yogi"],
  Z: ["Zenith", "Zephyr", "Zircon", "Zebra", "Zion"],
};

export function chooseTelegramThreadName(input: {
  slot: string | undefined;
  entropy?: number | string;
  getRandom?: () => number;
  occupied?: readonly string[];
}): string | undefined {
  if (!input.slot || !/^[A-Z]$/.test(input.slot)) return undefined;
  const names = TELEGRAM_THREAD_NAME_PALETTE[input.slot];
  if (!names || names.length === 0) return undefined;
  const occupied = new Set(
    (input.occupied ?? []).map((name) => getTelegramTopicIdentityName(name)),
  );
  const start = input.getRandom
    ? Math.max(
        0,
        Math.min(
          names.length - 1,
          Math.floor(input.getRandom() * names.length),
        ),
      )
    : getTelegramThreadNameEntropyIndex(input.entropy, names.length);
  for (let offset = 0; offset < names.length; offset += 1) {
    const name = names[(start + offset) % names.length];
    if (!occupied.has(getTelegramTopicIdentityName(name))) return name;
  }
  for (const paletteSlot of "ABCDEFGHIJKLMNOPQRSTUVWXYZ") {
    for (const name of TELEGRAM_THREAD_NAME_PALETTE[paletteSlot] ?? []) {
      if (!occupied.has(getTelegramTopicIdentityName(name))) return name;
    }
  }
  return undefined;
}

export function getTelegramThreadNameLeadingSlot(
  threadName: string | undefined,
): string | undefined {
  if (!threadName) return undefined;
  const first = getTelegramTopicIdentityName(threadName)[0];
  return first && /^[A-Z]$/.test(first) ? first : undefined;
}

function getTelegramThreadNameEntropyIndex(
  entropy: number | string | undefined,
  length: number,
): number {
  if (length <= 1) return 0;
  if (typeof entropy === "number" && entropy < 1_000_000_000_000) return 0;
  const value = entropy === undefined ? "0" : String(entropy);
  let hash = 2166136261;
  for (const char of value) {
    hash ^= char.codePointAt(0) ?? 0;
    hash = Math.imul(hash, 16777619) >>> 0;
  }
  return hash % length;
}

export function getTelegramTopicThreadNameValidationError(
  threadName: string,
  _slot: string | undefined,
): string | undefined {
  const identity = getTelegramTopicIdentityName(threadName);
  const reasons: string[] = [];
  if (!identity) reasons.push("it is empty after trimming");
  if (/\s/.test(identity)) reasons.push("it contains spaces");
  if (/[^A-Za-z]/.test(identity)) {
    reasons.push("it contains characters outside Latin A-Z letters");
  }
  if (!/^[A-Z]/.test(identity)) {
    reasons.push("it does not start with an uppercase Latin letter");
  }
  const genericLabels = new Set(["telegram", "leader", "follower"]);
  if (genericLabels.has(identity.toLowerCase())) {
    reasons.push("it is a generic role label");
  }
  if (/^[A-Z]$/.test(identity)) reasons.push("it is only a bare slot letter");
  if (reasons.length === 0) return undefined;
  return `Invalid Telegram instance name: ${reasons.join("; ")}. Use exactly one capitalized Latin word with no spaces, punctuation, emoji, non-Latin letters, or digits; it must not be a generic role label or only a bare slot letter.`;
}

export function getTelegramManualThreadDisplayNameValidationError(
  threadName: string,
): string | undefined {
  const trimmed = threadName.trim();
  const normalized = trimmed.replace(/\s+/g, " ");
  const reasons: string[] = [];
  if (!trimmed) reasons.push("it is empty after trimming");
  if (trimmed && /[^\x20-\x7E]/.test(trimmed)) {
    reasons.push("it contains characters outside printable ASCII");
  }
  if (normalized.length > 96) reasons.push("it is longer than 96 characters");
  if (/^[A-Z]$/.test(normalized)) {
    reasons.push("a bare slot letter is reserved for reset to automatic");
  }
  if (reasons.length === 0) return undefined;
  return `Invalid Telegram Thread display name: ${reasons.join("; ")}. Use 1–96 printable ASCII characters.`;
}

export function isTelegramTopicThreadNameValidForSlot(
  threadName: string,
  slot: string | undefined,
): boolean {
  return !getTelegramTopicThreadNameValidationError(threadName, slot);
}

function applyTopicNameTemplate(
  template: string,
  request: TelegramThreadTitleInput,
  slot?: string,
): string {
  const threadName =
    request.threadName?.replace(/\s+/g, " ").trim() || request.profileKey;
  let result = template
    .replaceAll("{threadName}", threadName)
    .replaceAll("{profileKey}", request.profileKey)
    .replaceAll("{instanceId}", request.instanceId);
  if (slot) result = result.replaceAll("{slot}", slot);
  return result;
}

export function getTelegramTopicName(
  request: TelegramThreadTitleInput,
  template = "{slot}",
  slot?: string,
): string {
  const name = applyTopicNameTemplate(template, request, slot)
    .replace(/\s+/g, " ")
    .trim();
  return (name || slot || "Pi").slice(0, 128);
}

export function getTelegramTopicTitleForThreadName(
  threadName: string,
  slot: string,
  template = "{threadName}",
): string {
  return getTelegramTopicName(
    {
      instanceId: "",
      profileKey: normalizeTelegramTopicTargetThreadName(threadName) || "Pi",
      threadName,
    },
    template,
    slot,
  );
}
