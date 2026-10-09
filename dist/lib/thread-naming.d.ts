/**
 * Telegram Thread naming rules and manual-name interaction
 * Zones: thread identity names, palettes, title formatting, runtime controls
 * Owns name/title value policy and one expiring exact-target dialog per session scope.
 * Excludes occupancy/slot allocation, display-mode projection, transport and API/store effects.
 */
import { type TelegramTarget } from "./target.ts";
export declare const TELEGRAM_THREAD_NAME_DIALOG_TTL_MS: number;
export type TelegramThreadNameDialogAction = "reset" | "cancel";
export interface TelegramThreadNameDialogCandidate {
    scope: string;
    target: TelegramTarget;
    dialogMessageId: number;
    phase: "input";
    expiresAtMs: number;
}
export interface TelegramThreadNameDialogLifetime {
    /** Independent recipient effect authority; never the input handle's deadline or ownership. */
    readonly assertAuthority?: () => void;
    isCurrent(): boolean;
    publish(dialogMessageId: number, assertPublicationCurrent?: () => void): TelegramThreadNameDialogCandidate | undefined;
    select(action: TelegramThreadNameDialogAction): {
        kind: "reset" | "cancel" | "expired";
    };
    consumeName(text: string): {
        kind: "name";
        name: string;
    } | {
        kind: "none" | "empty";
    };
    reopen(): TelegramThreadNameDialogCandidate | undefined;
    finish(): void;
}
export declare function createTelegramThreadNameDialogRuntime(options?: {
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
    }): {
        kind: "reset" | "cancel" | "expired";
    };
    consumeName(input: {
        scope: string;
        target: TelegramTarget;
        text: string;
    }): {
        kind: "name";
        name: string;
    } | {
        kind: "none" | "empty";
    };
    clearScope(scope: string): void;
    inspect(target: TelegramTarget): TelegramThreadNameDialogCandidate | undefined;
};
export interface TelegramThreadTitleInput {
    instanceId: string;
    profileKey: string;
    threadName?: string;
}
export declare function normalizeTelegramTopicTargetThreadName(threadName: string): string;
export declare function getTelegramTopicIdentityName(threadName: string): string;
export declare function chooseTelegramThreadName(input: {
    slot: string | undefined;
    entropy?: number | string;
    getRandom?: () => number;
    occupied?: readonly string[];
}): string | undefined;
export declare function getTelegramThreadNameLeadingSlot(threadName: string | undefined): string | undefined;
export declare function getTelegramTopicThreadNameValidationError(threadName: string, _slot: string | undefined): string | undefined;
export declare function getTelegramManualThreadDisplayNameValidationError(threadName: string): string | undefined;
export declare function isTelegramTopicThreadNameValidForSlot(threadName: string, slot: string | undefined): boolean;
export declare function getTelegramTopicName(request: TelegramThreadTitleInput, template?: string, slot?: string): string;
export declare function getTelegramTopicTitleForThreadName(threadName: string, slot: string, template?: string): string;
