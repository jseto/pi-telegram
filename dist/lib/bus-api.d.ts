/**
 * Telegram bus-aware API runtime
 * Zones: multi-instance bus, telegram api transport, live instance routing
 * Wraps the direct Telegram Bot API runtime so follower instances can route outbound calls through the bus leader
 */
import { type TelegramBusSelectedMenuDeliveryObservation, type TelegramBusSelectedMenuTextEffect } from "./bus.ts";
import type { TelegramBridgeApiRuntime } from "./telegram-api.ts";
export type TelegramBusApiCall = (method: string, args: unknown[]) => Promise<unknown>;
export interface TelegramBusAwareApiRuntimeDeps {
    directRuntime: TelegramBridgeApiRuntime;
    ownsDirect: () => boolean;
    callFollowerApi: TelegramBusApiCall;
    getDefaultTarget?: () => {
        chatId: number;
        threadId?: number;
    } | undefined;
}
/** Bound selected-menu projection only; no ordinary/raw API fallback and no activation during preparation. */
export declare function createTelegramSelectedMenuTextApi(deps: {
    operationId: string;
    registrationGeneration: string;
    target: {
        chatId: number;
        threadId: number;
    };
    assertAuthority: () => void;
    deliver(effect: TelegramBusSelectedMenuTextEffect, assertAuthority: () => void): Promise<TelegramBusSelectedMenuDeliveryObservation>;
}): Pick<TelegramBridgeApiRuntime, "sendMessage" | "editMessageText">;
export declare function createTelegramBusAwareApiRuntime(deps: TelegramBusAwareApiRuntimeDeps): TelegramBridgeApiRuntime;
