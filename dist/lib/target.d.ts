/**
 * Telegram transport destination value helpers
 * Zones: Bot API transport, routing, replies/previews, ownership, multi-instance bus
 * Owns the minimal `{ chatId, threadId? }` address shape shared by classic private chats
 * and Telegram UI threads mapped through Bot API `message_thread_id`.
 */
export interface TelegramTarget {
    chatId: number;
    threadId?: number;
}
export declare function createTelegramPrivateTarget(chatId: number): TelegramTarget;
export declare function createTelegramThreadTarget(chatId: number, threadId: number): TelegramTarget;
export declare function getTelegramTargetKey(target: TelegramTarget): string;
export declare function isTelegramThreadTarget(target: TelegramTarget): target is TelegramTarget & {
    threadId: number;
};
export declare function areTelegramTargetsEqual(left: TelegramTarget, right: TelegramTarget): boolean;
export declare function getTelegramTargetThreadParams(target: TelegramTarget): {
    message_thread_id?: number;
};
/** Pure wire parser for `{ chatId, threadId? }`; unknown fields are dropped and malformed shapes refuse. */
export declare function parseTelegramTarget(value: unknown): TelegramTarget | undefined;
/** Pure Bot API integer id parser accepting integer numbers or non-blank integer strings. */
export declare function parseTelegramIntegerId(value: unknown): number | undefined;
