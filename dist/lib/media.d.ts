/**
 * Telegram media and text extraction helpers
 * Zones: telegram inbound, media groups, filesystem paths
 * Normalizes inbound Telegram messages into reusable file, text, id, history, and media-group metadata
 */
export interface TelegramPhotoSize {
    file_id: string;
    file_size?: number;
}
export interface TelegramDocument {
    file_id: string;
    file_name?: string;
    mime_type?: string;
}
export type TelegramVideo = TelegramDocument;
export type TelegramAudio = TelegramDocument;
export type TelegramAnimation = TelegramDocument;
export interface TelegramVoice {
    file_id: string;
    mime_type?: string;
}
export interface TelegramRichMessage {
    blocks?: unknown[];
}
export interface TelegramMessageUser {
    id?: number;
    is_bot?: boolean;
    first_name?: string;
    last_name?: string;
    username?: string;
}
export interface TelegramMessageForwardOrigin {
    type?: string;
    sender_user?: TelegramMessageUser;
    sender_user_name?: string;
    sender_chat?: {
        title?: string;
        username?: string;
        id?: number;
    };
    chat?: {
        title?: string;
        username?: string;
        id?: number;
    };
    author_signature?: string;
}
export interface TelegramReplyToMessage {
    message_id?: number;
    from?: TelegramMessageUser;
    text?: string;
    caption?: string;
    rich_message?: TelegramRichMessage;
    photo?: TelegramPhotoSize[];
    document?: TelegramDocument;
    video?: TelegramVideo;
    audio?: TelegramAudio;
    voice?: TelegramVoice;
    animation?: TelegramAnimation;
    sticker?: TelegramSticker;
}
export interface TelegramSticker {
    file_id: string;
    is_video?: boolean;
    is_animated?: boolean;
}
export interface TelegramMediaMessage {
    message_id: number;
    chat?: {
        id: number;
        type?: string;
        username?: string;
    };
    from?: TelegramMessageUser;
    forward_origin?: TelegramMessageForwardOrigin;
    forward_from?: TelegramMessageUser;
    forward_sender_name?: string;
    text?: string;
    caption?: string;
    rich_message?: TelegramRichMessage;
    reply_to_message?: TelegramReplyToMessage;
    media_group_id?: string;
    photo?: TelegramPhotoSize[];
    document?: TelegramDocument;
    video?: TelegramVideo;
    audio?: TelegramAudio;
    voice?: TelegramVoice;
    animation?: TelegramAnimation;
    sticker?: TelegramSticker;
}
export interface TelegramMediaGroupMessage {
    message_id: number;
    chat: {
        id: number;
    };
    message_thread_id?: number;
    media_group_id?: string;
}
export interface TelegramMediaGroupState<TMessage, TContext = unknown> {
    messages: TMessage[];
    context?: TContext;
    flushTimer?: ReturnType<typeof setTimeout>;
    dispatching?: boolean;
    dispatchPromise?: Promise<void>;
    dispatchNow?: () => Promise<void>;
    suspended?: boolean;
    reschedule?: () => void;
}
export interface TelegramMediaGroupController<TMessage extends TelegramMediaGroupMessage, TContext = unknown> {
    queueMessage: (options: {
        message: TMessage;
        context?: TContext;
        dispatchMessages: (messages: TMessage[], ctx?: TContext) => unknown | Promise<unknown>;
    }) => boolean;
    removeMessages: (messageIds: number[]) => number;
    flushMessage: (messageId: number) => Promise<boolean>;
    suspend: () => void;
    resume: (context: TContext) => void;
    clear: () => void;
}
export interface TelegramMediaGroupDispatchRuntimeDeps<TMessage extends TelegramMediaGroupMessage, TContext> {
    mediaGroups: TelegramMediaGroupController<TMessage, TContext>;
    dispatchMessages: (messages: TMessage[], ctx: TContext) => Promise<void>;
    onDeferredMessage?: (message: TMessage) => void;
}
export interface TelegramMediaGroupDispatchRuntime<TMessage extends TelegramMediaGroupMessage, TContext> {
    handleMessage: (message: TMessage, ctx: TContext) => Promise<void>;
}
export interface TelegramMediaGroupControllerOptions {
    debounceMs?: number;
    setTimer?: (callback: () => void, ms: number) => ReturnType<typeof setTimeout>;
    clearTimer?: (timer: ReturnType<typeof setTimeout>) => void;
}
export type TelegramAttachmentKind = "photo" | "document" | "video" | "audio" | "voice" | "animation" | "sticker";
/** Where an inbound attachment came from; the download layer turns it into a scoped file name. */
export interface TelegramAttachmentSource {
    kind: string;
    messageId: number;
    index?: number;
    /** Sender-chosen file name, kept as a trailing readable segment. */
    userFileName?: string;
    chat?: {
        id: number;
        type?: string;
        username?: string;
    };
    /** Explicit source scope, e.g. a Guest Mode peer whose message IDs are not the bot chat's. */
    scope?: string;
}
export interface TelegramFileInfo {
    file_id: string;
    fileName: string;
    /** Present only when the sender chose the name. */
    userFileName?: string;
    /** Position inside a rich message. */
    index?: number;
    source?: TelegramAttachmentSource;
    mimeType?: string;
    kind: TelegramAttachmentKind;
    isImage: boolean;
}
export interface DownloadedTelegramFile {
    path: string;
    fileName?: string;
    isImage?: boolean;
    mimeType?: string;
    kind?: TelegramAttachmentKind;
}
export interface DownloadedTelegramMessageFile {
    path: string;
    fileName: string;
    isImage: boolean;
    mimeType?: string;
    kind?: TelegramAttachmentKind;
}
export interface DownloadTelegramMessageFilesDeps {
    downloadFile: (fileId: string, fileName: string, source?: TelegramAttachmentSource) => Promise<string>;
}
export declare function guessMediaType(path: string): string | undefined;
export declare function extractTelegramMessageText(message: TelegramMediaMessage): string;
export declare function extractTelegramForwardContextText(message: TelegramMediaMessage, allowedUserId?: number): string;
export declare function buildTelegramReplyContextBlock(message: TelegramMediaMessage, replyFiles?: Pick<DownloadedTelegramFile, "path">[], replyOutputs?: readonly string[]): string;
export declare function extractTelegramMessagesText(messages: TelegramMediaMessage[]): string;
export declare function extractTelegramMessagesPromptText(messages: TelegramMediaMessage[]): string;
export declare function extractFirstTelegramMessageText(messages: TelegramMediaMessage[]): string;
export declare function hasTelegramMessagesPromptContent(messages: TelegramMediaMessage[]): boolean;
export declare function collectTelegramMessageIds(messages: TelegramMediaMessage[]): number[];
export declare function getTelegramMediaGroupKey(message: TelegramMediaGroupMessage): string | undefined;
export declare function removePendingTelegramMediaGroupMessages<TMessage extends TelegramMediaGroupMessage>(groups: Map<string, TelegramMediaGroupState<TMessage, unknown>>, messageIds: number[], clearTimer: (timer: ReturnType<typeof setTimeout>) => void): number;
export declare function queueTelegramMediaGroupMessage<TMessage extends TelegramMediaGroupMessage, TContext = unknown>(options: {
    message: TMessage;
    context?: TContext;
    groups: Map<string, TelegramMediaGroupState<TMessage, TContext>>;
    debounceMs: number;
    setTimer: (callback: () => void, ms: number) => ReturnType<typeof setTimeout>;
    clearTimer: (timer: ReturnType<typeof setTimeout>) => void;
    dispatchMessages: (messages: TMessage[], ctx?: TContext) => unknown | Promise<unknown>;
}): boolean;
/** Timer and dispatch lifecycle shared by delayed media-group and text-group coalescing. */
export interface TelegramPendingGroupState<TMessage, TContext> {
    messages: TMessage[];
    context?: TContext;
    flushTimer?: ReturnType<typeof setTimeout>;
    dispatching?: boolean;
    suspended?: boolean;
    reschedule?: () => void;
    dispatchNow?: () => Promise<void>;
}
export interface TelegramPendingGroupLifecycle<TContext> {
    flushMessage: (messageId: number) => Promise<boolean>;
    suspend: () => void;
    resume: (context: TContext) => void;
    clear: () => void;
}
export declare function createTelegramPendingGroupLifecycle<TMessage extends {
    message_id: number;
}, TContext>(groups: Map<string, TelegramPendingGroupState<TMessage, TContext>>, clearTimer: (timer: ReturnType<typeof setTimeout>) => void): TelegramPendingGroupLifecycle<TContext>;
export declare function createTelegramMediaGroupController<TMessage extends TelegramMediaGroupMessage, TContext = unknown>(options?: TelegramMediaGroupControllerOptions): TelegramMediaGroupController<TMessage, TContext>;
export declare function createTelegramMediaGroupDispatchRuntime<TMessage extends TelegramMediaGroupMessage, TContext>(deps: TelegramMediaGroupDispatchRuntimeDeps<TMessage, TContext>): TelegramMediaGroupDispatchRuntime<TMessage, TContext>;
/** Pure `[title]` bullet section appended after a blank line; empty items leave text unchanged. */
export declare function appendTelegramListSection(text: string, title: string, items: string[]): string;
export declare function appendTelegramAttachmentSection(text: string, files: Pick<DownloadedTelegramFile, "path">[]): string;
export declare function formatTelegramHistoryText(rawText: string, files: DownloadedTelegramFile[], handlerOutputs?: string[]): string;
export declare function downloadTelegramMessageFiles(messages: TelegramMediaMessage[], deps: DownloadTelegramMessageFilesDeps): Promise<DownloadedTelegramMessageFile[]>;
export declare function collectTelegramFileInfos(messages: TelegramMediaMessage[]): TelegramFileInfo[];
