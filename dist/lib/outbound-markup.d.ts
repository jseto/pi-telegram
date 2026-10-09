/**
 * Telegram outbound markup parsing helpers
 * Zones: telegram outbound, assistant markup
 * Owns top-level assistant action comment extraction, attribute parsing, and markup stripping shared by voice and outbound delivery
 */
export interface TelegramTopLevelHtmlComment {
    raw: string;
    content: string;
    start: number;
    end: number;
}
export declare function collectTopLevelHtmlComments(markdown: string): {
    comments: TelegramTopLevelHtmlComment[];
    openCommentStart?: number;
};
export declare function replaceTelegramButtonFences(markdown: string, replace: (payload: string, closed: boolean) => string): string;
export declare function replaceTopLevelHtmlComments(markdown: string, replacer: (comment: TelegramTopLevelHtmlComment) => string): string;
export declare function parseTelegramButtonPayloadRows(source: string): Record<string, unknown>[][] | undefined;
export declare function parseTelegramActionPayloadRows(comment: TelegramTopLevelHtmlComment, command: string): Record<string, unknown>[][] | undefined;
export declare function stripTelegramCommentMarkupForPreview(markdown: string): string;
export declare function stripTelegramCommentMarkupForDelivery(markdown: string): string;
export interface TelegramVoiceReplyItem {
    text: string;
    lang?: string;
    rate?: string;
}
export interface TelegramVoiceReplyPlan {
    markdown: string;
    voiceText?: string;
    voiceReplies?: TelegramVoiceReplyItem[];
    lang?: string;
    rate?: string;
}
export declare function getTelegramActionString(payload: Record<string, unknown>, key: string): string | undefined;
export declare function planTelegramVoiceReply(markdown: string): TelegramVoiceReplyPlan;
