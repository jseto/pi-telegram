/**
 * Telegram thinking menu UI helpers
 * Zones: telegram ui, thinking controls, menu composition
 * Owns thinking-menu text, reply markup, callback handling, and thinking-menu message rendering
 */
import { type TelegramMenuMessageRuntimeDeps, type TelegramMenuRenderPayload, type TelegramModelMenuState, type TelegramReplyMarkup } from "./menu-model.ts";
import { type MenuModel, type ThinkingLevel } from "./model.ts";
export interface TelegramThinkingMenuCallbackDeps {
    setThinkingLevel: (level: ThinkingLevel) => void;
    getCurrentThinkingLevel: () => ThinkingLevel;
    updateThinkingMenuMessage: () => Promise<void>;
    answerCallbackQuery: (callbackQueryId: string, text?: string) => Promise<void>;
    isVoiceReplyActive?: () => boolean;
}
export interface TelegramThinkingMenuOpenDeps<TModel extends MenuModel = MenuModel> extends TelegramMenuMessageRuntimeDeps {
    getModelMenuState: () => Promise<TelegramModelMenuState<TModel>>;
    getActiveModel: () => TModel | undefined;
    getThinkingLevel: () => ThinkingLevel;
    storeModelMenuState: (state: TelegramModelMenuState<TModel>) => void;
    isVoiceReplyActive?: () => boolean;
}
/** Answers and returns true when voice replies or a non-reasoning model disable thinking controls. */
export declare function refuseUnavailableTelegramThinkingControls(callbackQueryId: string, activeModel: MenuModel | undefined, deps: {
    answerCallbackQuery: (callbackQueryId: string, text?: string) => Promise<void>;
    isVoiceReplyActive?: () => boolean;
}): Promise<boolean>;
export declare function handleTelegramThinkingMenuCallbackAction(callbackQueryId: string, data: string | undefined, activeModel: MenuModel | undefined, deps: TelegramThinkingMenuCallbackDeps): Promise<boolean>;
export declare function buildThinkingMenuText(): string;
export declare function buildThinkingMenuReplyMarkup(currentThinkingLevel: ThinkingLevel): TelegramReplyMarkup;
export declare function buildTelegramThinkingMenuRenderPayload(_activeModel: MenuModel | undefined, currentThinkingLevel: ThinkingLevel): TelegramMenuRenderPayload;
export declare function openTelegramThinkingMenu<TModel extends MenuModel = MenuModel>(deps: TelegramThinkingMenuOpenDeps<TModel>): Promise<void>;
export declare function updateTelegramThinkingMenuMessage(state: TelegramModelMenuState, activeModel: MenuModel | undefined, currentThinkingLevel: ThinkingLevel, deps: TelegramMenuMessageRuntimeDeps & {
    isVoiceReplyActive?: () => boolean;
}): Promise<void>;
