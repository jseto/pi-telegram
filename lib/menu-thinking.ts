/**
 * Telegram thinking menu UI helpers
 * Zones: telegram ui, thinking controls, menu composition
 * Owns thinking-menu text, reply markup, callback handling, and thinking-menu message rendering
 */

import {
  editTelegramMenuMessage,
  sendTelegramMenuMessage,
  type TelegramMenuMessageRuntimeDeps,
  type TelegramMenuRenderPayload,
  type TelegramModelMenuState,
  type TelegramReplyMarkup,
} from "./menu-model.ts";
import {
  isThinkingLevel,
  type MenuModel,
  THINKING_LEVELS,
  type ThinkingLevel,
} from "./model.ts";

export interface TelegramThinkingMenuCallbackDeps {
  setThinkingLevel: (level: ThinkingLevel) => void;
  getCurrentThinkingLevel: () => ThinkingLevel;
  updateThinkingMenuMessage: () => Promise<void>;
  answerCallbackQuery: (
    callbackQueryId: string,
    text?: string,
  ) => Promise<void>;
  isVoiceReplyActive?: () => boolean;
}

export interface TelegramThinkingMenuOpenDeps<
  TModel extends MenuModel = MenuModel,
> extends TelegramMenuMessageRuntimeDeps {
  getModelMenuState: () => Promise<TelegramModelMenuState<TModel>>;
  getActiveModel: () => TModel | undefined;
  getThinkingLevel: () => ThinkingLevel;
  storeModelMenuState: (state: TelegramModelMenuState<TModel>) => void;
  isVoiceReplyActive?: () => boolean;
}

function parseTelegramThinkingMenuCallbackAction(
  data: string | undefined,
): { kind: "thinking:set"; level: string } | undefined {
  if (!data?.startsWith("thinking:set:")) return undefined;
  return { kind: "thinking:set", level: data.slice("thinking:set:".length) };
}

/** Answers and returns true when voice replies or a non-reasoning model disable thinking controls. */
export async function refuseUnavailableTelegramThinkingControls(
  callbackQueryId: string,
  activeModel: MenuModel | undefined,
  deps: {
    answerCallbackQuery: (
      callbackQueryId: string,
      text?: string,
    ) => Promise<void>;
    isVoiceReplyActive?: () => boolean;
  },
): Promise<boolean> {
  if (deps.isVoiceReplyActive?.()) {
    await deps.answerCallbackQuery(
      callbackQueryId,
      "Thinking controls are disabled during voice replies",
    );
    return true;
  }
  if (!activeModel?.reasoning) {
    await deps.answerCallbackQuery(
      callbackQueryId,
      "This model has no reasoning controls",
    );
    return true;
  }
  return false;
}

export async function handleTelegramThinkingMenuCallbackAction(
  callbackQueryId: string,
  data: string | undefined,
  activeModel: MenuModel | undefined,
  deps: TelegramThinkingMenuCallbackDeps,
): Promise<boolean> {
  const action = parseTelegramThinkingMenuCallbackAction(data);
  if (!action) return false;
  if (!isThinkingLevel(action.level)) {
    await deps.answerCallbackQuery(callbackQueryId, "Invalid thinking level");
    return true;
  }
  if (
    await refuseUnavailableTelegramThinkingControls(
      callbackQueryId,
      activeModel,
      deps,
    )
  )
    return true;
  deps.setThinkingLevel(action.level);
  await deps.updateThinkingMenuMessage();
  await deps.answerCallbackQuery(
    callbackQueryId,
    `Thinking: ${deps.getCurrentThinkingLevel()}`,
  );
  return true;
}

export function buildThinkingMenuText(): string {
  return "<b>🧠 Choose a thinking level:</b>";
}

export function buildThinkingMenuReplyMarkup(
  currentThinkingLevel: ThinkingLevel,
): TelegramReplyMarkup {
  const rows = [[{ text: "⬆️ Main menu", callback_data: "menu:back" }]];
  const levelButtons = THINKING_LEVELS.map((level) => ({
    text: level === currentThinkingLevel ? `🟢 ${level}` : level,
    callback_data: `thinking:set:${level}`,
  }));
  rows.push(
    levelButtons.slice(0, 1),
    levelButtons.slice(1, 4),
    levelButtons.slice(4, 7),
  );
  return { inline_keyboard: rows };
}

export function buildTelegramThinkingMenuRenderPayload(
  _activeModel: MenuModel | undefined,
  currentThinkingLevel: ThinkingLevel,
): TelegramMenuRenderPayload {
  return {
    nextMode: "thinking",
    text: buildThinkingMenuText(),
    mode: "html",
    replyMarkup: buildThinkingMenuReplyMarkup(currentThinkingLevel),
  };
}

export async function openTelegramThinkingMenu<
  TModel extends MenuModel = MenuModel,
>(deps: TelegramThinkingMenuOpenDeps<TModel>): Promise<void> {
  if (deps.isVoiceReplyActive?.()) return;
  const state = await deps.getModelMenuState();
  const messageId = await sendTelegramMenuMessage(
    state,
    buildTelegramThinkingMenuRenderPayload(
      deps.getActiveModel(),
      deps.getThinkingLevel(),
    ),
    deps,
  );
  if (messageId === undefined) return;
  state.messageId = messageId;
  state.mode = "thinking";
  deps.storeModelMenuState(state);
}

export async function updateTelegramThinkingMenuMessage(
  state: TelegramModelMenuState,
  activeModel: MenuModel | undefined,
  currentThinkingLevel: ThinkingLevel,
  deps: TelegramMenuMessageRuntimeDeps & { isVoiceReplyActive?: () => boolean },
): Promise<void> {
  if (deps.isVoiceReplyActive?.()) return;
  await editTelegramMenuMessage(
    state,
    buildTelegramThinkingMenuRenderPayload(activeModel, currentThinkingLevel),
    deps,
  );
}
