/**
 * Telegram voice reply policy and provider registries
 * Zones: telegram, voice, pi agent prompts
 * Owns: voice reply modes (manual / mirror / always), voice turn flags and preview suppression,
 * voice prompt contributions, and the process-global synthesis/transcription provider registries.
 * Excludes: voice markup parsing (outbound-markup), audio generation and Telegram delivery
 * (outbound, outbound-voice), queue, preview lifecycle and turn building.
 */

const VOICE_SYNTHESIS_PROVIDER_REGISTRY_KEY =
  "__piTelegramVoiceSynthesisProviders__";
const VOICE_TRANSCRIPTION_PROVIDER_REGISTRY_KEY =
  "__piTelegramVoiceTranscriptionProviders__";

let nextGeneratedVoiceSynthesisProviderId = 0;
let nextGeneratedVoiceTranscriptionProviderId = 0;

function getNextAvailableProviderId<T>(
  registry: Map<string, T>,
  prefix: string,
  nextId: () => number,
): string {
  let id: string;
  do {
    id = `${prefix}-${nextId()}`;
  } while (registry.has(id));
  return id;
}

export type TelegramVoiceReplyMode = "manual" | "mirror" | "always";

export type TelegramVoiceSynthesisProviderResult = string | undefined;

export interface TelegramVoiceTurnView {
  voiceReplyPreferred?: boolean;
  voiceReplyRequired?: boolean;
  hasVoiceInput?: boolean;
  userText?: string;
}

export interface TelegramVoiceSynthesisProvider {
  (
    text: string,
    options?: { lang?: string; rate?: string },
  ): Promise<TelegramVoiceSynthesisProviderResult>;
  getVoicePolicy?: () => { replyMode?: TelegramVoiceReplyMode };
  getVoicePromptContribution?: (
    view: TelegramVoiceTurnView,
  ) => string | undefined;
}

export type TelegramVoiceTranscriptionProviderResult =
  string | { text: string; language?: string } | undefined;

export interface TelegramVoiceTranscriptionFile {
  path: string;
  fileName?: string;
  mimeType?: string;
  kind?: string;
}

export interface TelegramVoiceTranscriptionProvider {
  (
    file: TelegramVoiceTranscriptionFile,
    options?: { language?: string },
  ): Promise<TelegramVoiceTranscriptionProviderResult>;
}

// --- Voice Synthesis Provider Registry ---

function getOrCreateVoiceSynthesisProviderRegistry(): Map<
  string,
  TelegramVoiceSynthesisProvider
> {
  const existing = (globalThis as Record<string, unknown>)[
    VOICE_SYNTHESIS_PROVIDER_REGISTRY_KEY
  ];
  if (existing instanceof Map)
    return existing as Map<string, TelegramVoiceSynthesisProvider>;
  const registry = new Map<string, TelegramVoiceSynthesisProvider>();
  (globalThis as Record<string, unknown>)[
    VOICE_SYNTHESIS_PROVIDER_REGISTRY_KEY
  ] = registry;
  return registry;
}

/**
 * Register a high-level Telegram voice synthesis provider.
 *
 * Stable public API callers must pass a stable `options.id` so diagnostics,
 * replacement, and cleanup can identify the provider. Omitted ids remain a
 * compatibility path for pre-matrix callers and receive generated session-local
 * ids.
 */
export function registerTelegramVoiceSynthesisProvider(
  provider:
    | TelegramVoiceSynthesisProvider
    | ((
        text: string,
        options?: { lang?: string; rate?: string },
      ) => Promise<TelegramVoiceSynthesisProviderResult>),
  options?: { id?: string },
): () => void {
  const registry = getOrCreateVoiceSynthesisProviderRegistry();
  const id =
    options?.id ??
    getNextAvailableProviderId(
      registry,
      "voice-synthesis-provider",
      () => nextGeneratedVoiceSynthesisProviderId++,
    );
  const normalized =
    typeof provider === "function"
      ? (Object.assign(
          (text: string, options?: { lang?: string; rate?: string }) =>
            provider(text, options),
          {
            getVoicePolicy: (provider as TelegramVoiceSynthesisProvider)
              .getVoicePolicy,
            getVoicePromptContribution: (
              provider as TelegramVoiceSynthesisProvider
            ).getVoicePromptContribution,
          },
        ) as TelegramVoiceSynthesisProvider)
      : provider;
  registry.set(id, normalized);
  return () => {
    if (registry.get(id) === normalized) registry.delete(id);
  };
}

export function getTelegramVoiceSynthesisProviders(): TelegramVoiceSynthesisProvider[] {
  return Array.from(getOrCreateVoiceSynthesisProviderRegistry().values());
}

export function hasTelegramVoiceSynthesisProvider(): boolean {
  return getOrCreateVoiceSynthesisProviderRegistry().size > 0;
}

export function clearTelegramVoiceSynthesisProviders(): void {
  getOrCreateVoiceSynthesisProviderRegistry().clear();
}

function getOrCreateVoiceTranscriptionProviderRegistry(): Map<
  string,
  TelegramVoiceTranscriptionProvider
> {
  const existing = (globalThis as Record<string, unknown>)[
    VOICE_TRANSCRIPTION_PROVIDER_REGISTRY_KEY
  ];
  if (existing instanceof Map) {
    return existing as Map<string, TelegramVoiceTranscriptionProvider>;
  }
  const registry = new Map<string, TelegramVoiceTranscriptionProvider>();
  (globalThis as Record<string, unknown>)[
    VOICE_TRANSCRIPTION_PROVIDER_REGISTRY_KEY
  ] = registry;
  return registry;
}

/**
 * Register a high-level Telegram voice transcription provider.
 *
 * Stable public API callers must pass a stable `options.id`. Omitted ids remain
 * a compatibility path for pre-matrix callers and receive generated
 * session-local ids.
 */
export function registerTelegramVoiceTranscriptionProvider(
  provider: TelegramVoiceTranscriptionProvider,
  options?: { id?: string },
): () => void {
  const registry = getOrCreateVoiceTranscriptionProviderRegistry();
  const id =
    options?.id ??
    getNextAvailableProviderId(
      registry,
      "voice-transcription-provider",
      () => nextGeneratedVoiceTranscriptionProviderId++,
    );
  registry.set(id, provider);
  return () => {
    if (registry.get(id) === provider) registry.delete(id);
  };
}

export function getTelegramVoiceTranscriptionProviders(): TelegramVoiceTranscriptionProvider[] {
  return Array.from(getOrCreateVoiceTranscriptionProviderRegistry().values());
}

export function hasTelegramVoiceTranscriptionProvider(): boolean {
  return getOrCreateVoiceTranscriptionProviderRegistry().size > 0;
}

export function clearTelegramVoiceTranscriptionProviders(): void {
  getOrCreateVoiceTranscriptionProviderRegistry().clear();
}

// --- Voice Reply Modes ---

export const TELEGRAM_VOICE_REPLY_MODES = [
  "manual",
  "mirror",
  "always",
] as const;

/**
 * Returns the active voice reply mode for the current session.
 *
 * Pi-telegram owns reply-mode policy through telegram.json. If
 * config.voice.replyMode is missing, invalid, or legacy `hidden`, the effective
 * mode is manual.
 */
export function getTelegramVoiceReplyMode(config?: {
  voice?: { replyMode?: string };
}): TelegramVoiceReplyMode {
  const configMode = config?.voice?.replyMode;
  if (configMode === "mirror" || configMode === "always") return configMode;
  return "manual";
}

// --- Voice Turn Helpers ---

/** Small helper to compute the two voice flags from mode + hasVoiceFile */
export function computeVoiceTurnFlags(
  voiceReplyMode: TelegramVoiceReplyMode | undefined,
  hasVoiceFile: boolean,
) {
  return {
    voiceReplyPreferred: hasVoiceFile && voiceReplyMode === "mirror",
    voiceReplyRequired: voiceReplyMode === "always",
  };
}

/** Returns true if the given turn is tagged as a voice turn */
export function isVoiceTurn(
  turn:
    | { voiceReplyPreferred?: boolean; voiceReplyRequired?: boolean }
    | null
    | undefined,
): boolean {
  return !!(turn?.voiceReplyPreferred || turn?.voiceReplyRequired);
}

// --- Voice Prompt Contribution ---

export function computeVoicePromptContribution(
  voiceReplyMode: TelegramVoiceReplyMode | undefined,
  files: Array<{ kind?: string }>,
  rawText: string,
): string | undefined {
  const hasVoiceFile = files.some(
    (f) => f.kind === "voice" || f.kind === "audio",
  );

  const isVoiceTagged =
    voiceReplyMode === "always" ||
    (voiceReplyMode === "mirror" && hasVoiceFile);

  if (!isVoiceTagged) return undefined;

  const view: TelegramVoiceTurnView = {
    ...computeVoiceTurnFlags(voiceReplyMode, hasVoiceFile),
    hasVoiceInput: hasVoiceFile,
    userText: rawText,
  };

  // Let the voice synthesis provider supply additional instructions for the LLM when in voice mode.
  // When multiple providers are registered, the first one (in registration order)
  // that returns a non-empty string wins.
  for (const provider of getTelegramVoiceSynthesisProviders()) {
    if (typeof provider.getVoicePromptContribution === "function") {
      const contribution = provider.getVoicePromptContribution(view);
      if (contribution?.trim()) {
        return contribution.trim();
      }
    }
  }

  return undefined;
}

// --- Preview Suppression ---

/**
 * Returns true if the current turn should not show a text preview
 * (e.g. because it's a voice reply).
 */
export const shouldSuppressPreviewForVoice = isVoiceTurn;
