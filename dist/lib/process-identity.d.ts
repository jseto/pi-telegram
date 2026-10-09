/**
 * Process identity and liveness proofs
 * Zones: shared utils, multi-instance bus, durable admission
 * Owns PID liveness probes and stable Linux/macOS/Windows process-birth identity. Only an absent PID or a
 * mismatched birth proof establishes death; inaccessible metadata stays unverifiable.
 */
export declare function isProcessAlive(pid: number): boolean;
interface TelegramProcessBirthIdentityOptions {
    platform?: NodeJS.Platform;
    readProcStat?: (pid: number) => string;
    readDarwinProcessStart?: (pid: number) => string;
    readWindowsProcessStart?: (pid: number) => string;
}
export type TelegramProcessLiveness = "alive" | "dead" | "unverifiable";
interface TelegramProcessLivenessOptions extends TelegramProcessBirthIdentityOptions {
    isProcessAlive?: (pid: number) => boolean;
}
export declare function getTelegramProcessBirthIdentity(pid: number, fallbackGeneration: number | string, options?: TelegramProcessBirthIdentityOptions): string;
export declare function getTelegramProcessLiveness(owner: {
    processId: number;
    processBirthId: string;
}, options?: TelegramProcessLivenessOptions): TelegramProcessLiveness;
export declare function getTelegramProcessBirthIdentityLiveness(processBirthId: string, options?: TelegramProcessLivenessOptions): TelegramProcessLiveness;
export {};
