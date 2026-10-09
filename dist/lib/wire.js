/**
 * Non-coercing decoded wire-value inspection
 * Zones: wire fields, record shape, integer bounds
 * Owns shared shallow predicates; excludes schemas, lossless decoding,
 * required fields, normalization, storage and execution authority.
 */
/** Non-array object shape only; not a plain-object or JSON-serializability guarantee. */
export function isWireRecord(value) {
    return !!value && typeof value === "object" && !Array.isArray(value);
}
/** Checks only own enumerable string keys, not required, inherited or symbol fields. */
export function hasOnlyWireKeys(value, allowedKeys) {
    const allowed = new Set(allowedKeys);
    return Object.keys(value).every((key) => allowed.has(key));
}
export function isNonEmptyWireString(value) {
    return typeof value === "string" && value.length > 0;
}
export function isNonNegativeWireInteger(value) {
    return Number.isSafeInteger(value) && value >= 0;
}
