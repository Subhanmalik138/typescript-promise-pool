/**
 * Exponential backoff with full jitter.
 *
 * Retries wait a random delay in [0, min(base * factor^(attempt-1), max)].
 * The exponential growth keeps hammering a failing dependency unlikely,
 * while the jitter spreads retries out in time so many failing tasks don't
 * all retry in lockstep (thundering herd).
 */
export interface ResolvedBackoff {
    maxRetries: number;
    baseDelayMs: number;
    maxDelayMs: number;
    factor: number;
    jitter: boolean;
}
export declare function resolveBackoff(opts: {
    maxRetries?: number;
    baseDelayMs?: number;
    maxDelayMs?: number;
    factor?: number;
    jitter?: boolean;
}): ResolvedBackoff;
/** Delay before retry number `attempt` (1-based). `random` is injectable so
 *  the jitter distribution is unit-testable without flaky timing. */
export declare function computeDelayMs(attempt: number, backoff: ResolvedBackoff, random?: () => number): number;
/** Sleep that wakes early (rejecting with `onAbort`) if the signal fires. */
export declare function sleep(ms: number, signal?: AbortSignal): Promise<void>;
