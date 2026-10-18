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

export function resolveBackoff(opts: {
  maxRetries?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
  factor?: number;
  jitter?: boolean;
}): ResolvedBackoff {
  const merged = {
    maxRetries: 0,
    baseDelayMs: 100,
    maxDelayMs: 5000,
    factor: 2,
    jitter: true,
    ...opts,
  };
  if (!Number.isInteger(merged.maxRetries) || merged.maxRetries < 0) {
    throw new RangeError("maxRetries must be a non-negative integer");
  }
  if (merged.baseDelayMs < 0 || merged.maxDelayMs < 0) {
    throw new RangeError("backoff delays must be non-negative");
  }
  if (merged.factor < 1) {
    throw new RangeError("backoff factor must be >= 1");
  }
  return merged;
}

/** Delay before retry number `attempt` (1-based). `random` is injectable so
 *  the jitter distribution is unit-testable without flaky timing. */
export function computeDelayMs(
  attempt: number,
  backoff: ResolvedBackoff,
  random: () => number = Math.random,
): number {
  const grown = Math.min(
    backoff.baseDelayMs * backoff.factor ** (attempt - 1),
    backoff.maxDelayMs,
  );
  const capped = Math.max(0, grown);
  return backoff.jitter ? Math.floor(random() * (capped + 1)) : Math.floor(capped);
}

/** Sleep that wakes early (rejecting with `onAbort`) if the signal fires. */
export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(signal.reason);
      return;
    }
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(timer);
      reject(signal!.reason);
    };
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}
