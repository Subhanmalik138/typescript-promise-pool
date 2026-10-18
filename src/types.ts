/**
 * Public type surface for the promise pool.
 */

/** A unit of work. Receives an AbortSignal (fires on timeout / pool abort)
 *  and metadata about which task and attempt this is. */
export type TaskFn<T> = (signal: AbortSignal, meta: TaskMeta) => Promise<T>;

export interface TaskMeta {
  /** 1-based id assigned when the task was submitted. */
  id: number;
  /** 1-based attempt number for this execution. */
  attempt: number;
}

export interface BackoffOptions {
  /** Retries after the first attempt. Defaults to 0 (no retries). */
  maxRetries?: number;
  /** Delay before the first retry. Defaults to 100ms. */
  baseDelayMs?: number;
  /** Upper bound for any single retry delay. Defaults to 5000ms. */
  maxDelayMs?: number;
  /** Multiply the delay per attempt. Defaults to 2. */
  factor?: number;
  /** Randomize each delay in [0, computed] (full jitter). Defaults to true. */
  jitter?: boolean;
  /** Decide whether a failure is retryable. Defaults to retrying everything
   *  except aborts. Receives the error and the 1-based attempt that failed. */
  retryOn?: (error: unknown, attempt: number) => boolean;
}

export interface PoolOptions extends BackoffOptions {
  /** Max tasks running at once. Must be a positive integer. Defaults to 4. */
  concurrency?: number;
  /** Hard per-task deadline. A task that exceeds it fails with TimeoutError,
   *  even if it ignores its AbortSignal. No default (no timeout). */
  timeoutMs?: number;
  /** Pool-level cancellation. Aborting rejects queued tasks immediately and
   *  signals in-flight tasks (cooperative for tasks that ignore the signal). */
  signal?: AbortSignal;
  onStart?: (meta: TaskMeta) => void;
  onSuccess?: (meta: TaskMeta, value: unknown, elapsedMs: number) => void;
  onFailure?: (meta: TaskMeta, error: unknown) => void;
  onRetry?: (meta: TaskMeta, error: unknown, delayMs: number) => void;
  onDrain?: () => void;
}

export interface PoolStats {
  submitted: number;
  started: number;
  succeeded: number;
  failed: number;
  /** Total retry attempts (not tasks that retried). */
  retried: number;
  timedOut: number;
  pending: number;
  running: number;
}

/** One task's outcome, in the style of Promise.allSettled plus extras. */
export interface SettledTask<T> {
  status: "fulfilled" | "rejected";
  value?: T;
  reason?: unknown;
  /** How many attempts this task took (1 when it succeeded first try). */
  attempts: number;
  elapsedMs: number;
}
