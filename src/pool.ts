/**
 * A typed concurrency-limited task runner.
 *
 * Workers pull tasks from a FIFO queue, so a slow task never head-of-line
 * blocks the pool and `concurrency` is a hard cap on simultaneous execution.
 * Each task gets per-attempt timeouts (hard deadline, enforced with a race),
 * configurable retries with exponential backoff + jitter, and an AbortSignal
 * so tasks can cooperate with cancellation.
 *
 * Zero runtime dependencies. No Node.js APIs are used, so the pool also
 * runs in browsers and edge runtimes.
 */
import {
  computeDelayMs,
  resolveBackoff,
  sleep,
  type ResolvedBackoff,
} from "./backoff.js";
import { PoolClosedError, TaskAbortedError, TimeoutError } from "./errors.js";
import type {
  PoolOptions,
  PoolStats,
  SettledTask,
  TaskFn,
  TaskMeta,
} from "./types.js";

interface QueueEntry<T> {
  id: number;
  fn: TaskFn<T>;
  resolve: (value: T) => void;
  reject: (reason: unknown) => void;
  settle: (result: SettledTask<T>) => void;
}

export class PromisePool {
  private readonly concurrency: number;
  private readonly timeoutMs?: number;
  private readonly backoff: ResolvedBackoff;
  private readonly retryOn: (error: unknown, attempt: number) => boolean;
  private readonly hooks: Pick<
    PoolOptions,
    "onStart" | "onSuccess" | "onFailure" | "onRetry" | "onDrain"
  >;

  private readonly abortController = new AbortController();
  private readonly queue: Array<QueueEntry<unknown>> = [];
  private running = 0;
  private closed = false;
  private nextId = 1;
  private readonly drainWaiters: Array<() => void> = [];
  private readonly counters = {
    submitted: 0,
    started: 0,
    succeeded: 0,
    failed: 0,
    retried: 0,
    timedOut: 0,
  };

  constructor(options: PoolOptions = {}) {
    const { concurrency = 4, timeoutMs, signal } = options;
    if (!Number.isInteger(concurrency) || concurrency < 1) {
      throw new RangeError("concurrency must be a positive integer");
    }
    if (timeoutMs !== undefined && !(timeoutMs > 0)) {
      throw new RangeError("timeoutMs must be a positive number");
    }
    this.concurrency = concurrency;
    this.timeoutMs = timeoutMs;
    this.backoff = resolveBackoff(options);
    this.retryOn = options.retryOn ?? (() => true);
    this.hooks = {
      onStart: options.onStart,
      onSuccess: options.onSuccess,
      onFailure: options.onFailure,
      onRetry: options.onRetry,
      onDrain: options.onDrain,
    };

    if (signal) {
      if (signal.aborted) {
        this.abortController.abort(
          signal.reason ?? new TaskAbortedError("Pool signal was already aborted"),
        );
      } else {
        signal.addEventListener("abort", () => this.abort(signal.reason), {
          once: true,
        });
      }
    }
  }

  /** Max tasks running at once. */
  get maxConcurrency(): number {
    return this.concurrency;
  }

  /** Tasks waiting for a worker. */
  get pendingCount(): number {
    return this.queue.length;
  }

  /** Tasks currently executing (includes backoff waits between retries). */
  get activeCount(): number {
    return this.running;
  }

  get isClosed(): boolean {
    return this.closed;
  }

  get isAborted(): boolean {
    return this.abortController.signal.aborted;
  }

  /** Point-in-time snapshot of pool statistics. */
  get stats(): PoolStats {
    return {
      ...this.counters,
      pending: this.queue.length,
      running: this.running,
    };
  }

  /**
   * Submit a task. Resolves with the task's return value, or rejects with
   * whatever the task threw (TimeoutError on deadline, TaskAbortedError on
   * abort, PoolClosedError if the pool is closed).
   */
  add<T>(fn: TaskFn<T>): Promise<T> {
    return this.enqueue(fn, () => undefined);
  }

  /**
   * Run `fn` over every item with bounded concurrency. Never throws for
   * task failures: each item's outcome is captured in input order, in the
   * style of Promise.allSettled.
   */
  map<T, R>(
    items: readonly T[],
    fn: (item: T, signal: AbortSignal, meta: TaskMeta) => Promise<R>,
  ): Promise<Array<SettledTask<R>>> {
    if (this.closed) {
      return Promise.reject(new PoolClosedError());
    }
    const results = new Array<SettledTask<R>>(items.length);
    let remaining = items.length;
    if (remaining === 0) {
      return Promise.resolve(results);
    }
    return new Promise<Array<SettledTask<R>>>((resolve) => {
      items.forEach((item, index) => {
        const promise = this.enqueue<R>(
          (signal, meta) => fn(item, signal, meta),
          (settled) => {
            results[index] = settled;
            if (--remaining === 0) {
              resolve(results);
            }
          },
        );
        // Outcomes are consumed through `settle`; silence the twin promise.
        promise.then(undefined, () => undefined);
      });
    });
  }

  /**
   * Stop accepting new tasks and resolve once the queue is empty and every
   * in-flight task has finished.
   */
  close(): Promise<void> {
    this.closed = true;
    return this.drain();
  }

  /** Resolve once the queue is empty and every in-flight task has finished. */
  drain(): Promise<void> {
    if (this.running === 0 && this.queue.length === 0) {
      return Promise.resolve();
    }
    return new Promise<void>((resolve) => {
      this.drainWaiters.push(resolve);
    });
  }

  /**
   * Cancel the pool. Queued tasks reject immediately with a TaskAbortedError.
   * In-flight tasks get their AbortSignal (cooperative: a task that ignores
   * the signal still runs to completion and its `add()` promise settles with
   * the real outcome).
   */
  abort(reason?: unknown): void {
    if (this.abortController.signal.aborted) {
      return;
    }
    const error =
      reason instanceof Error
        ? reason
        : new TaskAbortedError(
            typeof reason === "string" ? reason : "PromisePool aborted",
          );
    this.abortController.abort(error);
    const queued = this.queue.splice(0);
    for (const entry of queued) {
      entry.reject(error);
      entry.settle({ status: "rejected", reason: error, attempts: 0, elapsedMs: 0 });
    }
    this.checkDrain();
  }

  private enqueue<T>(fn: TaskFn<T>, settle: (result: SettledTask<T>) => void): Promise<T> {
    if (this.abortController.signal.aborted) {
      const reason = this.abortController.signal.reason;
      settle({ status: "rejected", reason, attempts: 0, elapsedMs: 0 });
      return Promise.reject(reason);
    }
    if (this.closed) {
      return Promise.reject(new PoolClosedError());
    }
    const id = this.nextId++;
    this.counters.submitted++;
    const entry = new Promise<T>((resolve, reject) => {
      this.queue.push({
        id,
        fn: fn as TaskFn<unknown>,
        resolve: resolve as (value: unknown) => void,
        reject,
        settle: settle as (result: SettledTask<unknown>) => void,
      });
    });
    this.pump();
    return entry;
  }

  /** Start queued tasks while workers are free. */
  private pump(): void {
    while (
      !this.abortController.signal.aborted &&
      this.running < this.concurrency &&
      this.queue.length > 0
    ) {
      const entry = this.queue.shift()!;
      this.running++;
      void this.runEntry(entry).finally(() => {
        this.running--;
        this.pump();
        this.checkDrain();
      });
    }
  }

  private async runEntry(entry: QueueEntry<unknown>): Promise<void> {
    const meta: TaskMeta = { id: entry.id, attempt: 0 };
    const startedAt = Date.now();
    this.counters.started++;
    this.hooks.onStart?.({ ...meta });

    let attempts = 0;
    for (;;) {
      attempts++;
      meta.attempt = attempts;
      try {
        const value = await this.executeOnce(entry, meta);
        const elapsedMs = Date.now() - startedAt;
        this.counters.succeeded++;
        entry.resolve(value);
        entry.settle({ status: "fulfilled", value, attempts, elapsedMs });
        this.hooks.onSuccess?.({ ...meta }, value, elapsedMs);
        return;
      } catch (error) {
        const poolAborted = this.abortController.signal.aborted;
        const canRetry =
          !poolAborted &&
          attempts <= this.backoff.maxRetries &&
          this.retryOn(error, attempts);
        if (canRetry) {
          const delayMs = computeDelayMs(attempts, this.backoff);
          this.counters.retried++;
          this.hooks.onRetry?.({ ...meta }, error, delayMs);
          try {
            await sleep(delayMs, this.abortController.signal);
          } catch (abortReason) {
            // Pool aborted during the backoff wait.
            this.failEntry(entry, meta, attempts, startedAt, abortReason);
            return;
          }
          continue;
        }
        this.failEntry(entry, meta, attempts, startedAt, error);
        return;
      }
    }
  }

  private failEntry(
    entry: QueueEntry<unknown>,
    meta: TaskMeta,
    attempts: number,
    startedAt: number,
    error: unknown,
  ): void {
    const elapsedMs = Date.now() - startedAt;
    this.counters.failed++;
    if (error instanceof TimeoutError) {
      this.counters.timedOut++;
    }
    entry.reject(error);
    entry.settle({ status: "rejected", reason: error, attempts, elapsedMs });
    this.hooks.onFailure?.({ ...meta }, error);
  }

  /**
   * Run one attempt. The timeout is a hard deadline enforced with a race:
   * even a task that ignores its AbortSignal fails with TimeoutError on
   * time, freeing the worker slot.
   */
  private async executeOnce(
    entry: QueueEntry<unknown>,
    meta: TaskMeta,
  ): Promise<unknown> {
    const timeoutMs = this.timeoutMs;
    const ctrl = new AbortController();
    const poolSignal = this.abortController.signal;
    const forwardAbort = () => ctrl.abort(poolSignal.reason);
    if (poolSignal.aborted) {
      ctrl.abort(poolSignal.reason);
    } else {
      poolSignal.addEventListener("abort", forwardAbort, { once: true });
    }

    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      const taskPromise = entry.fn(ctrl.signal, meta);
      if (timeoutMs === undefined) {
        return await taskPromise;
      }
      let rejectOnTimeout!: (error: unknown) => void;
      const timeoutPromise = new Promise<never>((_, reject) => {
        rejectOnTimeout = reject;
      });
      timer = setTimeout(() => {
        const error = new TimeoutError(meta.id, timeoutMs);
        ctrl.abort(error);
        rejectOnTimeout(error);
      }, timeoutMs);
      return await Promise.race([taskPromise, timeoutPromise]);
    } finally {
      if (timer !== undefined) {
        clearTimeout(timer);
      }
      poolSignal.removeEventListener("abort", forwardAbort);
    }
  }

  private checkDrain(): void {
    if (this.running === 0 && this.queue.length === 0) {
      this.hooks.onDrain?.();
      const waiters = this.drainWaiters.splice(0);
      for (const resolve of waiters) {
        resolve();
      }
    }
  }

  /**
   * Run `fn` over every item with bounded concurrency, without managing a
   * pool instance yourself. The pool is closed once all items settle.
   */
  static map<T, R>(
    items: readonly T[],
    fn: (item: T, signal: AbortSignal, meta: TaskMeta) => Promise<R>,
    options: PoolOptions = {},
  ): Promise<Array<SettledTask<R>>> {
    const pool = new PromisePool(options);
    return pool.map(items, fn).finally(() => pool.close());
  }
}
