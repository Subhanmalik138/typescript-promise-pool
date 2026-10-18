import type { PoolOptions, PoolStats, SettledTask, TaskFn, TaskMeta } from "./types.js";
export declare class PromisePool {
    private readonly concurrency;
    private readonly timeoutMs?;
    private readonly backoff;
    private readonly retryOn;
    private readonly hooks;
    private readonly abortController;
    private readonly queue;
    private running;
    private closed;
    private nextId;
    private readonly drainWaiters;
    private readonly counters;
    constructor(options?: PoolOptions);
    /** Max tasks running at once. */
    get maxConcurrency(): number;
    /** Tasks waiting for a worker. */
    get pendingCount(): number;
    /** Tasks currently executing (includes backoff waits between retries). */
    get activeCount(): number;
    get isClosed(): boolean;
    get isAborted(): boolean;
    /** Point-in-time snapshot of pool statistics. */
    get stats(): PoolStats;
    /**
     * Submit a task. Resolves with the task's return value, or rejects with
     * whatever the task threw (TimeoutError on deadline, TaskAbortedError on
     * abort, PoolClosedError if the pool is closed).
     */
    add<T>(fn: TaskFn<T>): Promise<T>;
    /**
     * Run `fn` over every item with bounded concurrency. Never throws for
     * task failures: each item's outcome is captured in input order, in the
     * style of Promise.allSettled.
     */
    map<T, R>(items: readonly T[], fn: (item: T, signal: AbortSignal, meta: TaskMeta) => Promise<R>): Promise<Array<SettledTask<R>>>;
    /**
     * Stop accepting new tasks and resolve once the queue is empty and every
     * in-flight task has finished.
     */
    close(): Promise<void>;
    /** Resolve once the queue is empty and every in-flight task has finished. */
    drain(): Promise<void>;
    /**
     * Cancel the pool. Queued tasks reject immediately with a TaskAbortedError.
     * In-flight tasks get their AbortSignal (cooperative: a task that ignores
     * the signal still runs to completion and its `add()` promise settles with
     * the real outcome).
     */
    abort(reason?: unknown): void;
    private enqueue;
    /** Start queued tasks while workers are free. */
    private pump;
    private runEntry;
    private failEntry;
    /**
     * Run one attempt. The timeout is a hard deadline enforced with a race:
     * even a task that ignores its AbortSignal fails with TimeoutError on
     * time, freeing the worker slot.
     */
    private executeOnce;
    private checkDrain;
    /**
     * Run `fn` over every item with bounded concurrency, without managing a
     * pool instance yourself. The pool is closed once all items settle.
     */
    static map<T, R>(items: readonly T[], fn: (item: T, signal: AbortSignal, meta: TaskMeta) => Promise<R>, options?: PoolOptions): Promise<Array<SettledTask<R>>>;
}
