/**
 * Errors raised by the pool itself. Task functions may throw anything;
 * these are the only errors the pool generates.
 */
/** A task exceeded its per-task deadline. The AbortSignal handed to the
 *  task fires with this error as its reason at the same moment. */
export declare class TimeoutError extends Error {
    readonly taskId: number;
    readonly timeoutMs: number;
    constructor(taskId: number, timeoutMs: number);
}
/** add() was called after close(). */
export declare class PoolClosedError extends Error {
    constructor(message?: string);
}
/** A task was cancelled because the pool (or caller) aborted. */
export declare class TaskAbortedError extends Error {
    constructor(message?: string);
}
