/**
 * Errors raised by the pool itself. Task functions may throw anything;
 * these are the only errors the pool generates.
 */

/** A task exceeded its per-task deadline. The AbortSignal handed to the
 *  task fires with this error as its reason at the same moment. */
export class TimeoutError extends Error {
  readonly taskId: number;
  readonly timeoutMs: number;

  constructor(taskId: number, timeoutMs: number) {
    super(`Task ${taskId} timed out after ${timeoutMs}ms`);
    this.name = "TimeoutError";
    this.taskId = taskId;
    this.timeoutMs = timeoutMs;
  }
}

/** add() was called after close(). */
export class PoolClosedError extends Error {
  constructor(message = "PromisePool is closed: no new tasks accepted") {
    super(message);
    this.name = "PoolClosedError";
  }
}

/** A task was cancelled because the pool (or caller) aborted. */
export class TaskAbortedError extends Error {
  constructor(message = "Task aborted") {
    super(message);
    this.name = "TaskAbortedError";
  }
}
