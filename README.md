# promise-pool

A tiny, typed, zero-dependency concurrency limiter for async work. Submit tasks, cap how many run at once, and get per-task timeouts, retries with backoff, and progress hooks out of the box.

```ts
import { PromisePool } from "typescript-promise-pool";

// Fan out over 500 URLs, at most 8 requests in flight.
const results = await PromisePool.map(
  urls,
  async (url, signal) => {
    const res = await fetch(url, { signal });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return res.json();
  },
  { concurrency: 8, timeoutMs: 10_000, maxRetries: 2 },
);

for (const r of results) {
  if (r.status === "fulfilled") save(r.value);
  else console.error(`failed after ${r.attempts} attempts:`, r.reason);
}
```

## Why this exists

`Promise.all` fires everything at once — fine for five calls, a self-inflicted DDoS for five thousand. Hand-rolled worker loops tend to get timeouts, retries, and shutdown wrong. This is the piece I kept rewriting, so I packaged it once, properly.

## Features

- **Hard concurrency cap** — workers pull from a FIFO queue, so a slow task never blocks the tasks behind it.
- **Hard per-task timeouts** — enforced with a race, so even a task that ignores its `AbortSignal` fails on time and frees its worker slot.
- **Retries with exponential backoff + full jitter**, plus a `retryOn` predicate for non-retryable errors.
- **Cooperative cancellation** — abort the pool (or pass your own `AbortSignal`); queued tasks reject immediately and in-flight tasks get the signal.
- **`PromisePool.map`** — `Promise.allSettled` semantics over any array, results in input order.
- **Observability** — `onStart` / `onSuccess` / `onFailure` / `onRetry` / `onDrain` hooks and a live `stats` snapshot.
- **Zero runtime dependencies**, no Node.js APIs — runs in Node ≥ 18, browsers, and edge runtimes.

## Install

```bash
npm install typescript-promise-pool
```

## Usage

### Long-lived pool

```ts
const pool = new PromisePool({
  concurrency: 4,
  timeoutMs: 5_000,
  maxRetries: 3,
  baseDelayMs: 200,
  onRetry: (meta, err, delayMs) =>
    console.warn(`task ${meta.id} failed (attempt ${meta.attempt}), retrying in ${delayMs}ms`),
});

const user = await pool.add(async (signal) => fetchUser(id, signal));

// When you're done submitting:
await pool.close(); // waits for in-flight tasks, rejects further add() calls
```

### One-shot map

```ts
const results = await PromisePool.map(items, fn, {
  concurrency: 8,
  maxRetries: 2,
});
```

### Cancellation

```ts
const controller = new AbortController();
const pool = new PromisePool({ concurrency: 4, signal: controller.signal });

// ... later:
controller.abort(); // or pool.abort("shutting down")
```

## API

### `new PromisePool(options?)`

| Option | Default | Description |
|---|---|---|
| `concurrency` | `4` | Max tasks running at once (positive integer). |
| `timeoutMs` | — | Hard per-task deadline; exceeding it fails the task with `TimeoutError`. |
| `maxRetries` | `0` | Retries after the first attempt. |
| `baseDelayMs` | `100` | Delay before the first retry. |
| `maxDelayMs` | `5000` | Cap for any single retry delay. |
| `factor` | `2` | Backoff multiplier per attempt. |
| `jitter` | `true` | Randomize each delay in `[0, computed]` (full jitter). |
| `retryOn` | retry all | `(error, attempt) => boolean` — return `false` for non-retryable errors. Never retried after the pool aborts. |
| `signal` | — | External `AbortSignal` that aborts the whole pool. |
| `onStart` / `onSuccess` / `onFailure` / `onRetry` / `onDrain` | — | Lifecycle hooks. |

### Methods

- `add(fn): Promise<T>` — submit one task; resolves/rejects with the task's outcome.
- `map(items, fn): Promise<SettledTask<T>[]>` — run over an array, one result per item in input order.
- `drain(): Promise<void>` — resolve when the queue is empty and all tasks finished.
- `close(): Promise<void>` — stop accepting tasks, then drain.
- `abort(reason?)` — cancel queued tasks now, signal in-flight ones.
- `stats` — `{ submitted, started, succeeded, failed, retried, timedOut, pending, running }`.

### Errors

- `TimeoutError` — task exceeded `timeoutMs` (also delivered as the task signal's `reason`).
- `PoolClosedError` — `add()`/`map()` after `close()`.
- `TaskAbortedError` — task cancelled by pool abort.

## Design decisions

1. **Pull-based workers, not batch slicing.** Each worker takes the next task from a shared FIFO queue when it frees up. This keeps the concurrency cap exact under mixed task durations and preserves submission order for scheduling fairness — chunking with `Promise.all` can't do either.
2. **Timeouts are a race, not just a signal.** Many libraries only abort the signal and hope the task cooperates. Here the deadline also rejects the attempt directly, so a hung task can't leak its worker slot forever. The signal still fires (with the `TimeoutError` as its reason) so well-behaved tasks can clean up.
3. **Backoff is pure and injectable.** Delay math lives in `backoff.ts` with the random source as a parameter, so the jitter distribution is unit-tested deterministically instead of with flaky sleeps.
4. **Cancellation never retries.** A retry loop that keeps firing after the caller gave up is a classic bug; the pool checks the abort flag before every retry and wakes mid-backoff when aborted.

## Development

```bash
npm install
npm test        # vitest
npm run build   # tsc -> dist/
npm run lint    # tsc --noEmit
```

Node ≥ 18 required. Tests: 18 cases covering the concurrency cap, FIFO ordering, hard timeouts, retry/backoff behavior, map ordering, abort/close lifecycle, and backoff math.

## License

MIT
