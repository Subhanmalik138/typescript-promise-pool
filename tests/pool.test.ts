import { describe, expect, it } from "vitest";
import {
  PromisePool,
  TimeoutError,
  PoolClosedError,
  TaskAbortedError,
  computeDelayMs,
  resolveBackoff,
} from "../src/index.js";

const tick = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

describe("concurrency", () => {
  it("caps simultaneous tasks at the configured limit", async () => {
    const pool = new PromisePool({ concurrency: 3 });
    let active = 0;
    let maxActive = 0;
    const tasks = Array.from({ length: 10 }, (_, i) =>
      pool.add(async () => {
        active++;
        maxActive = Math.max(maxActive, active);
        await tick(15);
        active--;
        return i;
      }),
    );
    const results = await Promise.all(tasks);
    expect(results).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    expect(maxActive).toBe(3);
    await pool.close();
  });

  it("starts tasks in FIFO order", async () => {
    const pool = new PromisePool({ concurrency: 1 });
    const started: number[] = [];
    const mk = (id: number) => async () => {
      started.push(id);
    };
    await Promise.all([pool.add(mk(1)), pool.add(mk(2)), pool.add(mk(3))]);
    expect(started).toEqual([1, 2, 3]);
    await pool.close();
  });

  it("rejects invalid concurrency", () => {
    expect(() => new PromisePool({ concurrency: 0 })).toThrow(RangeError);
    expect(() => new PromisePool({ concurrency: 1.5 })).toThrow(RangeError);
    expect(() => new PromisePool({ concurrency: -2 })).toThrow(RangeError);
  });
});

describe("timeouts", () => {
  it("fails a slow task with TimeoutError and aborts its signal", async () => {
    const pool = new PromisePool({ timeoutMs: 40 });
    let seen: AbortSignal | undefined;
    await expect(
      pool.add(async (signal) => {
        seen = signal;
        await tick(500);
        return 1;
      }),
    ).rejects.toBeInstanceOf(TimeoutError);
    expect(seen!.aborted).toBe(true);
    expect(seen!.reason).toBeInstanceOf(TimeoutError);
    expect(pool.stats.timedOut).toBe(1);
    await pool.close();
  });

  it("enforces a hard deadline even when the task ignores its signal", async () => {
    const pool = new PromisePool({ timeoutMs: 30 });
    const start = Date.now();
    await expect(pool.add(() => new Promise<string>(() => {}))).rejects.toBeInstanceOf(
      TimeoutError,
    );
    // The worker slot is freed; the pool drains instead of hanging forever.
    expect(Date.now() - start).toBeLessThan(1000);
    await pool.drain();
  });

  it("rejects non-positive timeouts", () => {
    expect(() => new PromisePool({ timeoutMs: 0 })).toThrow(RangeError);
    expect(() => new PromisePool({ timeoutMs: -10 })).toThrow(RangeError);
  });
});

describe("retries", () => {
  it("retries with exponential backoff until the task succeeds", async () => {
    const delays: number[] = [];
    let calls = 0;
    const pool = new PromisePool({
      maxRetries: 3,
      baseDelayMs: 20,
      jitter: false,
      onRetry: (_meta, _err, delayMs) => delays.push(delayMs),
    });
    const value = await pool.add(async () => {
      calls++;
      if (calls < 3) throw new Error("flaky");
      return "ok";
    });
    expect(value).toBe("ok");
    expect(calls).toBe(3);
    expect(delays).toEqual([20, 40]);
    expect(pool.stats.retried).toBe(2);
    expect(pool.stats.succeeded).toBe(1);
    await pool.close();
  });

  it("gives up after maxRetries and reports the attempt count", async () => {
    let lastAttempt = 0;
    const pool = new PromisePool({
      maxRetries: 2,
      baseDelayMs: 5,
      jitter: false,
      onFailure: (meta) => {
        lastAttempt = meta.attempt;
      },
    });
    await expect(
      pool.add(async () => {
        throw new Error("boom");
      }),
    ).rejects.toThrow("boom");
    expect(lastAttempt).toBe(3); // 1 initial + 2 retries
    expect(pool.stats.failed).toBe(1);
    await pool.close();
  });

  it("honours a custom retryOn predicate", async () => {
    class FatalError extends Error {}
    let calls = 0;
    const pool = new PromisePool({
      maxRetries: 5,
      baseDelayMs: 5,
      retryOn: (err) => !(err instanceof FatalError),
    });
    await expect(
      pool.add(async () => {
        calls++;
        throw new FatalError("do not retry me");
      }),
    ).rejects.toBeInstanceOf(FatalError);
    expect(calls).toBe(1);
    await pool.close();
  });

  it("rejects invalid retry options", () => {
    expect(() => new PromisePool({ maxRetries: -1 })).toThrow(RangeError);
    expect(() => new PromisePool({ baseDelayMs: -1 })).toThrow(RangeError);
  });
});

describe("map", () => {
  it("preserves input order and captures failures per item", async () => {
    const results = await PromisePool.map(
      [1, 2, 3, 4],
      async (n) => {
        await tick((5 - n) * 10); // finish in reverse order on purpose
        if (n === 3) throw new Error("bad 3");
        return n * 10;
      },
      { concurrency: 2 },
    );
    expect(results.map((r) => r.status)).toEqual([
      "fulfilled",
      "fulfilled",
      "rejected",
      "fulfilled",
    ]);
    expect(results[0].value).toBe(10);
    expect(results[3].value).toBe(40);
    expect((results[2].reason as Error).message).toBe("bad 3");
    expect(results.every((r) => r.attempts === 1)).toBe(true);
  });

  it("resolves an empty input immediately", async () => {
    await expect(PromisePool.map([], async (x: number) => x)).resolves.toEqual([]);
  });
});

describe("lifecycle", () => {
  it("close() waits for in-flight work then rejects new tasks", async () => {
    const pool = new PromisePool({ concurrency: 1 });
    let finished = false;
    const p = pool.add(async () => {
      await tick(30);
      finished = true;
      return "done";
    });
    const closed = pool.close();
    await expect(p).resolves.toBe("done");
    await closed;
    expect(finished).toBe(true);
    await expect(pool.add(async () => 1)).rejects.toBeInstanceOf(PoolClosedError);
  });

  it("abort() rejects queued tasks and signals the running one", async () => {
    const pool = new PromisePool({ concurrency: 1 });
    let release!: () => void;
    const gate = new Promise<void>((r) => {
      release = r;
    });
    let runningSignal: AbortSignal | undefined;
    const p1 = pool.add(async (signal) => {
      runningSignal = signal;
      await gate;
      return "first";
    });
    const p2 = pool.add(async () => "second");
    const p3 = pool.add(async () => "third");
    p2.catch(() => undefined);
    p3.catch(() => undefined);

    pool.abort();
    await expect(p2).rejects.toBeInstanceOf(TaskAbortedError);
    await expect(p3).rejects.toBeInstanceOf(TaskAbortedError);
    expect(runningSignal!.aborted).toBe(true);

    release(); // the in-flight task cooperates and finishes normally
    await expect(p1).resolves.toBe("first");
    await pool.drain();
  });

  it("a pre-aborted signal rejects tasks immediately", async () => {
    const ctrl = new AbortController();
    ctrl.abort(new Error("cancelled upfront"));
    const pool = new PromisePool({ signal: ctrl.signal });
    await expect(pool.add(async () => 1)).rejects.toThrow("cancelled upfront");
  });
});

describe("events and stats", () => {
  it("emits lifecycle hooks and accounts every task", async () => {
    const events: string[] = [];
    const pool = new PromisePool({
      concurrency: 2,
      onStart: (m) => events.push(`start-${m.id}`),
      onSuccess: (m) => events.push(`ok-${m.id}`),
      onDrain: () => events.push("drain"),
    });
    await Promise.all([pool.add(async () => 1), pool.add(async () => 2)]);
    await pool.drain();
    expect(events).toContain("drain");
    expect(events.filter((e) => e.startsWith("start-"))).toHaveLength(2);
    expect(events.filter((e) => e.startsWith("ok-"))).toHaveLength(2);
    expect(pool.stats).toMatchObject({
      submitted: 2,
      started: 2,
      succeeded: 2,
      failed: 0,
      pending: 0,
      running: 0,
    });
  });
});

describe("backoff math", () => {
  it("grows exponentially and respects the cap without jitter", () => {
    const b = resolveBackoff({ baseDelayMs: 100, factor: 2, jitter: false });
    expect(computeDelayMs(1, b)).toBe(100);
    expect(computeDelayMs(2, b)).toBe(200);
    expect(computeDelayMs(3, b)).toBe(400);
    const capped = resolveBackoff({ baseDelayMs: 100, maxDelayMs: 250, jitter: false });
    expect(computeDelayMs(10, capped)).toBe(250);
  });

  it("jitters inside [0, computed] with an injectable random source", () => {
    const b = resolveBackoff({ baseDelayMs: 100, factor: 2, jitter: true });
    expect(computeDelayMs(2, b, () => 0)).toBe(0);
    expect(computeDelayMs(2, b, () => 0.5)).toBe(100); // floor(0.5 * 201)
    expect(computeDelayMs(2, b, () => 0.99999)).toBeLessThanOrEqual(200);
  });
});
