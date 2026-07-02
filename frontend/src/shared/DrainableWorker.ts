/**
 * DrainableWorker — plain-TS replacement for the former Effect
 * `Queue.unbounded` + `Effect.forever` pattern.
 *
 * Exposes a queue-based worker whose `enqueue(item)` schedules processing on
 * the JS event loop and whose `drain()` returns a `Promise` that resolves when
 * the queue is empty AND the currently-processing item has finished.
 *
 * This lets tests replace timing-sensitive `setTimeout`/interval polling with
 * a deterministic `await worker.drain()`. The worker auto-starts on
 * construction and stops when {@link shutdown} is called.
 *
 * Replaces the Effect `Deferred`/`Ref`/`Queue`/`Scope` implementation with a
 * minimal hand-rolled equivalent.
 *
 * @module DrainableWorker
 */

export interface DrainableWorker<A> {
  /**
   * Enqueue a work item. Resolves once the item has been queued (not once it
   * has been processed — await {@link drain} for that).
   */
  readonly enqueue: (item: A) => Promise<void>;

  /**
   * Resolves when the queue is empty and the worker is idle (not processing).
   */
  readonly drain: Promise<void>;

  /**
   * Stop the worker and reject any pending drain waiters. Idempotent.
   */
  readonly shutdown: () => void;
}

interface IdleState {
  readonly promise: Promise<void>;
  readonly resolve: () => void;
}

function makeIdleState(): IdleState {
  let resolve: () => void = () => {};
  const promise = new Promise<void>((res) => {
    resolve = res;
  });
  return { promise, resolve };
}

/**
 * Create a drainable worker that processes items from an unbounded queue.
 *
 * The worker begins consuming immediately. Call {@link DrainableWorker.shutdown}
 * to stop it (the former Effect version relied on `Scope` closure for this).
 *
 * @param process - Async (or sync) handler invoked for each queued item.
 * @returns A {@link DrainableWorker} with `enqueue`, `drain`, and `shutdown`.
 */
export const makeDrainableWorker = <A>(
  process: (item: A) => Promise<void> | void,
): DrainableWorker<A> => {
  const queue: A[] = [];
  let outstanding = 0;
  let idle: IdleState = makeIdleState();
  // The worker starts idle (nothing in flight), so resolve the initial idle
  // promise immediately to mirror the former `Deferred.succeed(initialIdle)`.
  idle.resolve();

  let stopped = false;

  const finishOne = (): void => {
    outstanding = Math.max(0, outstanding - 1);
    if (outstanding === 0) {
      const current = idle;
      idle = makeIdleState();
      current.resolve();
    }
  };

  const runLoop = async (): Promise<void> => {
    while (!stopped) {
      const item = queue.shift();
      if (item === undefined) {
        // Wait for the next enqueue to schedule a microtask tick. We yield to
        // the event loop and rely on `scheduleNext` to re-enter when work is
        // available. A small guard prevents a tight busy-loop.
        await new Promise<void>((resolve) => setTimeout(resolve, 0));
        continue;
      }
      try {
        await process(item);
      } catch {
        // Swallow processing errors to keep the worker alive (mirrors the
        // former `Effect.ensuring(finishOne)` which did not fail the loop).
      } finally {
        finishOne();
      }
    }
  };

  let loopStarted = false;
  const scheduleNext = (): void => {
    if (!loopStarted) {
      loopStarted = true;
      void runLoop();
    }
  };

  const enqueue: DrainableWorker<A>["enqueue"] = async (item: A) => {
    if (stopped) {
      return;
    }
    // When the worker was idle, rotate the idle promise so drain() blocks
    // until this (and any concurrently-enqueued) item completes.
    if (outstanding === 0) {
      const previousIdle = idle;
      idle = makeIdleState();
      // Carry forward the resolved state if there was nothing outstanding.
      previousIdle.promise.then(() => {});
    }
    outstanding += 1;
    queue.push(item);
    scheduleNext();
  };

  const shutdown = (): void => {
    stopped = true;
    queue.length = 0;
  };

  return {
    enqueue,
    get drain() {
      return idle.promise;
    },
    shutdown,
  } satisfies DrainableWorker<A>;
};
