import * as Net from "node:net";

/**
 * NetError — replaces the former Effect `Data.TaggedError("NetError")`.
 *
 * Carries an optional `cause` (the original Node error) so callers can inspect
 * `errno`/`code` when present. Kept as a plain `Error` subclass so it
 * inter-operates with `try`/`catch` and `instanceof` without the Effect
 * runtime.
 */
export class NetError extends Error {
  override readonly cause?: unknown;
  constructor({ message, cause }: { readonly message: string; readonly cause?: unknown }) {
    super(message);
    this.name = "NetError";
    if (cause !== undefined) {
      this.cause = cause;
    }
  }
}

function isErrnoExceptionWithCode(cause: unknown): cause is { readonly code: string } {
  return (
    typeof cause === "object" &&
    cause !== null &&
    "code" in cause &&
    typeof (cause as { readonly code: unknown }).code === "string"
  );
}

const closeServer = (server: Net.Server): void => {
  try {
    server.close();
  } catch {
    // Ignore close failures during cleanup.
  }
};

/** Promise wrapper around `server.listen` + `close` to test/probe a port. */
function listenOnce(server: Net.Server, options: Net.ListenOptions): Promise<void> {
  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(options, () => {
      server.close(() => resolve());
    });
  });
}

/** Probe whether a TCP server can bind to {host, port}. Resolves true on success. */
async function canListenOnHost(port: number, host: string): Promise<boolean> {
  const server = Net.createServer();
  server.unref();
  try {
    await listenOnce(server, { host, port });
    return true;
  } catch (cause) {
    // `EADDRNOTAVAIL` is treated as available so IPv6-absent hosts don't fail
    // loopback availability checks.
    if (isErrnoExceptionWithCode(cause) && cause.code === "EADDRNOTAVAIL") {
      return true;
    }
    return false;
  } finally {
    closeServer(server);
  }
}

/**
 * Reserve an ephemeral loopback port and release it immediately.
 * Resolves the reserved port number, or rejects with {@link NetError}.
 */
async function reserveLoopbackPort(host = "127.0.0.1"): Promise<number> {
  const probe = Net.createServer();
  try {
    return await new Promise<number>((resolve, reject) => {
      const settle = (result: number | NetError) => {
        probe.removeAllListeners();
        probe.close(() => {
          if (typeof result === "number") {
            resolve(result);
          } else {
            reject(result);
          }
        });
      };

      probe.once("error", (cause) =>
        settle(new NetError({ message: "Failed to reserve loopback port", cause })),
      );

      probe.listen(0, host, () => {
        const address = probe.address();
        const port = typeof address === "object" && address !== null ? address.port : 0;
        if (port > 0) {
          settle(port);
        } else {
          settle(new NetError({ message: "Failed to reserve loopback port" }));
        }
      });
    });
  } catch (cause) {
    if (cause instanceof NetError) {
      throw cause;
    }
    throw new NetError({ message: "Failed to reserve loopback port", cause });
  } finally {
    closeServer(probe);
  }
}

/** Try to reserve a specific port, falling back to an ephemeral one. */
async function tryReservePort(port: number): Promise<number> {
  try {
    const probe = Net.createServer();
    try {
      return await new Promise<number>((resolve, reject) => {
        const settle = (result: number | NetError) => {
          probe.removeAllListeners();
          probe.close(() => {
            if (typeof result === "number") resolve(result);
            else reject(result);
          });
        };
        probe.once("error", (cause) =>
          settle(new NetError({ message: "Could not find an available port.", cause })),
        );
        probe.listen(port, () => {
          const address = probe.address();
          const resolved = typeof address === "object" && address !== null ? address.port : 0;
          if (resolved > 0) {
            settle(resolved);
          } else {
            settle(new NetError({ message: "Could not find an available port." }));
          }
        });
      });
    } finally {
      closeServer(probe);
    }
  } catch {
    return reserveLoopbackPort();
  }
}

export interface NetServiceShape {
  /** Returns true when a TCP server can bind to {host, port}. */
  readonly canListenOnHost: (port: number, host: string) => Promise<boolean>;
  /** Checks loopback availability on both IPv4 and IPv6 localhost addresses. */
  readonly isPortAvailableOnLoopback: (port: number) => Promise<boolean>;
  /** Reserve an ephemeral loopback port and release it immediately. */
  readonly reserveLoopbackPort: (host?: string) => Promise<number>;
  /** Resolve an available listening port, preferring the provided port first. */
  readonly findAvailablePort: (preferred: number) => Promise<number>;
}

/**
 * Check loopback availability on both IPv4 (`127.0.0.1`) and IPv6 (`::1`).
 * Replaces the former Effect `Effect.zipWith(canListenOnHost(...), ...)`.
 */
async function isPortAvailableOnLoopbackImpl(port: number): Promise<boolean> {
  const [ipv4, ipv6] = await Promise.all([
    canListenOnHost(port, "127.0.0.1"),
    canListenOnHost(port, "::1"),
  ]);
  return ipv4 && ipv6;
}

/**
 * NetService — plain object replacing the former Effect `ServiceMap.Service` +
 * `Layer.sync`. Implements {@link NetServiceShape} with async functions.
 *
 * The former `static readonly layer` (an Effect `Layer`) is no longer needed:
 * callers now import `NetService` directly and `await` its methods.
 */
export const NetService: NetServiceShape = {
  canListenOnHost,
  isPortAvailableOnLoopback: isPortAvailableOnLoopbackImpl,
  reserveLoopbackPort,
  findAvailablePort: (preferred) => tryReservePort(preferred),
};
