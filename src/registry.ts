import { createHash } from "node:crypto";
import {
  MatrixError,
  HTTPError,
  MatrixHttpApi,
  Method,
  TypedEventEmitter,
  ClientEvent,
  SyncState,
} from "matrix-js-sdk";
import { Matrix } from "./matrix.js";
import { userId, type WebhookConfig } from "./config.js";
import { Webhook } from "./webhook.js";
export class AccessError extends Error {
  constructor(readonly status: 401 | 503) {
    super(status === 401 ? "Unauthorized" : "Matrix service unavailable");
  }
}
type Identity = Matrix["identity"];
export async function authenticate(
  homeserver: string,
  token: string,
  signal: AbortSignal,
  timeout = 10000,
): Promise<Identity> {
  const abort = new AbortController();
  const timer = setTimeout(() => abort.abort(), timeout);
  let unauthorized = false;
  const http = new MatrixHttpApi(new TypedEventEmitter(), {
    baseUrl: homeserver,
    prefix: "/_matrix/client/v3",
    accessToken: token,
    onlyData: true,
    // SDK detaches signals at headers; consume the body inside fetchFn to keep
    // timeout and disconnect cancellation effective through the entire transfer.
    fetchFn: (async (input: URL | RequestInfo, options?: RequestInit) => {
      const response = await fetch(input, {
        ...options,
        redirect: "error",
        signal: AbortSignal.any([
          signal,
          abort.signal,
          ...(options?.signal ? [options.signal] : []),
        ]),
      });
      unauthorized = response.status === 401;
      return new Response(await response.arrayBuffer(), {
        status: response.status,
        statusText: response.statusText,
        headers: response.headers,
      });
    }) as typeof fetch,
  });
  try {
    const identity = await http.authedRequest<Identity>(
      Method.Get,
      "/account/whoami",
    );
    if (
      !userId(identity?.user_id) ||
      (identity.device_id !== undefined &&
        typeof identity.device_id !== "string")
    )
      throw new AccessError(503);
    return identity;
  } catch (e) {
    if (
      unauthorized ||
      (e instanceof HTTPError && e.httpStatus === 401) ||
      (e instanceof MatrixError &&
        ["M_UNKNOWN_TOKEN", "M_UNAUTHORIZED"].includes(e.errcode ?? ""))
    )
      throw new AccessError(401);
    throw new AccessError(503);
  } finally {
    clearTimeout(timer);
    http.abort();
  }
}
function ready(matrix: Matrix, signal: AbortSignal, timeout: number) {
  return new Promise<void>((resolve, reject) => {
    const done = (error?: unknown) => {
      clearTimeout(timer);
      signal.removeEventListener("abort", aborted);
      matrix.client.removeListener(ClientEvent.Sync, synced);
      error ? reject(error) : resolve();
    };
    const aborted = () => done(new AccessError(503));
    const synced = (state: SyncState) => {
      if (state === SyncState.Prepared || state === SyncState.Syncing) done();
      else if (state === SyncState.Error || state === SyncState.Stopped)
        aborted();
    };
    const timer = setTimeout(aborted, timeout);
    matrix.client.on(ClientEvent.Sync, synced);
    signal.addEventListener("abort", aborted, { once: true });
    if (signal.aborted) aborted();
  });
}
type Entry = {
  key: string;
  matrix: Matrix;
  configured: boolean;
  owners: number;
  idle: number;
  abort: AbortController;
  promise: Promise<Matrix>;
  unobserve?: () => void;
  webhook?: Webhook;
  retired: boolean;
};
export type Lease = { matrix?: Matrix; release(): void };
export class Registry {
  private entries = new Map<string, Entry>();
  private owned = new Set<Entry>();
  private jobs = new Set<Promise<unknown>>();
  private abort = new AbortController();
  private admissions = 0;
  private timer: ReturnType<typeof setInterval>;
  private stopTask?: Promise<void>;
  constructor(
    private homeserver: string,
    private bounds = {
      capacity: 16,
      admission: 16,
      idle: 300000,
      sweep: 30000,
      readiness: 10000,
      auth: 10000,
    },
    private create = Matrix.create,
  ) {
    this.timer = setInterval(() => this.sweep(), bounds.sweep);
    this.timer.unref();
  }
  private key(token: string) {
    return createHash("sha256").update(token).digest("hex");
  }
  private sweep() {
    for (const entry of this.entries.values())
      if (
        !entry.configured &&
        !entry.owners &&
        Date.now() - entry.idle >= this.bounds.idle
      )
        this.retire(entry);
  }
  private retire(entry: Entry) {
    entry.retired = true;
    if (this.entries.get(entry.key) === entry) this.entries.delete(entry.key);
    if (!entry.owners || this.abort.signal.aborted) this.dispose(entry);
  }
  private dispose(entry: Entry) {
    entry.abort.abort();
    entry.unobserve?.();
    entry.matrix.stop();
    const stopping = entry.webhook?.stop() ?? Promise.resolve();
    this.track(stopping);
    this.owned.delete(entry);
  }
  private track<T>(promise: Promise<T>): Promise<T> {
    this.jobs.add(promise);
    void promise.then(
      () => this.jobs.delete(promise),
      () => this.jobs.delete(promise),
    );
    return promise;
  }
  private make(token: string, identity: Identity, config?: WebhookConfig) {
    let matrix: Matrix;
    try {
      matrix = this.create({ homeserver: this.homeserver, token }, identity);
    } catch {
      throw new AccessError(503);
    }
    const entry: Entry = {
      key: this.key(token),
      matrix,
      configured: !!config,
      owners: 0,
      idle: Date.now(),
      abort: new AbortController(),
      promise: undefined!,
      retired: false,
    };
    this.entries.set(entry.key, entry);
    this.owned.add(entry);
    if (config) {
      entry.webhook = new Webhook(config.url, config.bearer_token);
      entry.unobserve = matrix.observe((value, available) =>
        entry.webhook!.push(value, available),
      );
    }
    entry.promise = this.track(
      (async () => {
        // Listener is installed before sync, and observation before both. PREPARED
        // means usable room state; it does not open the observer's live boundary.
        const usable = ready(matrix, entry.abort.signal, this.bounds.readiness);
        const starting = matrix.start();
        try {
          await Promise.all([starting, usable]);
          if (entry.retired || this.abort.signal.aborted)
            throw new AccessError(503);
          return matrix;
        } catch {
          this.retire(entry);
          // Cancel immediately even when waiters have not yet released ownership.
          this.dispose(entry);
          await starting.catch(() => {});
          matrix.stop();
          throw new AccessError(503);
        }
      })(),
    );
    return entry;
  }
  async start(configs: WebhookConfig[]) {
    try {
      for (const config of configs) {
        if (this.abort.signal.aborted) throw new AccessError(503);
        const identity = await this.track(
          authenticate(
            this.homeserver,
            config.access_token,
            this.abort.signal,
            this.bounds.auth,
          ),
        );
        if (identity.user_id !== config.user_id || this.abort.signal.aborted)
          throw new AccessError(503);
        await this.make(config.access_token, identity, config).promise;
      }
    } catch {
      await this.stop();
      throw new AccessError(503);
    }
  }
  request(token: string, signal: AbortSignal, runtime = true): Promise<Lease> {
    if (this.abort.signal.aborted || this.admissions >= this.bounds.admission)
      return Promise.reject(new AccessError(503));
    this.admissions++;
    return this.track(
      this.resolve(token, signal, runtime).finally(() => {
        this.admissions--;
      }),
    );
  }
  private async resolve(
    token: string,
    signal: AbortSignal,
    runtime: boolean,
  ): Promise<Lease> {
    signal = AbortSignal.any([signal, this.abort.signal]);
    let identity: Identity;
    try {
      identity = await authenticate(
        this.homeserver,
        token,
        signal,
        this.bounds.auth,
      );
    } catch (e) {
      const old = this.entries.get(this.key(token));
      if (
        e instanceof AccessError &&
        e.status === 401 &&
        old &&
        !old.configured
      )
        this.retire(old);
      throw e;
    }
    if (signal.aborted) throw new AccessError(503);
    let entry = this.entries.get(this.key(token));
    if (
      entry &&
      (entry.matrix.identity.user_id !== identity.user_id ||
        entry.matrix.identity.device_id !== identity.device_id)
    ) {
      if (!entry.configured) this.retire(entry);
      throw new AccessError(503);
    }
    if (!runtime) return { release() {} };
    if (!entry) {
      this.sweep();
      if (
        [...this.owned].filter((e) => !e.configured).length >=
        this.bounds.capacity
      )
        throw new AccessError(503);
      entry = this.make(token, identity);
    }
    const current = entry;
    current.owners++;
    let released = false;
    const release = () => {
      if (released) return;
      released = true;
      signal.removeEventListener("abort", release);
      current.owners--;
      current.idle = Date.now();
      if (
        !current.owners &&
        (current.retired || (!current.configured && !completed))
      )
        this.retire(current);
    };
    let completed = false;
    signal.addEventListener("abort", release, { once: true });
    try {
      const matrix = await new Promise<Matrix>((resolve, reject) => {
        const aborted = () => reject(new AccessError(503));
        signal.addEventListener("abort", aborted, { once: true });
        current.promise
          .then(resolve, reject)
          .finally(() => signal.removeEventListener("abort", aborted));
        if (signal.aborted) aborted();
      });
      if (signal.aborted || current.retired) throw new AccessError(503);
      completed = true;
      return { matrix, release };
    } catch (e) {
      release();
      throw e;
    }
  }
  stop(): Promise<void> {
    return (this.stopTask ??= (async () => {
      clearInterval(this.timer);
      this.abort.abort();
      for (const entry of [...this.owned]) this.retire(entry);
      while (this.jobs.size) await Promise.allSettled([...this.jobs]);
    })());
  }
}
