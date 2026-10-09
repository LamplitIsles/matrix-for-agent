export type WebhookItem = object | ((signal: AbortSignal) => Promise<object | null>);

export class Webhook {
  private queue: { item: WebhookItem; available: () => boolean }[] = [];
  private running?: Promise<void>;
  private stopped = false;
  private abort = new AbortController();
  constructor(
    private url: string,
    private token?: string,
    private diagnostic: (s: string) => void = console.error,
    private bounds = { capacity: 100, timeout: 5000, attempts: 3, delay: 250 },
  ) {}
  push(item: WebhookItem, available: () => boolean = () => true) {
    if (this.stopped) return;
    if (this.queue.length >= this.bounds.capacity) {
      this.diagnostic("Webhook queue overflow: newest message dropped");
      return;
    }
    this.queue.push({ item, available });
    this.start();
  }
  private start() {
    this.running ??= this.drain().finally(() => {
      this.running = undefined;
      if (this.queue.length && !this.stopped) this.start();
    });
  }
  private async drain() {
    while (this.queue.length && !this.stopped) {
      const { item, available } = this.queue[0];
      const value = typeof item === "function" ? await item(this.abort.signal) : item;
      if (this.stopped) break;
      if (value === null) {
        this.queue.shift();
        continue;
      }
      // Resolve and serialize once: every retry carries the same bytes.
      const body = JSON.stringify(value);
      let accepted = false;
      for (
        let attempt = 0;
        attempt < this.bounds.attempts && !this.stopped && available();
        attempt++
      ) {
        try {
          const response = await fetch(this.url, {
            method: "POST",
            redirect: "manual",
            headers: {
              "content-type": "application/json",
              ...(this.token ? { authorization: `Bearer ${this.token}` } : {}),
            },
            body,
            signal: AbortSignal.any([
              this.abort.signal,
              AbortSignal.timeout(this.bounds.timeout),
            ]),
          });
          accepted = response.ok;
          await response.body?.cancel();
        } catch {
          /* Diagnostics intentionally exclude network errors and credentials. */
        }
        if (accepted || this.stopped) break;
        if (attempt + 1 < this.bounds.attempts)
          await new Promise<void>((resolve) => {
            const timer = setTimeout(done, this.bounds.delay);
            const signal = this.abort.signal;
            function done() {
              clearTimeout(timer);
              signal.removeEventListener("abort", done);
              resolve();
            }
            signal.addEventListener("abort", done, { once: true });
          });
      }
      if (!accepted && !this.stopped && available())
        this.diagnostic("Webhook retries exhausted: message dropped");
      this.queue.shift();
    }
  }
  async stop() {
    this.stopped = true;
    this.abort.abort();
    this.queue = [];
    await this.running;
  }
}
