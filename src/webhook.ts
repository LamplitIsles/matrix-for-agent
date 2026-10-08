export class Webhook {
  private queue: unknown[] = [];
  private running?: Promise<void>;
  private stopped = false;
  private abort = new AbortController();
  constructor(
    private url: string,
    private token?: string,
    private diagnostic: (s: string) => void = console.error,
    private bounds = { capacity: 100, timeout: 5000, attempts: 3, delay: 250 },
  ) {}
  push(value: unknown) {
    if (this.stopped) return;
    if (this.queue.length >= this.bounds.capacity) {
      this.diagnostic("Webhook queue overflow: newest message dropped");
      return;
    }
    this.queue.push(value);
    this.running ??= this.drain().finally(() => {
      this.running = undefined;
    });
  }
  private async drain() {
    while (this.queue.length && !this.stopped) {
      const value = this.queue[0];
      let accepted = false;
      for (
        let attempt = 0;
        attempt < this.bounds.attempts && !this.stopped;
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
            body: JSON.stringify(value),
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
      if (!accepted && !this.stopped)
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
