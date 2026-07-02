# RetryPolicy

## What it shows

Storage faults are sometimes transient (a throttle, a brief network blip). This
sample wraps a write in `BoundedExponentialRetryPolicy` and pairs it with a
`LatticeIdempotencyContext` scope, so the operation is retried under a stable
**idempotency key**. A simulated transient fault fails the first two attempts;
the third succeeds, and because every attempt carried the same idempotency key
the retries collapse to a single logical mutation.

## Run it

```
dotnet run --project samples/RetryPolicy
```

## Expected output

```
Silo starting... ready.

== Retrying a write that fails twice under a simulated transient fault ==
  attempt #1...
  attempt #2...
  attempt #3...
  attempt succeeded - write committed.

  tree['orders/42'] = shipped  (after 3 attempts)

Done. The operation survived transient faults and the retried write
collapsed to a single mutation under one idempotency key.
```

## When to use

- Wrapping writes that can hit transient backend faults (throttling, timeouts)
  where a bounded, backing-off retry turns a blip into a success.
- Any retried mutation that must stay exactly-once: supply a caller-owned
  idempotency key so a replay does not double-apply.

## When not to use

- Deterministic, non-transient failures (bad input, auth). Retrying just delays
  the inevitable error - let it surface. Classify which exceptions are retryable
  via the policy's classifier.

## Feature docs

[docs/lattice/retry-policy.md](../../docs/lattice/retry-policy.md)
