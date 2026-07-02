# TTL

## What it shows

Per-entry **time-to-live**: a key written with a `TimeSpan` TTL is visible to
every read until its absolute expiry instant (resolved server-side as
`UtcNow + ttl`), after which every read path treats it as absent. A plain write
carries no expiry and never lapses. This sample writes one expiring key and one
durable key, reads both before expiry, waits past the TTL, and reads again to
show only the expiring key disappear.

## Run it

```
dotnet run --project samples/Ttl
```

## Expected output

```
== Ttl sample ==

Wrote 'session:token' with a 2s TTL and 'account:alice' with no TTL.

Immediately after write:
   session:token = abc123
   account:alice = Alice

Waiting 3s for the TTL to elapse...

After the TTL elapsed:
   session:token = <not found>
   account:alice = Alice

-> the expiring key vanished from reads; the durable key stayed.

Done.
```

## When to use

- Entries that should self-expire without an explicit delete: session tokens,
  short-lived caches, rate-limit counters, one-time codes.
- When you want expiry resolved consistently on the server (client clock skew
  does not shift individual entry lifetimes).

## When not to use

- Data that must persist until explicitly removed - simply omit the TTL overload.
- As a precise scheduler. Expiry hides an entry from reads at its instant, but
  physical reclamation is deferred to tombstone compaction after the configured
  grace period; do not rely on TTL for exact-time side effects.

## Feature doc

[docs/lattice/ttl.md](../../docs/lattice/ttl.md)
