# Auto-bootstrap on fall-off-the-log

When a receiver cluster has fallen so far behind a sender that the sender has
already trimmed the WAL entries the receiver still needs, incremental
replication cannot bridge the gap and the receiver must re-seed from a fresh
snapshot. Two seams collaborate to detect and react to this condition:

| Seam | Side | Default | Purpose |
|------|------|---------|---------|
| `ILatticeWalIntrospection` | sender | Built-in | Returns the oldest still-available WAL entry HLC for a tree by walking each per-shard WAL grain and taking the minimum head timestamp. |
| `ILatticeFallOffLogDetector` | receiver | Built-in | Compares the receiver's per-origin high-water-mark against the sender's oldest-available HLC, records the `peer.fell_off_log` metric on detection, and (when configured) invokes `ILatticeBootstrapCoordinator.BootstrapAsync`. |

## Detection rule

Fall-off is detected when, for a given `(treeName, sourceClusterId)`, the
receiver's per-origin high-water-mark is **strictly less than** the sender's
oldest still-available WAL entry HLC. Equality is intentionally not a
fall-off - the receiver has applied exactly up to the sender's oldest entry
and can resume incrementally from the next one.

## Triggering a check

Today the sender's oldest-available HLC is plumbed through the call shape as
an explicit parameter:

```csharp verify
var detector = client.ServiceProvider.GetRequiredService<ILatticeFallOffLogDetector>();
var introspection = client.ServiceProvider.GetRequiredService<ILatticeWalIntrospection>();

var senderOldest = await introspection.GetOldestAvailableHlcAsync("tree-a");
if (senderOldest is { } hlc)
{
    var decision = await detector.CheckAndTriggerAsync("tree-a", "site-a", hlc);
    if (decision.FellOffLog && !decision.BootstrapTriggered)
    {
        // Auto-bootstrap is disabled; operator drives the re-seed manually.
    }
}
```

A future transport revision will fold the sender's oldest HLC into the batch
envelope so each inbound apply naturally populates the parameter; until then,
co-located callers can use `ILatticeWalIntrospection` directly.

## Configuration

`LatticeReplicationOptions.AutoBootstrapOnFallOffLog` (default `true`) gates
whether detection automatically calls
`ILatticeBootstrapCoordinator.BootstrapAsync`. When disabled, the metric still
fires and the returned `FallOffLogDecision.FellOffLog` flag is `true`, but the
bootstrap kickoff is the operator's responsibility.

## Observability

The `peer.fell_off_log` counter on the `orleans.lattice.replication` meter is
incremented exactly once per fresh detection, tagged `tree` and `origin`. An
alert on `rate(peer.fell_off_log) > 0` flags a receiver that has lost
incremental ground against a peer.

While a bootstrap is already draining for the same `(tree, sourceClusterId)`,
the detector consults `ILatticeBootstrapCoordinator.GetStatusAsync` first and
absorbs duplicate probes: `peer.fell_off_log` is **not** re-incremented, the
warning log is downgraded to debug verbosity, and the
`peer.fell_off_log_suppressed` counter (same tag set) increments instead.
Operators wiring alerts should therefore:

- Alert on `rate(peer.fell_off_log)` for fresh fall-off detection.
- Surface `peer.fell_off_log_suppressed` as a non-alerting dashboard metric so
  long-running drains remain visible without paging.
- Use `FallOffLogDecision.Suppressed` (also surfaced on the detector return
  value) to distinguish "the detector did not fire" from "the detector fired
  and the coordinator was already handling it" inside diagnostic tooling.

## Idempotency

The bootstrap coordinator's idempotency contract handles concurrent detection
cleanly: a kickoff for the same `(tree, sourceClusterId)` while a bootstrap
is already in flight from the same source is a no-op; a kickoff from a
different source cluster throws and the exception propagates verbatim out of
`CheckAndTriggerAsync`. Repeated detection while a bootstrap is already
running is therefore harmless, and the detector projects that idempotency
into the `peer.fell_off_log_suppressed` counter so it remains observable.