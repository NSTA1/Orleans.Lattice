# Auto-bootstrap on fall-off-the-log

When a receiver cluster has fallen so far behind a sender that the sender has
already trimmed the WAL entries the receiver still needs, incremental
replication cannot bridge the gap and the receiver must re-seed from a fresh
snapshot. Two seams collaborate to detect and react to this condition:

| Seam | Side | Default | Purpose |
|------|------|---------|---------|
| `ILatticeWalIntrospection` | sender | `LatticeWalIntrospection` | Returns the oldest still-available WAL entry HLC for a tree by walking each `IWalShardGrain` and taking the minimum head timestamp. |
| `ILatticeFallOffLogDetector` | receiver | `LatticeFallOffLogDetector` | Compares the receiver's per-origin high-water-mark against the sender's oldest-available HLC, records the `peer.fell_off_log` metric on detection, and (when configured) invokes `ILatticeBootstrapCoordinator.BootstrapAsync`. |

## Detection rule

Fall-off is detected when, for a given `(treeName, sourceClusterId)`, the
receiver's per-origin high-water-mark is **strictly less than** the sender's
oldest still-available WAL entry HLC. Equality is intentionally not a
fall-off — the receiver has applied exactly up to the sender's oldest entry
and can resume incrementally from the next one.

## Triggering a check

Today the sender's oldest-available HLC is plumbed through the call shape as
an explicit parameter:

```csharp
var detector = host.Services.GetRequiredService<ILatticeFallOffLogDetector>();
var introspection = host.Services.GetRequiredService<ILatticeWalIntrospection>();

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
incremented exactly once per detection, tagged `tree` and `origin`. An alert
on `rate(peer.fell_off_log) > 0` flags a receiver that has lost incremental
ground against a peer.

## Idempotency

The bootstrap coordinator's idempotency contract handles concurrent detection
cleanly: a kickoff for the same `(tree, sourceClusterId)` while a bootstrap
is already in flight from the same source is a no-op; a kickoff from a
different source cluster throws and the exception propagates verbatim out of
`CheckAndTriggerAsync`. Repeated detection while a bootstrap is already
running is therefore harmless.