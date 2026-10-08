---
agent_spec: "docs/agents/procedures/replication-reseed.yaml"
---

# Auto-bootstrap on fall-off-the-log

When a receiver cluster has fallen so far behind a sender that the sender has
already trimmed the WAL entries the receiver still needs, incremental
replication cannot bridge the gap and the receiver must re-seed from a fresh
snapshot. These seams collaborate to detect and react to this condition:

| Seam | Side | Default | Purpose |
|------|------|---------|---------|
| `ILatticeWalIntrospection` | receiver | Built-in | Returns the oldest local retained WAL entry HLC for a tree, either overall or grouped by the origin that authored the entries. The maintenance probe uses this local reading. |
| `ILatticeFallOffLogDetector` | receiver | Built-in | Compares the receiver's per-origin high-water-mark against that receiver's local oldest retained HLC for the same origin, records the `peer.fell_off_log` metric on detection, and (when configured) invokes `ILatticeBootstrapCoordinator.BootstrapAsync`. |
| Source shipper forced-gap detection | sender | Built-in | Detects that a shipping read returned a first sequence above the requested sequence because the source WAL was trimmed before the peer received those entries, then stamps `ReplicationBatch.ReseedAfterEpoch` so the receiver re-seeds fully. |

## Detection rule

The receiver-side probe detects local fall-off when, for a given `(treeName, sourceClusterId)`, the receiver's per-origin high-water-mark is **strictly less than** the oldest local retained WAL entry that the receiver still has for that origin. Equality is intentionally not a fall-off - the receiver has applied exactly up to the oldest retained local entry and can resume incrementally from the next one.

A different detector covers source-side WAL trims. When the source shipper reads from sequence `N` and the first retained shipping entry is above `N`, the source knows the peer missed entries that were trimmed before shipping. It records the current export epoch, withholds saga records, and stamps `ReplicationBatch.ReseedAfterEpoch` on pushes until the receiver echoes a completed bootstrap from a later export.

## Triggering a check

The local oldest-available HLC is plumbed through the call shape as an explicit parameter:

```csharp verify
var detector = client.ServiceProvider.GetRequiredService<ILatticeFallOffLogDetector>();
var introspection = client.ServiceProvider.GetRequiredService<ILatticeWalIntrospection>();

var localOldestByOrigin = await introspection.GetOldestAvailableHlcByOriginAsync("tree-a");
if (localOldestByOrigin.TryGetValue("site-a", out var hlc))
{
    var decision = await detector.CheckAndTriggerAsync("tree-a", "site-a", hlc);
    if (decision.FellOffLog && !decision.BootstrapTriggered)
    {
        // Auto-bootstrap is disabled; operator drives the re-seed manually.
    }
}
```

The built-in maintenance grain supplies this value from the receiver's local WAL. It is not the cross-cluster source trim path; source trims are detected by the shipper's sequence read as described below.

The per-tree replication maintenance pass also runs the check on its own
cadence, every `LatticeReplicationOptions.MaintenanceFallOffCheckInterval`
(default 30 seconds): it reads a bounded window at the head of each local WAL
partition, takes the oldest retained entry each current peer authored in that
window (`ILatticeWalIntrospection.GetOldestAvailableHlcByOriginAsync`), and
passes it to `CheckAndTriggerAsync`. A peer with no authored entry in that
window is skipped, and the local cluster is never probed against its own origin.

`ILatticeWalIntrospection` addresses the WAL partitions by the tree id it is
given and, like the [change feed](change-feed.md), does not follow a tree's
alias; the maintenance pass passes the logical tree id. After a shadow-cutover
restore, a resize, a schema remediation, or an operator alias change repoints
the tree at another physical copy, the probe therefore reads the retired copy's
log - or nothing, once that copy is purged - rather than the log the tree's new
writes land in.

## Sender-requested re-seed

The fall-off detector compares the receiver's high-water mark against the receiver's own local log, so it cannot see records the sender's `WalRetention` ceiling trimmed before shipping them. The sender detects that case itself (a forced gap, see [Replication drivers](replication-drivers.md#forced-gap-a-peer-taken-off-the-log)): a shipping page whose first sequence is greater than the requested sequence, with the source WAL's trim watermark at or past the requested sequence, means the source trimmed entries the peer never received (a jump the watermark has not reached is a hole, issue #4621). The shipper records the current export epoch, withholds saga records, and asks the receiver to re-seed on each push via `ReplicationBatch.ReseedAfterEpoch`. The receiver starts the same `BootstrapAsync` the detector would, under the same `AutoBootstrapOnFallOffLog` switch, and only when no bootstrap is running and none from an export after the sender's requested epoch has completed ([#4534](https://github.com/NSTA1/Orleans.Lattice/issues/4534)).

A custom `IReplicationTransport` must carry `ReplicationBatch.ReseedAfterEpoch` to the receiver and echo `ReplicationAck.BootstrapEpoch` back, as the gRPC transport does with the `x-lattice-replication-reseed-after` header. A transport that drops the field fails closed: the source keeps withholding saga records, the peer status reads `Stalled`, and an operator must re-seed or fix the transport before saga traffic resumes.

## Configuration

`LatticeReplicationOptions.AutoBootstrapOnFallOffLog` (default `true`) gates
whether detection automatically calls
`ILatticeBootstrapCoordinator.BootstrapAsync`. When disabled, the metric still
fires and the returned `FallOffLogDecision.FellOffLog` flag is `true`, but the
bootstrap kickoff is the operator's responsibility.

## Observability

The `peer.fell_off_log` counter on the `orleans.lattice.replication` meter is
incremented exactly once per fresh detection, tagged `tree`, `origin`, and
`tenant`. An alert on `rate(peer.fell_off_log) > 0` flags a receiver that has
lost incremental ground against a peer.

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