# Anti-entropy bootstrap-snapshot fallback (GC'd divergence)

[Targeted leaf re-replay](anti-entropy-leaf-rereplay.md) repairs a localised divergence by re-shipping the relevant write-ahead-log entries. But when the local WAL has been garbage-collected past the divergence point, those entries are gone - re-replay emits the operator-only `leaf_rereplay.skipped{reason=wal_trimmed}` signal and stops. The **bootstrap-snapshot fallback** is the next line of defence: a strictly opt-in pass that re-derives the committed projection of *only* the divergent leaf range from the live tree (which is immune to WAL trimming) and re-ships those committed entries to the diverged peer.

The repair travels the **same** replication transport and causal-stable apply path as ordinary replication. Re-shipped entries carry their committed-projection clock verbatim and are de-duplicated at the receiver on `(originClusterId, hlc)`, so re-sending is idempotent.

## How the scope stays proportional to the drift

The snapshot is bounded to the `[StartKey, EndKey)` covering ranges the [Merkle walk](anti-entropy-merkle-walk.md) localised - not the whole tree. A new range-scoped export overload is the seam:

- `ISnapshotProvider.ExportAsync(treeName, ranges, asOfHlc, ct)` yields only entries whose key falls inside at least one range, using the same ordinal half-open `[StartKey, EndKey)` membership (`LeafReReplayRange.Contains`) the re-replay selection uses, so the two stages localise on byte-identical boundaries.

The fallback ships **committed projection rows only**: prepared (not-yet-decided) saga rows and tombstoned keys are skipped, since the committed projection already reflects every decided value. Each row becomes a `Set` stamped with the local cluster id, capped per pass and always shipping at least one entry.

## Scope and limitations

- **Client-side scoping in the default provider.** The default `ISnapshotProvider` implements the range overload by exporting the whole tree and filtering the stream client-side - it does **not** push the range bound into storage. A storage-aware provider can override the overload to avoid streaming out-of-range entries it then discards; the metadata it returns must match the whole-tree export at the same as-of HLC so receivers pin the same resume cut.
- **Cross-cluster push needs a real transport.** The re-ship goes through `IReplicationTransport`; the default no-op transport acks but does not deliver. Wire the gRPC binding (or a custom transport) for genuine cross-cluster repair, exactly as for leaf re-replay.
- **Committed projection only.** Prepared-saga rows are not re-shipped; per-entry origin is stamped as the local cluster id on the wire, matching the existing whole-tree bootstrap convention. For CRDT-mode trees the re-shipped value is the committed state at snapshot time applied through the receiver's per-tree merge mode, consistent with whole-tree bootstrap semantics.
- **Bounded per pass.** A divergence larger than the entry/byte caps makes partial progress per cadence.

## Enabling it

The fallback ships **dark** and is gated: targeted leaf re-replay must be enabled and must report a trimmed WAL, the walk must have localised at least one leaf, and `BootstrapFallbackEnabled` must be `true`. An un-opted host sees no new behaviour; when the WAL-trimmed signal fires while the flag is off, a single `bootstrap_fallback.skipped{reason=disabled}` is emitted so operators can see the fallback was available but not taken.

```csharp verify
siloBuilder.AddLatticeReplication(o =>
{
    o.ClusterId = "cluster-a";
    o.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>
    {
        ["orders"] = LatticeMergeMode.LwwRegister,
    };

    // Detection + localisation + WAL re-replay (all off by default).
    o.DigestProbeEnabled = true;
    o.MerkleWalkEnabled = true;
    o.LeafReReplayEnabled = true;

    // GC'd-divergence fallback (off by default). Runs only after a trimmed WAL.
    o.BootstrapFallbackEnabled = true;
    o.BootstrapFallbackMaxEntries = 4096;
    o.BootstrapFallbackMaxBytes = 1024 * 1024;
});
```

| Option | Default | Notes |
|---|---|---|
| `BootstrapFallbackEnabled` | `false` | Master switch. When `false`, a WAL-trimmed divergence is counted (`bootstrap_fallback.skipped{reason=disabled}`) but never repaired. |
| `BootstrapFallbackMaxEntries` | `4096` | Soft cap on committed entries re-shipped per pass; always ships at least one. Validated `>= 1`. |
| `BootstrapFallbackMaxBytes` | `1048576` | Soft cap on the estimated re-shipped payload bytes per pass; always ships at least one. Validated `>= 1`. |

## Observability

Three counters on the `orleans.lattice.replication` meter:

| Metric | Tags | Emitted |
|---|---|---|
| `orleans.lattice.replication.bootstrap_fallback.triggered` | `tree`, `peer` | Once when a fallback pass begins (after the ranges-non-empty check). |
| `orleans.lattice.replication.bootstrap_fallback.entries` | `tree`, `peer` | By the number of committed entries re-shipped to the peer in a pass. |
| `orleans.lattice.replication.bootstrap_fallback.skipped` | `tree`, `peer`, `reason` | Once per pass that skipped without re-shipping. |

Skip reasons: `disabled` (the feature is off but a trimmed-WAL divergence was available), `range_empty` (the localiser produced no ranges), and `empty` (the scoped export yielded no committed entries in range).

The metric-name constants and the skip-reason mapping are exposed for dashboards built from the public surface:

```csharp verify
_ = LatticeReplicationMetrics.BootstrapFallbackTriggeredName;
_ = LatticeReplicationMetrics.BootstrapFallbackEntriesName;
_ = LatticeReplicationMetrics.BootstrapFallbackSkippedName;

string tag = LatticeReplicationMetrics.BootstrapFallbackSkipReasonTag(BootstrapFallbackSkipReason.Disabled);
System.Diagnostics.Debug.Assert(tag == LatticeReplicationMetrics.BootstrapFallbackSkipDisabled);
```
