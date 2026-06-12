# Automatic drift remediation (operator playbook)

Cross-cluster replication in `Orleans.Lattice.Replication` is eventually consistent: every mutation rides the per-tree write-ahead log to each peer, the receiver applies it HLC-monotonically, and concurrent edits converge through the per-tree `LatticeMergeMode`. In the steady state every cluster eventually holds the same data for a given shard. **Silent divergence** - two clusters that have applied different effective state for the same shard and stay that way - should never happen, but a transport bug, a partial garbage-collection of the WAL, or an operator mistake can produce it.

The anti-entropy stack is the safety net for that case. It is a layered pipeline that **detects** divergence, **localises** it, **repairs** it, and **guards** the repair behind operator controls. This page is the operator playbook for the stack as a whole: what each stage does, the single posture they all share (off by default), how to opt in end to end, the metrics they expose, and how to read the failure modes you will see in telemetry.

Each stage has its own reference page; this playbook links them rather than repeating their detail:

- [Anti-entropy digest probe](anti-entropy-digest-probe.md) - detection.
- [Anti-entropy Merkle-walk drift localisation](anti-entropy-merkle-walk.md) - localisation.
- [Anti-entropy targeted leaf re-replay](anti-entropy-leaf-rereplay.md) - repair from the WAL.
- [Anti-entropy bootstrap-snapshot fallback](anti-entropy-bootstrap-fallback.md) - repair when the WAL has been trimmed.
- [Anti-entropy remediation guards](anti-entropy-remediation-guards.md) - opt-in gate, rate cap, circuit breaker.

## Default-off posture

Every stage ships **dark**. With defaults unchanged, a host runs ordinary replication and nothing else: no probe scheduler, no new RPC traffic, no automatic repair, and no behaviour change. The whole stack is opt-in, stage by stage, so you can enable detection and watch drift telemetry for as long as you like before ever enabling automatic repair.

| Stage | Master flag | Default |
|---|---|---|
| Digest probe (detection) | `DigestProbeEnabled` | `false` |
| Merkle walk (localisation) | `MerkleWalkEnabled` | `false` |
| Targeted leaf re-replay (repair) | `LeafReReplayEnabled` | `false` |
| Bootstrap-snapshot fallback (repair) | `BootstrapFallbackEnabled` | `false` |
| Automatic repair master gate | `AutoRemediateOnDigestMismatch` | `false` |

The flags are layered AND-gates. Localisation only runs on a detected mismatch; repair only runs on a localised leaf; the bootstrap fallback only runs when leaf re-replay reports a trimmed WAL; and `AutoRemediateOnDigestMismatch` is an additional master gate in front of *both* repair stages. Detection and localisation are never gated by the repair controls, so you can observe drift without sending any repair traffic.

## The pipeline, stage by stage

1. **Detect.** The digest probe is a low-frequency, read-only background pass that compares each shard's local content digest against every peer's digest. A sustained `Mismatch` for a `(tree, shard, peer)` triple is the signal that those clusters have genuinely diverged. The probe never mutates data and never advances a replication cursor.
2. **Localise.** On a mismatch, the Merkle walk descends the local B+ tree top-down and narrows the divergence to a single leaf or a small set of leaves, using the clusters' one shared coordinate - separator-key ranges. It is strictly read-only.
3. **Repair from the WAL.** Targeted leaf re-replay re-ships the retained WAL entries covering the localised ranges to the diverged peer. The repair travels the same TX-aware, causal-stable apply path as ordinary replication and is de-duplicated at the receiver on `(originClusterId, hlc)`, so it is idempotent.
4. **Repair when the WAL is gone.** If the local WAL has been garbage-collected past the divergence point, re-replay cannot help. The bootstrap-snapshot fallback re-derives the committed projection of only the divergent leaf range from the live tree (which is immune to WAL trimming) and re-ships those committed rows.
5. **Guard.** The remediation guards wrap the repair stages with an operator opt-in gate, a per-`(tree, peer)` rate cap, and a per-`(tree, peer)` circuit breaker, so automatic repair is opt-in, bounded, and self-fencing.

## Opting in end to end

To enable the full pipeline including automatic repair, opt into each stage and the master gate. The byte and entry caps below bound the cost of a single pass so a pathological tree cannot turn a background safety net into an expensive scan.

```csharp verify
siloBuilder.AddLatticeReplication(o =>
{
    o.ClusterId = "cluster-a";
    o.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>
    {
        ["orders"] = LatticeMergeMode.LwwRegister,
    };

    // 1. Detection (off by default).
    o.DigestProbeEnabled = true;
    o.DigestProbeInterval = TimeSpan.FromMinutes(5);
    o.DigestProbeJitter = 0.2;

    // 2. Localisation (off by default). Runs only on a detected mismatch.
    o.MerkleWalkEnabled = true;
    o.MerkleWalkMaxDepth = 16;
    o.MerkleWalkMaxBytes = 1024 * 1024;

    // 3. Repair from the WAL (off by default). Runs only after a localised leaf.
    o.LeafReReplayEnabled = true;
    o.LeafReReplayMaxEntries = 4096;
    o.LeafReReplayMaxBytes = 1024 * 1024;

    // 4. Repair when the WAL is trimmed (off by default).
    o.BootstrapFallbackEnabled = true;
    o.BootstrapFallbackMaxEntries = 4096;
    o.BootstrapFallbackMaxBytes = 1024 * 1024;

    // 5. Guards: master gate, rate cap, circuit breaker (gate off by default).
    o.AutoRemediateOnDigestMismatch = true;
    o.RemediationTrafficBudgetFraction = 0.01;
    o.RemediationTrafficWindow = TimeSpan.FromMinutes(1);
    o.RemediationFailureThreshold = 3;
    o.RemediationCircuitResetInterval = TimeSpan.FromMinutes(5);
});
```

Two cross-cutting prerequisites apply to the repair stages:

- **A real transport.** The repair re-ship goes through `IReplicationTransport`; the default no-op transport acks but does not deliver. Wire the gRPC binding (or a custom transport) for genuine cross-cluster repair.
- **Projection-digest maintenance must be on.** Detection reads the core library's leaf-projection digest, which only exists when `MaintainProjectionDigest` is `true` (the default for user trees). A tree that opts out of digest maintenance has no digest to compare, so the entire stack is inert for it - see [the projection-rebuild digest opt-out](../lattice/projection-rebuild.md) for the cross-cluster impact.

## Metrics surface

Every stage emits on the single `orleans.lattice.replication` meter. The table below is the operator's at-a-glance index; each stage's reference page documents its tags and emission semantics in full.

| Stage | Metric | Read it as |
|---|---|---|
| Detection | `digest_probe.compared` | Every shard/peer comparison, tagged with its `outcome`. |
| Detection | `digest_probe.mismatch` | Genuine divergence for a `(tree, shard, peer)` triple. |
| Localisation | `merkle_walk.localised` | A pass narrowed the mismatch to one or more leaves. |
| Localisation | `merkle_walk.aborted` | A pass stopped before localising, tagged with its `reason`. |
| Repair (WAL) | `leaf_rereplay.entries` | WAL entries re-shipped to the peer. |
| Repair (WAL) | `leaf_rereplay.skipped` | A pass skipped without re-shipping, tagged with its `reason`. |
| Repair (snapshot) | `bootstrap_fallback.triggered` | A fallback pass began. |
| Repair (snapshot) | `bootstrap_fallback.entries` | Committed entries re-shipped to the peer. |
| Repair (snapshot) | `bootstrap_fallback.skipped` | A fallback pass skipped without re-shipping, tagged with its `reason`. |
| Guards | `digest_remediation.disabled` | An observable gauge of every `(tree, peer)` whose repair is currently disabled. |
| Guards | `digest_remediation.skipped` | A repair pass skipped before sending traffic, tagged with its `reason`. |

All metric names are exposed as constants on `LatticeReplicationMetrics` for dashboards built from the public surface. The shipped Grafana dashboard and the panel-to-metric mapping live under [observability](observability.md).

## Failure-mode matrix

The stack is designed to fail safe and to make *why* it is not repairing legible in telemetry. The three failure modes an operator most often needs to recognise:

| Failure mode | What you see | What it means | Operator action |
|---|---|---|---|
| **Version skew** | `digest_probe.compared{outcome=version_skew}` and `merkle_walk.aborted{reason=version_skew}` | The two clusters carry different contribution-function versions, so their digests are not comparable - typically a rolling upgrade in flight. No divergence is asserted and no repair is attempted. | Expected during an upgrade; it self-clears once both sides run the same version. Investigate only if it persists after the rollout completes. |
| **WAL-trimmed divergence** | `leaf_rereplay.skipped{reason=wal_trimmed}`, then either `bootstrap_fallback.triggered` (fallback on) or `bootstrap_fallback.skipped{reason=disabled}` (fallback off) | The missing writes have been garbage-collected from the local WAL, so re-replay cannot supply them. | Enable `BootstrapFallbackEnabled` so the snapshot fallback can re-derive the committed projection of the divergent range. While it is off, the divergence is detected and localised but not repaired. |
| **Circuit-breaker tripped** | `digest_remediation.disabled{reason=circuit_open}` for a `(tree, peer)`, with `digest_remediation.skipped{reason=circuit_open}` per skipped pass | Repair failed `RemediationFailureThreshold` times in a row for that pair, so the breaker opened and is fencing further repair for `RemediationCircuitResetInterval`. | Investigate the underlying repair failures (transport, peer health). The breaker half-opens after the cooldown and closes itself on a successful trial pass; no manual reset is required. |

Two further skip reasons are normal background noise rather than failures: `digest_probe.compared{outcome=remote_unavailable}` (the peer has digesting turned off for that tree) and `digest_remediation.skipped{reason=opt_out}` / `digest_remediation.disabled{reason=opt_out}` (you have not set `AutoRemediateOnDigestMismatch`, so detection runs but repair is intentionally off). A spent rate cap surfaces as `digest_remediation.skipped{reason=budget_exhausted}` and clears when the `RemediationTrafficWindow` rolls over.

## Recommended rollout

1. Enable detection alone (`DigestProbeEnabled`) on a representative tree and watch `digest_probe.mismatch`. Confirm the baseline is zero in the steady state.
2. Add localisation (`MerkleWalkEnabled`) and confirm walks complete or abort with an understood reason.
3. Wire a real transport and enable the repair stages (`LeafReReplayEnabled`, then `BootstrapFallbackEnabled`) with the guards in place, but leave `AutoRemediateOnDigestMismatch` off so you can rehearse the telemetry.
4. Flip `AutoRemediateOnDigestMismatch` last, starting with conservative `RemediationTrafficBudgetFraction` and `RemediationFailureThreshold` values, and watch `digest_remediation.disabled` to confirm the guards behave as expected under load.
