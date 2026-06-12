# Anti-entropy remediation guards (opt-in, rate cap, circuit breaker)

The anti-entropy chain detects drift (the [digest probe](anti-entropy-digest-probe.md) and the read-only [Merkle walk](anti-entropy-merkle-walk.md)) and, optionally, repairs it ([targeted leaf re-replay](anti-entropy-leaf-rereplay.md) and the [bootstrap-snapshot fallback](anti-entropy-bootstrap-fallback.md)). The remediation guards wrap the **repair** stage with three operator controls so automatic repair is opt-in, rate-limited, and self-fencing. Detection is never gated by these controls - an operator can watch drift telemetry without opting into automatic repair.

All three guards ship **dark**: with defaults unchanged, an un-opted host detects and probes drift exactly as before and attempts no automatic repair.

## 1. Operator opt-in master gate

`AutoRemediateOnDigestMismatch` (default `false`) is the master switch for *all* automatic remediation - both leaf re-replay and the bootstrap-snapshot fallback. While it is off, a localised drift records a single skip with reason `opt_out` and the `digest_remediation.disabled` gauge reports the affected `(tree, peer)`; no repair traffic is sent. It is an additional AND-gate in front of the existing per-feature flags (`MerkleWalkEnabled`, `LeafReReplayEnabled`, `BootstrapFallbackEnabled`), which still apply on top of it.

## 2. Per-(tree, peer) rate cap

When remediation is enabled, repair re-ship volume is rate-limited per `(tree, peer)` to a small fraction of the ordinary ship-batch budget. The effective per-window entry budget is `max(1, ceil(RemediationTrafficBudgetFraction * ShipBatchSize))` - about 3 entries with the defaults (`0.01 * 256`). The first pass in a fresh window always runs (so one repair burst is permitted); once a pair has spent its window budget, further passes are skipped with reason `budget_exhausted` until the `RemediationTrafficWindow` rolls over. The window is a deterministic elapsed-time accounting interval, kept in-process on the per-shard/tree digest-probe grain.

## 3. Per-(tree, peer) circuit breaker

After `RemediationFailureThreshold` consecutive failures for a `(tree, peer)`, the circuit breaker opens: remediation is skipped with reason `circuit_open` and the gauge reports the disabled state for `RemediationCircuitResetInterval`. After the cooldown the breaker half-opens and the next evaluation runs one trial pass - success closes the breaker and clears the gauge, a failed trial re-opens it for a fresh cooldown. Any success resets the consecutive-failure count. A "failure" is a remediation pass that threw or whose re-ship sink reported zero entries shipped despite candidate entries having been selected.

## Enabling it

```csharp verify
siloBuilder.AddLatticeReplication(o =>
{
    o.ClusterId = "cluster-a";
    o.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>
    {
        ["orders"] = LatticeMergeMode.LwwRegister,
    };

    // Detection + localisation + repair (all off by default).
    o.DigestProbeEnabled = true;
    o.MerkleWalkEnabled = true;
    o.LeafReReplayEnabled = true;
    o.BootstrapFallbackEnabled = true;

    // Master gate for automatic repair (off by default).
    o.AutoRemediateOnDigestMismatch = true;

    // Rate cap: 1% of ShipBatchSize per (tree, peer) per window.
    o.RemediationTrafficBudgetFraction = 0.01;
    o.RemediationTrafficWindow = TimeSpan.FromMinutes(1);

    // Circuit breaker: open after 3 consecutive failures, cool down for 5 minutes.
    o.RemediationFailureThreshold = 3;
    o.RemediationCircuitResetInterval = TimeSpan.FromMinutes(5);
});
```

| Option | Default | Notes |
|---|---|---|
| `AutoRemediateOnDigestMismatch` | `false` | Master gate for all automatic repair. When `false`, drift is detected and probed but never repaired (`digest_remediation.skipped{reason=opt_out}`). |
| `RemediationTrafficBudgetFraction` | `0.01` | Fraction of `ShipBatchSize` a `(tree, peer)` may re-ship per window. Validated to `(0.0, 1.0]`. |
| `RemediationTrafficWindow` | `00:01:00` | Deterministic accounting window for the rate cap. Validated `> TimeSpan.Zero`. |
| `RemediationFailureThreshold` | `3` | Consecutive failures that open the circuit breaker. Validated `>= 1`. |
| `RemediationCircuitResetInterval` | `00:05:00` | Cooldown before the breaker half-opens. Validated `> TimeSpan.Zero`. |

## Observability

| Metric | Tags | Emitted |
|---|---|---|
| `orleans.lattice.replication.digest_remediation.disabled` | `tree`, `peer`, `reason` | Observable gauge, value `1` for each `(tree, peer)` whose remediation is currently disabled. No series means remediation is permitted. |
| `orleans.lattice.replication.digest_remediation.skipped` | `tree`, `peer`, `reason` | Counter, once per remediation pass skipped before sending repair traffic. |

Reasons: `opt_out` (the host has not set `AutoRemediateOnDigestMismatch`), `budget_exhausted` (the per-window rate cap is spent), and `circuit_open` (the breaker tripped on consecutive failures).

The metric-name constants and the reason mapping are exposed for dashboards built from the public surface:

```csharp verify
_ = LatticeReplicationMetrics.DigestRemediationDisabledName;
_ = LatticeReplicationMetrics.DigestRemediationSkippedName;

string tag = LatticeReplicationMetrics.DigestRemediationDisabledReasonTag(RemediationDisabledReason.CircuitOpen);
System.Diagnostics.Debug.Assert(tag == LatticeReplicationMetrics.DigestRemediationReasonCircuitOpen);
```
