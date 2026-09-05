# Back-pressure health check

`Orleans.Lattice.Replication` ships an ASP.NET Core `IHealthCheck` that turns the per-peer telemetry maintained by `ReplicationPeerStats` (see [observability](observability.md)) into a single `Healthy` / `Degraded` / `Unhealthy` verdict suitable for a Kubernetes readiness probe, an Azure App Service health endpoint, or any other host that consumes `Microsoft.Extensions.Diagnostics.HealthChecks`.

The check is **purely a consumer** of the existing peer telemetry surface. It does not poll, schedule, or invoke RPCs; every probe walks the in-memory `ReplicationPeerStats.Snapshot()` once and returns. A high-frequency probe (1 Hz or faster) costs roughly an `O(peers)` dictionary scan and is safe to run on the same cadence as the host's other readiness checks.

## Registration

```csharp verify
siloBuilder.AddLatticeReplication(o =>
{
    o.ClusterId = "cluster-a";
    o.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>
    {
        ["orders"] = LatticeMergeMode.LwwRegister,
    };
});

siloBuilder.Services
    .AddHealthChecks()
    .AddLatticeReplicationHealthCheck();
```

`AddLatticeReplicationHealthCheck` must be called **after** `AddLatticeReplication`: the check reads the cluster-wide singleton `ReplicationPeerStats` registered by the latter, so the registration order matters. The default registered name is `"orleans.lattice.replication"` (the same string as `LatticeReplicationHealthCheckOptions.DefaultName`). Override the name and tags when the host has more than one health check (for example, to expose the replication probe under a `ready` ASP.NET Core tag):

```csharp verify
siloBuilder.Services
    .AddHealthChecks()
    .AddLatticeReplicationHealthCheck(name: "replication", tags: new[] { "ready" });
```

## Threshold tiers

The check classifies every `(tree, peer)` pair captured in telemetry against three orthogonal signals. Each signal has a **soft** (degraded) and **hard** (unhealthy) bound; the worst per-peer classification across all signals becomes the peer's verdict, and the worst per-peer verdict becomes the aggregate probe result. Set a tier to `null` to disable that signal entirely.

| Signal | Source | Default soft | Default hard |
|---|---|---|---|
| `EntriesBehind` | `ReplicationPeerSnapshot.EntriesBehind` (WAL entries the sender has yet to ship) | 1 000 | 10 000 |
| `LastContactSeconds` | `ReplicationPeerSnapshot.LastContactSeconds` (age of last successful contact) | 30 s | 300 s |
| `ConsecutiveErrors` | `ReplicationPeerSnapshot.ConsecutiveErrors` (failure streak since last success) | 5 | 50 |

The `EntriesBehind`, `LastContactSeconds`, and `ConsecutiveErrors` tiers above classify **outbound** snapshot rows only - inbound rows carry zero `EntriesBehind` by construction and the inbound counterparts of the contact / error tiers are exposed separately as the **inbound silence** signal:

| Property | Description | Default |
|---|---|---|
| `InboundDegradedAfter` | Duration of inbound silence after which the row contributes `Degraded` to the aggregate verdict. | `Timeout.InfiniteTimeSpan` (disabled) |
| `InboundCriticalAfter` | Duration of inbound silence after which the row contributes `Unhealthy` to the aggregate verdict. | `Timeout.InfiniteTimeSpan` (disabled) |

The inbound signal is opt-in - a host that wants readiness gating on inbound liveness configures finite thresholds. A peer that this silo only ships to (and never receives from) produces no inbound rows and is excluded from this signal regardless of the configured thresholds. Inbound rows appear in the `degradedPeers` / `unhealthyPeers` arrays with the label suffix `" (inbound)"` so dashboards can distinguish them from outbound rows.

Defaults are exposed as `public static readonly` fields on `LatticeReplicationHealthCheckOptions` (`DefaultEntriesBehind`, `DefaultLastContactSeconds`, `DefaultConsecutiveErrors`). A host overrides any subset:

```csharp verify
siloBuilder.Services.Configure<LatticeReplicationHealthCheckOptions>(o =>
{
    // Tighter back-pressure bound for an interactive workload.
    o.EntriesBehind = new LatticeReplicationHealthCheckOptions.LongTier(200, 2_000);
    // Disable the contact-age signal entirely - rely on the error streak alone.
    o.LastContactSeconds = null;
});
```

A peer whose observed signal is **strictly greater than** the soft bound classifies as at least `Degraded`; **strictly greater than** the hard bound classifies as `Unhealthy` immediately - no sustained-degraded grace window applies on the hard path.

## Sustained-degraded escalation

A transient degraded blip (one or two probes) is usually noise; a sustained degraded state is a real back-pressure event. The check escalates a peer that has remained `Degraded` for longer than `UnhealthyAfter` to `Unhealthy`:

- The escalation timer starts on the transition from `Healthy` to `Degraded`.
- It is **reset to null** the moment the peer drops back below every soft bound. A subsequent re-degradation starts a fresh timer.
- A peer that hits the hard (`Unhealthy`) bound on any signal **always** reports `Unhealthy` immediately and drops any prior degraded-since record, so a future recovery starts cleanly.
- A **non-positive** `UnhealthyAfter` (`TimeSpan.Zero`, `Timeout.InfiniteTimeSpan`, or any negative) disables sustained-degraded escalation entirely - it does not mean "escalate immediately" - leaving a hard (unhealthy) bound on some signal as the only path to `Unhealthy`. For the strictest gating, use a small positive value: escalation then fires on the first probe after the one that entered the degraded tier.

Default is 60 seconds, sized to absorb one or two probe-cadence blips while escalating within an interactive operator-response window.

The per-peer "first-degraded-at" map lives on the health-check instance itself, so `AddLatticeReplicationHealthCheck` registers the check as a **singleton** on the underlying `ServiceCollection` (the default `IHealthChecksBuilder.AddCheck<T>` lifetime is transient, which would discard the escalation map on every probe). A custom registration that wants to replace the check must respect that lifetime, otherwise sustained-degraded escalation will silently stop firing.

## NaN contact samples

`ReplicationPeerSnapshot.LastContactSeconds` is `double.NaN` for a peer that has never had a successful contact recorded - either because it has only ever failed (`RecordError` without a paired `RecordSuccess`) or because it has only ever had backlog recorded against it. The check **excludes NaN samples** from the `LastContactSeconds` tier, on the principle that "we have never tried" and "we tried and failed" are operationally distinct conditions. The `ConsecutiveErrors` tier covers the latter so the two signals are orthogonal: a peer that fails its first ship attempt reports `ConsecutiveErrors = 1` and `LastContactSeconds = NaN`, contributes to the error tier, and is silent on the contact tier.

## Probe result shape

The check returns a `HealthCheckResult` whose `Data` dictionary populates a standard set of keys for a dashboard or alert rule to pivot on:

| Key | Type | Description |
|---|---|---|
| `peers` | `int` | Total number of `(tree, peer)` pairs in telemetry. |
| `degraded` | `int` | Count of peers in the degraded tier (after sustained-degraded escalation has been applied). |
| `unhealthy` | `int` | Count of peers in the unhealthy tier. |
| `degradedPeers` | `string[]` | `tree/peer` labels for every degraded peer. Present only when `degraded > 0`. |
| `unhealthyPeers` | `string[]` | `tree/peer` labels for every unhealthy peer. Present only when `unhealthy > 0`. |

The aggregate `Status` is the worst per-peer classification across the snapshot. An empty snapshot returns `Healthy` with `peers = 0` - a fresh silo that has not yet recorded any peer telemetry is by definition not in back-pressure.

## Telemetry-drop garbage collection

A peer that drops out of `ReplicationPeerStats.Snapshot()` between probes (e.g. a cross-cluster peer the operator removed from the topology) is **cleared from the degraded-since map** on the next probe. A future re-appearance starts a fresh grace window; the implementation does not retain stale "first-degraded-at" records past the point where the underlying telemetry stops including the peer.

## Related metrics

The metrics in [observability](observability.md) are the raw inputs the health check classifies:

- `orleans.lattice.replication.peer.entries_behind` feeds the `EntriesBehind` tier.
- `orleans.lattice.replication.peer.last_contact_seconds` feeds the `LastContactSeconds` tier.
- `orleans.lattice.replication.peer.consecutive_errors` feeds the `ConsecutiveErrors` tier.

A host that exports the meter to OpenTelemetry can recreate the threshold tiers as alert rules and have the health check act purely as a gate for the orchestrator's readiness probe. The meter and the health check are independent surfaces; either, both, or neither may be subscribed without impacting the other.
