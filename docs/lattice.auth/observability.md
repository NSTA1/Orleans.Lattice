# Authorization observability

`Orleans.Lattice.Auth` publishes its telemetry on a single [.NET meter](https://learn.microsoft.com/dotnet/core/diagnostics/metrics) and offers an optional durable audit trail. This page catalogues both, and documents the reserved subject-resolution-cache counters honestly so a dashboard never claims a metric that does not emit.

## The meter

Every authorization instrument is published on one meter, named by the `LatticeAuthMetrics.MeterName` constant, so an OpenTelemetry pipeline can subscribe once and receive every authorization metric:

```text
orleans.lattice.auth
```

Recording is guarded by each instrument's `Enabled` flag: when no listener is attached the gate builds no tag list and does no measurement work, so the meter is zero-cost on the hot path when nobody is listening.

### Instruments

| Instrument | Name | Kind | Emitted? | Meaning |
|---|---|---|---|---|
| Decisions | `orleans.lattice.auth.decisions` | Counter | Yes | One per gated decision (allow or deny), including bootstrap-admin bypasses and strict-fence denials. |
| Decision duration | `orleans.lattice.auth.decision.duration` | Histogram (ms) | Yes | Gate-entry-to-decision latency. |
| Snapshot rebuilds | `orleans.lattice.auth.snapshot.rebuilds` | Counter | Yes | One per successful compiled-policy snapshot rebuild. |
| Snapshot epoch | `orleans.lattice.auth.snapshot.epoch` | Observable gauge | Yes (on scrape) | The current compiled-policy epoch. |
| Snapshot age | `orleans.lattice.auth.snapshot.age` | Observable gauge | Yes (on scrape) | Age of the current compiled snapshot. |
| Subject-cache hits | `orleans.lattice.auth.subject_cache.hits` | Counter | **No (reserved)** | See [Reserved subject-cache counters](#reserved-subject-cache-counters). |
| Subject-cache misses | `orleans.lattice.auth.subject_cache.misses` | Counter | **No (reserved)** | See [Reserved subject-cache counters](#reserved-subject-cache-counters). |

### Tags

The decision counter and the decision-latency histogram carry three tags:

| Tag | Constant | Values |
|---|---|---|
| `operation` | `LatticeAuthMetrics.TagOperation` | The authorized `LatticeOperation`. |
| `tree` | `LatticeAuthMetrics.TagTree` | The target tree id. |
| `effect` | `LatticeAuthMetrics.TagEffect` | `allow` or `deny`. |

## The audit sink

Beyond aggregate metrics, an operator can capture a per-decision **audit trail** by enabling the audit sink and registering an `ILatticeAuthAuditSink`. The gate hands each decision to the sink off the request path (it observes the returned task but does not await it), so auditing never adds latency to the operation.

Configure the sink through options:

```csharp verify
siloBuilder.AddLatticeAuth(options =>
{
    options.EnableAuditSink = true;

    // DenyOnly (default) records only refusals; AllDecisions records every
    // gated decision at materially higher volume.
    options.AuditVerbosity = LatticeAuthAuditVerbosity.AllDecisions;

    // Sample a fraction of eligible events (1.0 = every event).
    options.AuditSamplingRatio = 1.0;

    // Optionally persist a durable audit trail in a reserved lattice tree.
    options.EnableDurableAuditTrail = true;
});
```

Provide the sink implementation:

```csharp verify
public sealed class ConsoleAuditSink : ILatticeAuthAuditSink
{
    public ValueTask WriteAsync(
        LatticeAuthDecisionEvent decisionEvent,
        CancellationToken cancellationToken = default)
    {
        // decisionEvent carries the subject, operation, tree, key/range, the
        // decided effect, the matched rule id and scope, the policy epoch, and
        // a UTC timestamp - everything needed for an access record.
        return ValueTask.CompletedTask;
    }
}
```

Register the sink in DI so the gate picks it up:

```csharp
siloBuilder.Services.AddSingleton<ILatticeAuthAuditSink, ConsoleAuditSink>();
```

A `LatticeAuthDecisionEvent` exposes the subject id, the operation, the tree id, the effect, the matched rule id and scope, the policy epoch, an optional key / range, an optional reason, and the UTC timestamp.

## Reserved subject-cache counters

The meter declares two subject-resolution cache counters, `orleans.lattice.auth.subject_cache.hits` and `orleans.lattice.auth.subject_cache.misses`. **They are a reserved seam and are not emitted in this version.** The instruments and their record entry points are published so operators and custom exporters can bind to a stable name, and so a future version can begin emitting without a surface change, but nothing in the shipped subject-resolution pipeline records through them, so both counters read zero.

The reason the wiring is deferred rather than shipped is layering. The subject-resolution cache that would source the hit/miss signal lives in the `Orleans.Lattice.Membership` package. Membership sits **below** authorization in the package graph and deliberately does not reference the authorization meter; making it do so would introduce a backward package dependency and a reference cycle. Emitting real hit/miss values therefore awaits a dedicated cross-package instrumentation seam rather than a layering violation. Until that seam exists, a dashboard should treat these two counters as always-zero placeholders. Membership's resolution cache TTL is still configurable through `LatticeMembershipOptions.ResolutionCacheTtl`; only the hit/miss counters are unwired.

## See also

- [Security posture](security-posture.md) - includes the measured enforcement cost per operation.
- [`Orleans.Lattice.Auth`](README.md) - the authorization concepts these instruments observe.
