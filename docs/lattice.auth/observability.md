# Authorization observability

`Orleans.Lattice.Auth` publishes its telemetry on a single [.NET meter](https://learn.microsoft.com/dotnet/core/diagnostics/metrics) and offers an optional durable audit trail. This page catalogues both.

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
| Snapshot subjects | `orleans.lattice.auth.snapshot.subjects` | Observable gauge | Yes (on scrape) | Distinct members (users and groups) for which a policy is configured. |

### Policy-coverage gauge

`orleans.lattice.auth.snapshot.subjects` reports how many **distinct** members - users and groups - are referenced by at least one rule in the current compiled policy. It is the count of members for which an authorization policy is configured: a user and a group that share an id count separately, and a member referenced by many rules or across many governed trees counts once. The gauge is computed when the snapshot is (re)built, so it moves in step with `snapshot.epoch`, and it reads from the compiled snapshot on scrape without touching storage. Watch it for unexpected drops (a policy edit that removed coverage) or unbounded growth (per-member rules accumulating where a group rule would do).

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

Register the sink in DI so the gate picks it up, for example with `siloBuilder.Services.AddSingleton<ILatticeAuthAuditSink, ConsoleAuditSink>();`.

A `LatticeAuthDecisionEvent` exposes the subject id, the operation, the tree id, the effect, the matched rule id and scope, the policy epoch, an optional key / range, an optional reason, and the UTC timestamp.

## Subject-resolution cache counters

The per-silo subject-resolution cache is owned by `Orleans.Lattice.Membership`, so its hit / miss counters live on the **membership** meter (`orleans.lattice.membership`), recorded at the cache itself. They are documented in [Membership observability](../lattice.membership/observability.md). The authorization meter does not carry them: putting a counter for a membership-owned cache on the auth meter would invert the package layering (membership sits below authorization), so the signal belongs where the cache lives.

## See also

- [Membership observability](../lattice.membership/observability.md) - the subject-resolution cache hit / miss counters on the `orleans.lattice.membership` meter.
- [Security posture](security-posture.md) - includes the measured enforcement cost per operation.
- [`Orleans.Lattice.Auth`](README.md) - the authorization concepts these instruments observe.
