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
| Snapshot age | `orleans.lattice.auth.snapshot.age` | Observable gauge (s) | Yes (on scrape) | Seconds since the current compiled snapshot was last rebuilt. Reports no measurement until the first rebuild. |
| Snapshot subjects | `orleans.lattice.auth.snapshot.subjects` | Observable gauge | Yes (on scrape) | Distinct members (users and groups) for which a policy is configured. |

### Policy-coverage gauge

`orleans.lattice.auth.snapshot.subjects` reports how many **distinct** members - users and groups - are referenced by at least one rule in the current compiled policy. It is the count of members for which an authorization policy is configured: a user and a group that share an id count separately, and a member referenced by many rules or across many governed trees counts once. The gauge is computed when the snapshot is (re)built, so it moves in step with `snapshot.epoch`, and it reads from the compiled snapshot on scrape without touching storage. Watch it for unexpected drops (a policy edit that removed coverage) or unbounded growth (per-member rules accumulating where a group rule would do).

### Tags

The decision counter and the decision-latency histogram carry four tags:

| Tag | Constant | Values |
|---|---|---|
| `operation` | `LatticeAuthMetrics.TagOperation` | The authorized `LatticeOperation`, as its flag name (for example `Read`); a composite mask renders as the enum's comma-separated `ToString()` form. |
| `tree` | `LatticeAuthMetrics.TagTree` | The target tree id. |
| `tenant` | `LatticeTenantLabel.TagTenant` | The owning tenant derived from `tree`. Always emitted, on tenancy-on and tenancy-off clusters alike. |
| `effect` | `LatticeAuthMetrics.TagEffect` | `allow` or `deny`. |

The snapshot-rebuild counter and the three snapshot gauges carry only the `tenant` tag, fixed to the platform sentinel `_platform_` (`LatticeTenantLabel.PlatformTenant`): the compiled snapshot is silo-wide and belongs to no tenant.

### Zero-primed effect arms

The decision counter publishes **both** effect arms at zero the first time the gate
decides a given `operation`/`tree` pair, before either outcome has occurred. A pair
that has only ever been allowed therefore still carries an `effect="deny"` series
reading `0`.

This exists because the two readings an operator most needs to tell apart were
otherwise indistinguishable. Without priming, a `deny` series is created by the first
denial, so its absence carries at least four meanings:

- the gate ran and denied nothing,
- the gate never ran for that operation or tree,
- the package was never registered on the silo,
- the path is exempt and never consults the gate.

Only the first is good news, and it is the reading an operator is least likely to
interrogate, because it arrives as a healthy dashboard. With priming, a flat zero on
`deny` is a **measured** zero, and an absent series means the gate did not evaluate
that pair at all. Alert on the absence, not only on the value.

**The boundary.** Priming is per `operation`/`tree` pair and starts at that pair's
first decision, not at silo start: the cross product of operations and trees is not
knowable before traffic arrives. A pair that has never been decided has no series of
either arm, which is the correct reading of it. Priming is also bounded: each silo's
gate primes at most 4,096 distinct `operation`/`tree` pairs, and a pair first decided
after that bound is reached falls back to the unprimed behaviour, where its `deny`
arm appears only at its first denial.

**The latency histogram is deliberately not primed.**
`orleans.lattice.auth.decision.duration` carries the same tags and has the same
absent-arm behaviour, but the remedy does not carry across. A counter is primed by
adding zero, which changes no aggregate; priming a histogram means recording a `0` ms
sample, which is a real observation that moves count, sum, and every bucket below the
first. Priming it would corrupt the distribution it exists to measure.

## The audit sink

Beyond aggregate metrics, an operator can capture a per-decision **audit trail** by enabling the audit sink and registering an `ILatticeAuthAuditSink`. The gate does not await each sink's asynchronous completion - it hands off the returned task and observes only the ones that do not complete synchronously - but the synchronous portion of a sink's `WriteAsync` runs inline on the request path, so a sink must return promptly (offload slow work itself) to avoid adding latency to the operation.

`AddLatticeAuth` always registers two built-in sinks: a logger sink that writes each decision event to the silo `ILogger` (denies at warning, allows at debug), and a durable audit-trail sink that appends events to a reserved append-only lattice tree but stays inert (writes nothing) until `EnableDurableAuditTrail` is set. Any `ILatticeAuthAuditSink` you register is **additive** - it runs alongside the built-ins, it does not replace them.

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
- [Security posture](security-posture.md) - the threat model, fail-closed guarantees, and trust boundary of the gate these instruments observe.
- [`Orleans.Lattice.Auth`](README.md) - the authorization concepts these instruments observe.
