---
agent_spec: "docs/agents/api/replication.json"
---

# Orleans.Lattice.Api.Replication configuration

The package's public options types are `LatticeApiReplicationOptions`, bound through `AddLatticeReplicationApi(configure)`, and `LatticeReplicationStatusOptions`, bound through `AddLatticeReplicationStatusApi(configure)`. Each is resolvable via `IOptions<T>`.

## `LatticeApiReplicationOptions`

The facade currently exposes no tunable knobs. The type is the stable registration front door: it lets later work add configuration without changing the `AddLatticeReplicationApi` signature. Register the facade with no options today:

```csharp verify
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Replication;

siloBuilder
    .AddLatticeReplication(options =>
    {
        options.ClusterId = "cluster-a";
    }, enableRuntimeConfig: true)
    .AddLatticeReplicationApi();
```

## `LatticeReplicationStatusOptions`

The thresholds the read-only peer-status facade (`ILatticeReplicationStatus`) uses to turn a link's measured backlog, error streak, and time since last contact into its `ReplicationLinkHealth`. Each signal has a lagging and a stalled bound. A signal strictly greater than its lagging bound makes the link at least `Lagging`, and one strictly greater than its stalled bound makes it `Stalled`; the worst signal wins. Setting a bound to `null` disables it. The no-contact bounds apply only once a link has made a successful contact, so a link that has never made one and trips no other bound is `Unknown`.

| Property | Type | Default | Meaning |
|---|---|---|---|
| `LaggingEntriesBehind` | `long?` | `1000` (`DefaultLaggingEntriesBehind`) | Outbound backlog, in WAL entries, above which a link is `Lagging`. |
| `StalledEntriesBehind` | `long?` | `10000` (`DefaultStalledEntriesBehind`) | Outbound backlog, in WAL entries, above which a link is `Stalled`. |
| `LaggingConsecutiveErrors` | `long?` | `5` (`DefaultLaggingConsecutiveErrors`) | Consecutive failed contacts, in either direction, above which a link is `Lagging`. |
| `StalledConsecutiveErrors` | `long?` | `50` (`DefaultStalledConsecutiveErrors`) | Consecutive failed contacts, in either direction, above which a link is `Stalled`. |
| `LaggingAfterNoContact` | `TimeSpan?` | 30 seconds (`DefaultLaggingAfterNoContact`) | Time since the last successful outbound contact above which an outbound link is `Lagging`. The liveness probe refreshes an idle outbound link, so silence here means the peer is not answering. |
| `StalledAfterNoContact` | `TimeSpan?` | 5 minutes (`DefaultStalledAfterNoContact`) | Time since the last successful outbound contact above which an outbound link is `Stalled`. |
| `InboundLaggingAfterNoContact` | `TimeSpan?` | `null` (disabled) | Time since the peer's entries were last applied locally above which an inbound link is `Lagging`. Off by default: an inbound link is refreshed only when the peer writes, so an idle peer is not an unhealthy one. |
| `InboundStalledAfterNoContact` | `TimeSpan?` | `null` (disabled) | Time since the peer's entries were last applied locally above which an inbound link is `Stalled`. |

The backlog bounds apply to outbound links only, because an inbound link tracks no backlog; the error-streak bounds apply in both directions. The outbound defaults match those of `LatticeReplicationHealthCheckOptions`, so the status report and the replication health check agree out of the box.

Validation rejects a negative bound, and a lagging bound greater than its stalled bound when both are set, because a misordered pair would make `Lagging` unreachable for that signal. Nothing reads the options at silo start: the facade reads them through `IOptionsMonitor` each time it builds a report, so a reloaded value applies to the next report and an invalid one fails that call with an `OptionsValidationException`.

## What is configured elsewhere

This facade drives the replication config authority but does not re-expose its configuration.

- The **static seed / fallback** replicated-tree set and the local cluster identity are configured on [`Orleans.Lattice.Replication`](../lattice.replication/configuration.md) through `LatticeReplicationOptions` (`ReplicatedTrees`, `ClusterId`).
- Which trees are **runtime-enabled** is not configuration at all: it is authored at runtime - through this facade, or by an installed app as it activates (see [`Orleans.Lattice.Apps`](../lattice.apps/README.md#replication-intent)) - and distributed as the `sys-replication-config` tree. See [runtime replication configuration](../lattice.replication/runtime-config.md).
- Transport concerns - authorization enforcement, the credential and active-tenant headers, and advertised auth schemes - live on the [gRPC binding](../lattice.api.replication.grpc/configuration.md), not here; TLS and other channel policy belong to the hosting ASP.NET Core server and the caller's gRPC channel.
