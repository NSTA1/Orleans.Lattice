# Orleans.Lattice.Api.Telemetry

A **backend-neutral telemetry facade** for a Lattice cluster. It answers a small,
curated set of named queries over a Prometheus-compatible metrics backend, scopes
every answer to the caller's tenant on the server, and refuses anything outside
the curated set.

It exists because the Explorer's desktop head cannot enforce tenant scoping
locally - a head that derived its own tenant filter would be asking the client to
police its own access. So the facade derives the scope, the head renders whatever
the server pinned, and the backend is never reachable directly.

## What it is not

It is not a query proxy. A caller names a **query id** from a catalogue; it never
supplies PromQL. There is no wire field through which query text could arrive, so
the discovery surface and the execution surface cannot disagree about what is
runnable.

## Registration

```csharp verify
using System;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Telemetry;

var services = new ServiceCollection();

services.Configure<LatticeTelemetryOptions>(options =>
{
    options.BackendAddress = new Uri("https://metrics.internal:9090");
    options.AuthMode = LatticeTelemetryBackendAuthMode.Bearer;
    options.Credential = new LatticeTelemetryBackendCredential { BearerToken = "backend-token" };

    // Fail closed: serve only the metrics named here.
    options.MetricAccess = LatticeTelemetryMetricAccessMode.DenyAllExceptAllowed;
    options.AllowedMetrics.Add("orleans_lattice_shard_*");

    // Refuse a window the backend would answer at ruinous cost.
    options.MaxRange = TimeSpan.FromHours(24);
    options.MaxStep = TimeSpan.FromMinutes(5);
});

services.AddLatticeTelemetryApi();
```

`AddLatticeTelemetryApi()` is idempotent and wires the backend client itself, so a
transport binding layered on top neither repeats nor reconfigures it.

## Options

| Property | Type | Default | Meaning |
|---|---|---|---|
| `BackendAddress` | `Uri?` | `null` | The Prometheus-compatible endpoint. Must be absolute. Unset means no backend is configured, and every query reports as unoffered. |
| `AuthMode` | `LatticeTelemetryBackendAuthMode` | `None` | `None`, `Bearer`, `Basic`, `MutualTls`, or `DynamicBearer` (a token resolved per request through `ITelemetryBackendTokenProvider`). |
| `Credential` | `LatticeTelemetryBackendCredential?` | `null` | The static credential for `Bearer`, `Basic`, or `MutualTls`. Required for those three modes; not consulted under `None` or `DynamicBearer`. |
| `RequestTimeout` | `TimeSpan` | 30 seconds | Per-request timeout against the backend. |
| `MaxRange` | `TimeSpan` | 24 hours | The widest window a range query may evaluate. |
| `MaxStep` | `TimeSpan` | 1 hour | The coarsest step a range query may request. |
| `MetricAccess` | `LatticeTelemetryMetricAccessMode` | `ReadAll` | `ReadAll`, or `DenyAllExceptAllowed` to serve only `AllowedMetrics`. |
| `AllowedMetrics` | `IList<string>` | empty | The allow-list consulted under `DenyAllExceptAllowed`. Each entry is an exact metric name or a `*` wildcard pattern (for example `orleans_lattice_wal_*`). Ignored under `ReadAll`. |

The proxy stamps the configured backend credential on every backend request and
**never** forwards the caller's Lattice credential to it: the caller-side grant
and the backend-side credential are two independent halves of the trust boundary.

`AddLatticeTelemetryApi()` registers no options validation - the host owns binding
and validating the options. Register `LatticeTelemetryOptionsValidator` as an
`IValidateOptions<LatticeTelemetryOptions>` to enforce the rules above (an absolute
backend address, the credential member each static auth mode needs, strictly
positive timeout and guardrails, and a non-empty allow-list under
`DenyAllExceptAllowed`) when the options are first resolved.

## The allow-list is enforced on extracted names, not on the raw string

Under `DenyAllExceptAllowed`, every metric name a query will actually evaluate is
extracted from its PromQL by `PromQlMetricExtractor` and checked against
`AllowedMetrics` (each entry an exact name or a `*` wildcard pattern such as
`orleans_lattice_wal_*`).

The extractor is **deliberately conservative rather than a full PromQL parser**:
it recognises an identifier as a metric name only where one may legally appear -
not when followed by `(`, not inside a string or a numeric/duration literal, and
not inside a `{...}` label matcher unless it is the reserved `__name__` label. A
keyword is skipped only where Prometheus reads it as one: Prometheus also accepts
the aggregation operators, `and` / `or` / `unless`, `by`, `without`, `offset`,
`start`, and `end` as a bare metric name wherever an operand is expected, so
`up or min` evaluates the metric `min`, and the extractor reports it as a
referenced name. Erring towards extracting more,
rather than fewer, names is what keeps it fail-closed: a name it cannot resolve
is refused, not admitted.

Three rules are load-bearing, because the extractor and the backend must agree
about what will be evaluated:

- a `#` comment is discarded as whitespace exactly as Prometheus's own lexer
  discards it, before any string or brace state is entered. An earlier version
  had no rule for `#`, so a quote opened inside a comment was scanned as a string
  opener and swallowed the rest of the query - hiding a metric name from the
  allow-list that the backend then evaluated anyway;
- an exact `__name__="up"` matcher contributes its value as a referenced name,
  while a regex (`__name__=~`) or negative (`__name__!=`, `__name__!~`) matcher
  cannot be reduced to a fixed set, so it sets
  `PromQlMetricReferences.HasUnresolvableNameMatcher` and the gate fails closed.
  That shuts the bypass where a caller named a denied series only through
  `__name__`;
- a top-level `{...}` label selector - terminated or not - that is neither
  anchored to a metric name nor pinned by an exact `__name__` matcher, such as
  the right-hand side of `up or {job="api"}`, selects series across every metric
  name, so it sets `PromQlMetricReferences.HasUnconstrainedSelector` and the gate
  fails closed even when the expression also names an admitted metric.
  A query from which no metric name can be extracted at all is refused too.

## Facade surface

`ILatticeTelemetry` has two methods:

| Method | Signature |
|---|---|
| `GetCatalogAsync` | `Task<TelemetryQueryCatalog> GetCatalogAsync(CancellationToken cancellationToken = default)` |
| `QueryAsync` | `Task<TelemetryQueryResponse> QueryAsync(TelemetryQueryRequest request, CancellationToken cancellationToken = default)` |

## The curated catalogue

`LatticeTelemetryQueries.Definitions` is the complete built-in catalogue: fifteen
server-authored entries at catalogue revision `LatticeTelemetryQueries.Version`
(`1`), in ascending query-id order. Each entry's `TelemetryQueryDescriptor` names
the OpenTelemetry instruments it reads (for example `orleans.lattice.shard.reads`);
its PromQL template reads the Prometheus exposition names the backend holds when
the host publishes through the OpenTelemetry Prometheus exporter - `orleans_lattice_*`,
with the `_total` suffix on counters and the unit word on histogram buckets
(`_milliseconds_bucket`).

| Query id | Kind | Unit | Prometheus series read |
|---|---|---|---|
| `tenant.quota.byte_utilization` | Instant | `1` | `orleans_lattice_tenancy_usage_bytes`, `orleans_lattice_tenancy_quota_bytes` |
| `tenant.usage.bytes` | Instant | `By` | `orleans_lattice_tenancy_usage_bytes` |
| `tree.admission.utilization` | Instant | `1` | `orleans_lattice_admission_utilization` |
| `tree.atomic_write.outcome_rate` | Range | `{saga}/s` | `orleans_lattice_atomic_write_completed_total` |
| `tree.cache.hit_ratio` | Range | `1` | `orleans_lattice_cache_hits_total`, `orleans_lattice_cache_misses_total` |
| `tree.read.operation_rate` | Range | `{op}/s` | `orleans_lattice_shard_reads_total` |
| `tree.scan.latency_p95` | Range | `ms` | `orleans_lattice_leaf_scan_duration_milliseconds_bucket` |
| `tree.storage.bytes` | Instant | `By` | `orleans_lattice_storage_total_bytes` |
| `tree.storage.bytes_trend` | Range | `By` | `orleans_lattice_storage_total_bytes` |
| `tree.tombstones.created_rate` | Range | `{tombstone}/s` | `orleans_lattice_leaf_tombstones_created_total` |
| `tree.tombstones.reaped_rate` | Range | `{tombstone}/s` | `orleans_lattice_leaf_tombstones_reaped_total` |
| `tree.wal.saturation_state` | Instant | `1` | `orleans_lattice_wal_saturation_state` |
| `tree.write.latency_p95` | Range | `ms` | `orleans_lattice_leaf_write_duration_milliseconds_bucket` |
| `tree.write.operation_rate` | Range | `{op}/s` | `orleans_lattice_shard_writes_total` |
| `tree.write.record_rate` | Range | `{record}/s` | `orleans_lattice_shard_records_written_total` |

The two `tenant.*` entries read the tenancy add-on's `orleans.lattice.tenancy`
meter; on a cluster without it they evaluate cleanly and return no series. Every
`Range` entry accepts a time range, a step, and a tree filter;
`tree.storage.bytes`, `tree.admission.utilization`, and
`tree.wal.saturation_state` accept a tree filter; the `tenant.*` entries take no
parameters. Each entry also declares `TelemetryQueryBounds`: a requested step is
clamped into the entry's step budget, but a window outside the entry's bounds -
or outside the deployment-wide `MaxRange` / `MaxStep` guardrails - is refused
with `TelemetryQueryBoundsException` rather than silently narrowed.

Under `DenyAllExceptAllowed`, each entry's footprint - the exposition names its
template reads - is checked against `AllowedMetrics` once, when the catalogue is
built. An entry whose series are not all admitted is left out of the catalogue and
is unreachable by id, indistinguishable from an unknown id, so allow-list the
exposition names above (for example `orleans_lattice_shard_*`), not the dotted
instrument names. `AddLatticeTelemetryApi()` registers the built-in
`LatticeTelemetryQueryCatalog` with `TryAdd`, so a host that curates its own
queries registers a catalogue built from its own `TelemetryQueryDefinition` set
first.

## `GetCatalogAsync` degrades; it does not fail

Discovery **never surfaces a backend fault**. An unconfigured backend, and a caller entitled to no query, both receive `TelemetryQueryCatalog.Empty` rather than an exception, so a client renders no panels instead of erroring - and the two cases stay indistinguishable, so a refusal leaks nothing about the deployment.

**This is load-bearing for callers, and changing it would break them silently.** A client may therefore treat a transport-level `Unavailable` from `GetCatalog` as *the surface is unreachable*, because a mere metrics-store outage cannot produce one. The Explorer's telemetry client relies on exactly that to tell "the telemetry add-on is not installed" apart from "the metrics backend is having a bad minute" - the first hides the surface, the second shows a retryable error on it.

If this method were ever changed so that a backend fault could escape it, that client would begin hiding the telemetry surface during ordinary metrics outages, telling an operator to install something they already have. No test in the client would catch it, because the client's tests exercise its own classification rather than this contract. Treat the degradation as part of the published behaviour of `ILatticeTelemetry`, not as an implementation detail of the current backend.

## Tenant scope is derived, never accepted

`TelemetryTenantScopeResolver` decides the effective scope from the caller's own
identity. A request may state a *preference*; the resolver may refuse it. The
response's `Scope` (a `TelemetryTenantScope`) reports what actually happened:

| Field | Meaning |
|---|---|
| `RequestedVisibility` | What the caller asked for. |
| `EffectiveVisibility` | What the server granted. |
| `TenantId` | The tenant the answer is pinned to. |
| `WasDowngraded` | The request was narrowed. A UI must say so rather than silently showing less. |
| `IsCrossTenant` | The answer spans tenants, which only a platform operator can obtain. |

A caller-supplied tenant id is never trusted. A non-operator asking for a
different tenant is pinned to its own, and the answer is marked downgraded.

## Failure surface

Three exceptions, all declared in `Orleans.Lattice.Api.Abstractions` so every
transport binding can name them without referencing this package:

| Exception | Means |
|---|---|
| `TelemetryQueryNotFoundException` | The query id is unknown **or** not offered by this deployment. The two are deliberately indistinguishable, so a caller learns nothing about the deployment from a refusal. |
| `TelemetryQueryBoundsException` | A well-formed request whose window or step exceeds the guardrails. |
| `TelemetryBackendException` | The backend was unreachable, timed out, or answered unusably. Not the caller's fault. |

`QueryAsync` can also refuse the caller before any of these: it throws
`LatticeAuthorizationDeniedException` when a real access gate is registered and
the caller lacks the cluster-wide `Telemetry` capability (discovery instead
degrades to the empty catalogue), `LatticeTenantAccessDeniedException` when the
caller cannot be attributed to any tenant, and `ArgumentException` when the tree
filter contains a control character. Both refusals are core `Orleans.Lattice`
types, so a binding can name them too.

**A binding must not forward `TelemetryBackendException.Message` to a remote
caller.** It embeds the underlying transport fault, which routinely carries the
backend host and port. Log it, and answer with a fixed reason naming only what
the caller already supplied.

## See also

- [`Orleans.Lattice.Api.Telemetry.Grpc`](../lattice.api.telemetry.grpc/README.md) - the gRPC binding that exposes this facade to a remote head.
- [Writing an Explorer plugin](../lattice.explorer/writing-a-plugin.md) - the Explorer's telemetry panels consume this through a client seam.
