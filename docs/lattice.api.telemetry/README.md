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

    // Fail closed: serve only the metrics named here.
    options.MetricAccess = LatticeTelemetryMetricAccessMode.DenyAllExceptAllowed;
    options.AllowedMetrics.Add("lattice_tree_entries");

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
| `Credential` | `LatticeTelemetryBackendCredential?` | `null` | The static credential for `Bearer`, `Basic`, or `MutualTls`. Required whenever `AuthMode` is not `None`. |
| `RequestTimeout` | `TimeSpan` | 30 seconds | Per-request timeout against the backend. |
| `MaxRange` | `TimeSpan` | 24 hours | The widest window a range query may evaluate. |
| `MaxStep` | `TimeSpan` | 1 hour | The coarsest step a range query may request. |
| `MetricAccess` | `LatticeTelemetryMetricAccessMode` | `ReadAll` | `ReadAll`, or `DenyAllExceptAllowed` to serve only `AllowedMetrics`. |
| `AllowedMetrics` | `IList<string>` | empty | The allow-list consulted under `DenyAllExceptAllowed`. Each entry is an exact metric name or a `*` wildcard pattern (for example `lattice_wal_*`). Ignored under `ReadAll`. |

The proxy stamps the configured backend credential on every backend request and
**never** forwards the caller's Lattice credential to it: the caller-side grant
and the backend-side credential are two independent halves of the trust boundary.

## The allow-list is enforced on extracted names, not on the raw string

Under `DenyAllExceptAllowed`, every metric name a query will actually evaluate is
extracted from its PromQL by `PromQlMetricExtractor` and checked against
`AllowedMetrics` (each entry an exact name or a `*` wildcard pattern such as
`lattice_wal_*`).

The extractor is **deliberately conservative rather than a full PromQL parser**:
it recognises an identifier as a metric name only where one may legally appear -
not when followed by `(`, not a keyword or aggregation operator, not inside a
string or a numeric/duration literal, and not inside a `{...}` label matcher
unless it is the reserved `__name__` label. Erring towards extracting more,
rather than fewer, names is what keeps it fail-closed: a name it cannot resolve
is refused, not admitted.

Two rules are load-bearing, because the extractor and the backend must agree
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
  `__name__`.

## `GetCatalogAsync` degrades; it does not fail

Discovery **never surfaces a backend fault**. An unconfigured backend, and a caller entitled to no query, both receive `TelemetryQueryCatalog.Empty` rather than an exception, so a client renders no panels instead of erroring - and the two cases stay indistinguishable, so a refusal leaks nothing about the deployment.

**This is load-bearing for callers, and changing it would break them silently.** A client may therefore treat a transport-level `Unavailable` from `GetCatalog` as *the surface is unreachable*, because a mere metrics-store outage cannot produce one. The Explorer's telemetry client relies on exactly that to tell "the telemetry add-on is not installed" apart from "the metrics backend is having a bad minute" - the first hides the surface, the second shows a retryable error on it.

If this method were ever changed so that a backend fault could escape it, that client would begin hiding the telemetry surface during ordinary metrics outages, telling an operator to install something they already have. No test in the client would catch it, because the client's tests exercise its own classification rather than this contract. Treat the degradation as part of the published behaviour of `ILatticeTelemetry`, not as an implementation detail of the current backend.

## Tenant scope is derived, never accepted

`TelemetryTenantScopeResolver` decides the effective scope from the caller's own
identity. A request may state a *preference*; the resolver may refuse it. The
response reports what actually happened:

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

**A binding must not forward `TelemetryBackendException.Message` to a remote
caller.** It embeds the underlying transport fault, which routinely carries the
backend host and port. Log it, and answer with a fixed reason naming only what
the caller already supplied.

## See also

- [`Orleans.Lattice.Api.Telemetry.Grpc`](../lattice.api.telemetry.grpc/README.md) - the gRPC binding that exposes this facade to a remote head.
- [Writing an Explorer plugin](../lattice.explorer/writing-a-plugin.md) - the Explorer's telemetry panels consume this through a client seam.
