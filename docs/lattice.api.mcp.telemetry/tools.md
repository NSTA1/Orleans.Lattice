# Tools

The telemetry module exposes four **read-only** tools, all named `lattice_telemetry_<verb>`. They appear only when the host has called `AddTelemetryTools(...)` and the authenticated caller holds a `LatticeOperation.Telemetry` grant. Every tool carries `readOnlyHint = true` and `destructiveHint = false`; there are no write, delete, or control verbs.

## Opting in

```csharp verify
using Orleans.Lattice.Api.Mcp.Telemetry;

var services = new ServiceCollection();
services.AddLatticeMcp();
services.AddTelemetryTools(o =>
    o.BackendAddress = new Uri("https://prometheus.internal:9090/"));
```

There is no destructive opt-in flag, unlike the data, backup, and auth modules: adding the module advertises exactly the four read-only tools below to a granted caller.

## The tools

| Tool | Purpose |
|---|---|
| `lattice_telemetry_query` | Evaluate a PromQL expression at a single instant, returning the projected vector or scalar series. |
| `lattice_telemetry_query_range` | Evaluate a PromQL expression over a time range at a fixed resolution, returning the projected matrix series. The range and step are bounded by the configured guardrails. |
| `lattice_telemetry_list_metrics` | List the backend metric names, filtered to those the metric-access policy admits under the deny-all posture. |
| `lattice_telemetry_metric_metadata` | Read backend metadata (type, help text, and unit) for a named metric, or for every admitted metric when none is named. |

### `lattice_telemetry_query`

| Argument | Type | Purpose |
|---|---|---|
| `query` | `string` | The PromQL expression to evaluate at a single instant, for example `up` or `rate(lattice_wal_append_total[5m])`. |
| `time` | `DateTimeOffset?` | Optional evaluation timestamp; `null` evaluates at the backend's current time. |

Returns a `TelemetryQueryResult`: on success, the backend `ResultType` (`vector`, `matrix`, `scalar`, or `string`) and the projected `Series`; on failure, `Success = false` and an `Error`.

### `lattice_telemetry_query_range`

| Argument | Type | Purpose |
|---|---|---|
| `query` | `string` | The PromQL expression to evaluate across the range. |
| `start` | `DateTimeOffset` | The inclusive start of the range. |
| `end` | `DateTimeOffset` | The inclusive end of the range; must be at or after `start`. |
| `step` | `TimeSpan` | The resolution step between evaluation points. Must be strictly positive. |

Returns a `TelemetryQueryResult` (a `matrix`). The call is rejected with a clean `Success = false` result - without ever hitting the backend - when `end < start`, when `step <= 0`, when `end - start` exceeds `MaxRange`, or when `step` exceeds `MaxStep`.

### `lattice_telemetry_list_metrics`

Takes no arguments. Returns a `TelemetryMetricListResult` carrying the backend metric names. Under `DenyAllExceptAllowed` the list is filtered to the admitted names; under `ReadAll` it is returned whole.

### `lattice_telemetry_metric_metadata`

| Argument | Type | Purpose |
|---|---|---|
| `metric` | `string?` | Optional metric name to look up; `null` returns metadata for every metric the caller may see. |

Returns a `TelemetryMetricMetadataResult` carrying, per metric, its `Type`, `Help`, and `Unit`. A named metric the allow-list does not admit is rejected with a clean failure; an unnamed call returns only the admitted metrics. This lookup keys on the **OTEL base instrument name**, not the Prometheus exposition name that `list_metrics` returns, so pass the base name and drop any `_total`/`_bucket`/`_count`/`_sum` suffix. A named lookup that resolves to no metadata comes back successful but empty, carrying a `Notice` advisory that flags the likely exposition-name-versus-base-name mismatch; and a `404` from the backend metadata endpoint degrades to a successful empty result rather than a failure.

## Result shapes

The tool results are plain records projected to structured JSON by the MCP SDK (no Orleans serialization attributes). Each result carries a `Success` flag and, on failure, an `Error`:

- `TelemetryQueryResult` - `Success`, `Error`, `ResultType`, and `Series` (each `TelemetrySeries` carries its `Labels` and its `Samples`, and each `TelemetrySample` a `Timestamp` and `Value`).
- `TelemetryMetricListResult` - `Success`, `Error`, and `Metrics`.
- `TelemetryMetricMetadataResult` - `Success`, `Error`, `Metrics` (each `TelemetryMetricMetadata` carries `Metric`, `Type`, `Help`, and `Unit`), and a non-fatal `Notice` advisory. `Notice` is populated when a named lookup resolves to no metadata - typically because a Prometheus exposition name (with a `_total`/`_bucket`/`_count`/`_sum` suffix) was passed where the OTEL base instrument name is expected - so an unrecognised name is distinguishable from an admitted-but-genuinely-empty listing.

A backend timeout, HTTP failure, non-success status, or malformed payload is caught and surfaced on `Error`, so the agent always observes a structured result rather than a transport fault. The one deliberate exception is `metric_metadata`: a `404` from the backend metadata endpoint degrades to `Success = true` with an empty `Metrics` list rather than an error. A genuine caller cancellation still propagates.

## Next

- [Security](security.md) - the dual-credential trust boundary and the metric-access allow-list.
- [Setup](setup.md) - registering the module, the backend, and the guardrails.
