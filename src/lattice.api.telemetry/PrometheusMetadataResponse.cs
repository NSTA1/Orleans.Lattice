using System.Text.Json;

namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// The envelope a Prometheus / PromQL-compatible backend returns for a metric
/// metadata lookup. This models only the transport envelope; a binding projects
/// <see cref="Data"/> into the shape it surfaces to callers.
/// </summary>
/// <param name="Status">
/// The backend's <c>status</c> field, <c>"success"</c> on a successful lookup.
/// </param>
/// <param name="Data">
/// The raw <c>data</c> payload (per-metric type, help, and unit), left as a
/// <see cref="JsonElement"/> for a binding to map.
/// </param>
public readonly record struct PrometheusMetadataResponse(string Status, JsonElement Data);
