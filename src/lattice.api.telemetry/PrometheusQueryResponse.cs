using System.Text.Json;

namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// The envelope a Prometheus / PromQL-compatible backend returns for an instant
/// or range query. This models only the transport envelope; a binding projects
/// <see cref="Data"/> into the shapes it surfaces to callers.
/// </summary>
/// <param name="Status">
/// The backend's <c>status</c> field, <c>"success"</c> on a successful query.
/// </param>
/// <param name="Data">
/// The raw <c>data</c> payload (result type and series), left as a
/// <see cref="JsonElement"/> for a binding to map.
/// </param>
public readonly record struct PrometheusQueryResponse(string Status, JsonElement Data);
