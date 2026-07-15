using System.Text.Json;

namespace Orleans.Lattice.Api.Mcp.Telemetry;

/// <summary>
/// The envelope a Prometheus / PromQL-compatible backend returns for an instant
/// or range query. C1 models only the transport envelope; the Phase D tool layer
/// projects <see cref="Data"/> into the shapes it surfaces to callers.
/// </summary>
/// <param name="Status">
/// The backend's <c>status</c> field, <c>"success"</c> on a successful query.
/// </param>
/// <param name="Data">
/// The raw <c>data</c> payload (result type and series), left as a
/// <see cref="JsonElement"/> for Phase D to map.
/// </param>
internal readonly record struct PrometheusQueryResponse(string Status, JsonElement Data);
