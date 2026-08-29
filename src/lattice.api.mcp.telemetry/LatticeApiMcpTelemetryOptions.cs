using Orleans.Lattice.Api.Telemetry;

namespace Orleans.Lattice.Api.Mcp.Telemetry;

/// <summary>
/// Options for the optional <c>Orleans.Lattice.Api.Mcp.Telemetry</c> add-on: the
/// MCP-binding-specific view of the transport-neutral
/// <see cref="LatticeTelemetryOptions"/>, carrying the read-only Prometheus /
/// PromQL-compatible backend the telemetry tools proxy, the backend credential
/// presented on the far side of the dual-credential trust boundary, the
/// range-query guardrails, and the metric-access allow-list. Bound by
/// <see cref="LatticeMcpTelemetryServiceCollectionExtensions.AddTelemetryTools"/>
/// and resolvable via <c>IOptions&lt;LatticeApiMcpTelemetryOptions&gt;</c>.
/// </summary>
/// <remarks>
/// <para>
/// Every setting is inherited unchanged from <see cref="LatticeTelemetryOptions"/>,
/// which the neutral <c>Orleans.Lattice.Api.Telemetry</c> package owns along with
/// the proxy, guardrails, and allow-list that consume them. This type exists so a
/// host configures and resolves the telemetry add-on by the add-on's own name, and
/// so a future MCP-only setting has a place to land without widening the neutral
/// surface.
/// </para>
/// <para>
/// The proxy stamps the configured
/// <see cref="LatticeTelemetryOptions.Credential"/> selected by
/// <see cref="LatticeTelemetryOptions.AuthMode"/> on every backend request and
/// <b>never</b> forwards the caller's Lattice credential to the backend: the
/// MCP-side authorization (a <c>LatticeOperation.Telemetry</c> grant) and the
/// backend-side credential are two independent halves of the trust boundary.
/// </para>
/// </remarks>
public sealed class LatticeApiMcpTelemetryOptions : LatticeTelemetryOptions;
