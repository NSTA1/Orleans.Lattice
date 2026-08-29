using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Api.Telemetry;

namespace Orleans.Lattice.Api.Mcp.Telemetry;

/// <summary>
/// The telemetry tool-module opt-in for the <c>Orleans.Lattice.Api.Mcp</c>
/// binding. Adds the MCP tool group that proxies a read-only Prometheus /
/// PromQL-compatible backend as cluster telemetry tools.
/// </summary>
public static class LatticeMcpTelemetryServiceCollectionExtensions
{
    /// <summary>
    /// Opts the cluster telemetry surface into the MCP binding: binds
    /// <see cref="LatticeApiMcpTelemetryOptions"/> from
    /// <paramref name="configure"/> and validates it, registers the default
    /// <see cref="IPrometheusQueryClient"/> backend proxy and its HTTP client, and
    /// registers the telemetry tool group so its tools are advertised to a caller
    /// holding a <c>LatticeOperation.Telemetry</c> grant. Idempotent: calling it
    /// more than once registers exactly one tool group and one backend client. The
    /// host must also have called <c>AddLatticeMcp</c> for the tools to be
    /// reachable.
    /// </summary>
    /// <remarks>
    /// The backend proxy, the metric-access policy, and the range guardrails come
    /// from the transport-neutral <c>Orleans.Lattice.Api.Telemetry</c> package
    /// through
    /// <see cref="LatticeApiTelemetryServiceCollectionExtensions.AddLatticeTelemetryBackend"/>,
    /// so this binding contributes only the MCP tool surface over them. The proxy
    /// stamps the configured <b>backend</b> credential (bearer, basic, dynamic
    /// bearer, or mutual-TLS) selected by
    /// <see cref="LatticeTelemetryOptions.AuthMode"/> and never forwards the
    /// caller's Lattice credential to the backend: MCP-side authorization and the
    /// backend credential are the two independent halves of the trust boundary.
    /// </remarks>
    /// <param name="services">The host's service collection.</param>
    /// <param name="configure">
    /// Delegate that populates <see cref="LatticeApiMcpTelemetryOptions"/> (at
    /// minimum the backend address and, for a non-<c>None</c> auth mode, the
    /// backend credential).
    /// </param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddTelemetryTools(
        this IServiceCollection services,
        Action<LatticeApiMcpTelemetryOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        services.Configure(configure);
        services.TryAddEnumerable(ServiceDescriptor.Singleton<
            IValidateOptions<LatticeApiMcpTelemetryOptions>,
            LatticeApiMcpTelemetryOptionsValidator>());

        // The neutral machinery is bound to IOptions<LatticeTelemetryOptions>.
        // Forward it to the very same instance this binding's own options resolve
        // to, so the host configures and validates one object and the proxy, the
        // policy, and the guardrails all observe exactly those values.
        services.TryAddSingleton<IOptions<LatticeTelemetryOptions>>(
            static provider => Options.Create<LatticeTelemetryOptions>(
                provider.GetRequiredService<IOptions<LatticeApiMcpTelemetryOptions>>().Value));

        services.AddLatticeTelemetryBackend();

        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<ILatticeApiMcpToolGroup, TelemetryToolGroup>());

        return services;
    }
}
