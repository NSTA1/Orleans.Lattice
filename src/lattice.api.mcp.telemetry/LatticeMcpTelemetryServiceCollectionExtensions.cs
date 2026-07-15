using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

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
    /// The backend proxy stamps the configured <b>backend</b> credential (bearer,
    /// basic, or mutual-TLS) selected by
    /// <see cref="LatticeApiMcpTelemetryOptions.AuthMode"/> and never forwards the
    /// caller's Lattice credential to the backend: MCP-side authorization and the
    /// backend credential are the two independent halves of the trust boundary.
    /// C1 registers the tool group with no tools; Phase D contributes the
    /// metric-query tools.
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

        // The metric-access policy is built once from the bound options (its
        // wildcard patterns are precompiled), so a per-call admission check never
        // recompiles a pattern.
        services.TryAddSingleton(provider => new TelemetryMetricAccessPolicy(
            provider.GetRequiredService<IOptions<LatticeApiMcpTelemetryOptions>>().Value));

        // Default backend proxy and its HTTP client. Guarded so a host may register
        // its own IPrometheusQueryClient first and have this call defer to it, and
        // so a second AddTelemetryTools call does not register a duplicate client.
        if (services.All(d => d.ServiceType != typeof(IPrometheusQueryClient)))
        {
            services
                .AddHttpClient<IPrometheusQueryClient, PrometheusQueryClient>(ConfigureBackendClient)
                .ConfigurePrimaryHttpMessageHandler(BuildBackendHandler);
        }

        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<ILatticeApiMcpToolGroup, TelemetryToolGroup>());

        return services;
    }

    private static void ConfigureBackendClient(IServiceProvider provider, HttpClient client)
    {
        var options = provider.GetRequiredService<IOptions<LatticeApiMcpTelemetryOptions>>().Value;
        if (options.BackendAddress is not null)
        {
            client.BaseAddress = options.BackendAddress;
        }

        client.Timeout = options.RequestTimeout;
    }

    private static HttpMessageHandler BuildBackendHandler(IServiceProvider provider)
    {
        var options = provider.GetRequiredService<IOptions<LatticeApiMcpTelemetryOptions>>().Value;
        var handler = new HttpClientHandler();
        if (options.AuthMode == LatticeTelemetryBackendAuthMode.MutualTls
            && options.Credential?.ClientCertificate is { } certificate)
        {
            handler.ClientCertificates.Add(certificate);
        }

        return handler;
    }
}
