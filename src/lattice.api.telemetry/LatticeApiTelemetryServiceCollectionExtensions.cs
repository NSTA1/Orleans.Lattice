using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// Registers the transport-neutral telemetry backend proxy: the metric-access
/// policy and the default <see cref="IPrometheusQueryClient"/> over the
/// configured read-only Prometheus / PromQL-compatible backend. Every telemetry
/// binding (the MCP tool group, the gRPC service, the Explorer's client seam)
/// calls this once so the backend credential, the guardrails, and the allow-list
/// are wired identically no matter which transport is in front of them.
/// </summary>
public static class LatticeApiTelemetryServiceCollectionExtensions
{
    /// <summary>
    /// Registers the metric-access policy and, unless the host already supplied
    /// one, the default backend proxy and its <see cref="HttpClient"/>, both bound
    /// to <c>IOptions&lt;LatticeTelemetryOptions&gt;</c>. Idempotent: calling it
    /// more than once registers exactly one policy and one backend client. The
    /// caller is responsible for binding and validating
    /// <see cref="LatticeTelemetryOptions"/> (or a type derived from it).
    /// </summary>
    /// <remarks>
    /// The proxy stamps the configured <b>backend</b> credential (bearer, basic,
    /// dynamic bearer, or mutual-TLS) selected by
    /// <see cref="LatticeTelemetryOptions.AuthMode"/> and takes no dependency on
    /// any Lattice credential source, so a caller's Lattice credential can never
    /// be forwarded to the backend.
    /// </remarks>
    /// <param name="services">The host's service collection.</param>
    /// <returns>The service collection for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    public static IServiceCollection AddLatticeTelemetryBackend(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        // The metric-access policy is built once from the bound options (its
        // wildcard patterns are precompiled), so a per-call admission check never
        // recompiles a pattern.
        services.TryAddSingleton(provider => new TelemetryMetricAccessPolicy(
            provider.GetRequiredService<IOptions<LatticeTelemetryOptions>>().Value));

        // Default backend proxy and its HTTP client. Guarded so a host may register
        // its own IPrometheusQueryClient first and have this call defer to it, and
        // so a second registration call does not register a duplicate client.
        if (services.All(d => d.ServiceType != typeof(IPrometheusQueryClient)))
        {
            services
                .AddHttpClient<IPrometheusQueryClient, PrometheusQueryClient>(ConfigureBackendClient)
                .ConfigurePrimaryHttpMessageHandler(BuildBackendHandler);
        }

        return services;
    }

    private static void ConfigureBackendClient(IServiceProvider provider, HttpClient client)
    {
        var options = provider.GetRequiredService<IOptions<LatticeTelemetryOptions>>().Value;
        if (options.BackendAddress is not null)
        {
            client.BaseAddress = options.BackendAddress;
        }

        client.Timeout = options.RequestTimeout;
    }

    private static HttpMessageHandler BuildBackendHandler(IServiceProvider provider)
    {
        var options = provider.GetRequiredService<IOptions<LatticeTelemetryOptions>>().Value;
        var handler = new HttpClientHandler();
        if (options.AuthMode == LatticeTelemetryBackendAuthMode.MutualTls
            && options.Credential?.ClientCertificate is { } certificate)
        {
            handler.ClientCertificates.Add(certificate);
        }

        return handler;
    }
}
