using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Api.Telemetry;

namespace Orleans.Lattice.Api.Mcp.Telemetry.Azure;

/// <summary>
/// Registers the Azure managed-identity backend-token provider that satisfies the
/// telemetry proxy's <see cref="LatticeTelemetryBackendAuthMode.DynamicBearer"/>
/// mode, letting any telemetry binding authenticate to an Azure Monitor
/// managed-Prometheus query endpoint with a rotating Entra access token.
/// </summary>
/// <remarks>
/// The seam this satisfies - <see cref="ITelemetryBackendTokenProvider"/> - belongs
/// to the transport-neutral <c>Orleans.Lattice.Api.Telemetry</c> facade, so this
/// package takes no dependency on the MCP server surface: a client head that hosts
/// the facade directly gets the Azure token provider without the MCP binding.
/// </remarks>
public static class LatticeAzureTelemetryTokenServiceCollectionExtensions
{
    /// <summary>
    /// Binds and validates <see cref="AzureTelemetryBackendTokenOptions"/> from
    /// <paramref name="configure"/> and registers the
    /// <see cref="ITelemetryBackendTokenProvider"/> the telemetry proxy consults in
    /// <see cref="LatticeTelemetryBackendAuthMode.DynamicBearer"/> mode. Call this
    /// alongside whichever call registers the telemetry backend - <c>AddTelemetryTools</c>
    /// for the MCP binding, or
    /// <see cref="LatticeApiTelemetryServiceCollectionExtensions.AddLatticeTelemetryBackend"/>
    /// for a head that hosts the neutral facade directly - which must set
    /// <see cref="LatticeTelemetryOptions.AuthMode"/> to
    /// <see cref="LatticeTelemetryBackendAuthMode.DynamicBearer"/>. Idempotent: a
    /// second call rebinds the options but registers exactly one provider, and a
    /// host that registered its own <see cref="ITelemetryBackendTokenProvider"/>
    /// first keeps it.
    /// </summary>
    /// <param name="services">The host's service collection.</param>
    /// <param name="configure">
    /// Delegate that populates <see cref="AzureTelemetryBackendTokenOptions"/> - at
    /// minimum the <see cref="AzureTelemetryBackendTokenOptions.Credential"/>.
    /// </param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddAzureTelemetryBackendToken(
        this IServiceCollection services,
        Action<AzureTelemetryBackendTokenOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        services.Configure(configure);
        services.TryAddEnumerable(ServiceDescriptor.Singleton<
            IValidateOptions<AzureTelemetryBackendTokenOptions>,
            AzureTelemetryBackendTokenOptionsValidator>());
        services.TryAddSingleton<ITelemetryBackendTokenProvider, AzureTelemetryBackendTokenProvider>();

        return services;
    }
}
