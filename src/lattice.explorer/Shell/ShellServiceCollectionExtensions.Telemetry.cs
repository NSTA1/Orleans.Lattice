using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Shell.Areas.Telemetry;
using Orleans.Lattice.Explorer.Shell.Navigation;

namespace Orleans.Lattice.Explorer.Shell;

/// <summary>The Telemetry area's registrations (A9, issue #3827).</summary>
internal static partial class ShellServiceCollectionExtensions
{
    /// <summary>
    /// Registers the Telemetry area and the circuit's shared telemetry catalogue.
    /// Both are scoped per circuit; the area binds only to the transport-neutral
    /// <c>ILatticeTelemetry</c> facade the transport layer registers.
    /// </summary>
    /// <param name="services">The service collection to register into.</param>
    static partial void AddTelemetry(IServiceCollection services)
    {
        services.TryAddScoped<TelemetryCatalogCache>();
        services.AddExplorerArea<TelemetryArea>();
    }
}
