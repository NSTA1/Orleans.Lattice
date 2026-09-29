using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Telemetry;
using Orleans.Lattice.Explorer.UI.Areas.Telemetry;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Telemetry;

/// <summary>
/// The bUnit context the Telemetry area is tested under: the Shell registered as a
/// head registers it, with the telemetry facade replaced by a <see cref="FakeTelemetry"/>
/// so nothing dials the configured endpoint.
/// </summary>
public abstract class TelemetryTestContext : ShellChromeTestContext
{
    /// <summary>Replaces the transport's telemetry adapter with the fake.</summary>
    protected TelemetryTestContext()
    {
        Telemetry = new FakeTelemetry();

        // Registered after the Shell, so this singleton is the one the circuit resolves.
        Services.AddKeyedSingleton<ILatticeTelemetry>(ShellFacades.Key, Telemetry);

        // The chrome context may clear the real areas (glue PR #3864); this fixture
        // tests the Telemetry area, so it registers it again (idempotent).
        Services.AddExplorerArea<TelemetryArea>();
    }

    /// <summary>The scripted telemetry facade.</summary>
    internal FakeTelemetry Telemetry { get; }

    /// <summary>Navigates to <paramref name="relative"/> and renders the Telemetry page there.</summary>
    /// <param name="relative">The base-relative address, such as <c>telemetry/latency?range=1h</c>.</param>
    /// <param name="band">The width band the layout cascades.</param>
    /// <returns>The rendered page.</returns>
    internal IRenderedComponent<TelemetryPage> RenderPage(string relative, LtBreakpoint band = LtBreakpoint.Expanded)
    {
        Navigation.NavigateTo(relative);
        return Render<TelemetryPage>(parameters => parameters.AddCascadingValue(LtBreakpointCascade.Name, band));
    }

    /// <summary>The circuit's shared telemetry catalogue.</summary>
    internal TelemetryCatalogCache CatalogCache => Services.GetRequiredService<TelemetryCatalogCache>();
}
