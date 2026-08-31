using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Starts and owns the second Explorer web head the end-to-end journeys drive: the
/// same shipped shell, composed over a demo cluster's worth of facts so a journey can
/// reach the state it is about.
/// <para>
/// It is a separate head rather than a reconfiguration of the default one on purpose.
/// The accessibility sweep measures the shell in its disconnected, signed-out,
/// single-area state and every one of its assertions is calibrated to that; adding
/// tenants, trees and a second area to the shared head would silently move what those
/// fixtures measure. Two heads keep the two concerns independent, and this one starts
/// only when a journey fixture runs.
/// </para>
/// <para>
/// A <c>[SetUpFixture]</c> in this namespace runs in addition to - not instead of -
/// the assembly's existing one, so the Playwright browser is already launched by the
/// time this starts and is shared.
/// </para>
/// </summary>
[SetUpFixture]
public sealed class JourneyAppHostSetup
{
    /// <summary>The path of the test-only endpoint that withdraws a tenant from the world.</summary>
    internal const string WithdrawTenantPath = "/journey-world/withdraw-globex";

    /// <summary>The path of the test-only endpoint that restores the starting world.</summary>
    internal const string ResetWorldPath = "/journey-world/reset";

    private static ExplorerAppHost? _host;
    private static JourneyWorld? _world;

    /// <summary>The running journey web head.</summary>
    internal static ExplorerAppHost Host =>
        _host ?? throw new InvalidOperationException(NotStarted);

    /// <summary>The demo cluster's mutable facts, shared with the running head.</summary>
    internal static JourneyWorld World =>
        _world ?? throw new InvalidOperationException(NotStarted);

    private const string NotStarted =
        "The journey web head is not running. This member is only valid while "
        + nameof(JourneyAppHostSetup)
        + " one-time setup has completed and before its teardown.";

    /// <summary>Starts the journey head once, before any journey fixture runs.</summary>
    [OneTimeSetUp]
    public async Task StartAsync()
    {
        var world = new JourneyWorld();
        _world = world;
        _host = await ExplorerAppHost.StartAsync(
            services => ConfigureJourneyServices(services, world),
            MapWorldControls);
    }

    /// <summary>Stops the journey head after every journey fixture has run.</summary>
    [OneTimeTearDown]
    public async Task StopAsync()
    {
        if (_host is not null)
        {
            await _host.DisposeAsync();
        }

        _host = null;
        _world = null;
    }

    /// <summary>
    /// Registers the journey world's seams. Every one of these is a contract the
    /// product publishes and registers with <c>TryAdd</c>, so registering here - before
    /// the head registers its own default - is enough to win, with no reflection and no
    /// service-descriptor surgery.
    /// </summary>
    private static void ConfigureJourneyServices(IServiceCollection services, JourneyWorld world)
    {
        services.AddSingleton(world);

        // Tenancy: the operator gate and the reachable-tenant list must be in the
        // collection before AddExplorerTenantView fills in the rest, or its fail-closed
        // defaults win and no picker can ever appear.
        services.TryAddScoped<IExplorerTenantOperatorGate, JourneyOperatorGate>();
        services.TryAddScoped<IExplorerAccessibleTenantSource, JourneyAccessibleTenantSource>();
        services.AddExplorerTenantView();

        // Something to open.
        services.TryAddScoped<ICatalogReader, JourneyCatalogReader>();

        // Somewhere to go: one area that is always reachable, and one whose
        // reachability genuinely follows the caller's credential through the shipped
        // four-state contract.
        services.AddExplorerPlugin(new JourneyWorkbenchPlugin());
        services.AddScoped<JourneyLedgerGate>();
        services.AddExplorerPlugin<JourneyLedgerPlugin>();

        // A detail surface wide enough to make the per-selection strip overflow at a
        // phone width, so the overflow menu's containment can be measured at all.
        services.AddExplorerPlugin(new JourneyWideSurfacePlugin());
    }

    /// <summary>
    /// Maps the two test-only endpoints a journey moves the world through. They exist
    /// because the fail-closed restore journey has to reproduce an entitlement being
    /// revoked <i>between</i> two visits, which no amount of in-page driving can do.
    /// </summary>
    private static void MapWorldControls(WebApplication app)
    {
        app.MapPost(WithdrawTenantPath, (JourneyWorld world) =>
        {
            world.WithdrawGlobex();
            return Results.NoContent();
        });

        app.MapPost(ResetWorldPath, (JourneyWorld world) =>
        {
            world.Reset();
            return Results.NoContent();
        });
    }
}
