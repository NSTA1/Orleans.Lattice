using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Telemetry;
using Orleans.Lattice.Explorer.Plugins.Telemetry.Workspace;

namespace Orleans.Lattice.Explorer.Tests.Telemetry;

/// <summary>
/// The Telemetry area's selected panel, carried on the shell's declared state
/// contract: remembered between visits, addressable in a link, and validated
/// against the catalogue the cluster is offering now.
/// </summary>
/// <remarks>
/// <para>
/// The acceptance criterion is "sub-surface and query state are URL-addressable
/// with lower-case paths and persist", and the issue names this preference
/// specifically: it existed as a bare string constant that nothing read, and had
/// to move onto the documented contract rather than an ad hoc <c>SetAsync</c>.
/// </para>
/// <para>
/// The real session stack is used rather than a substitute: the route model is a
/// pure in-memory type and the preference store falls back to an in-memory
/// backing store, so nothing reaches a browser, and the workspace is exercised
/// against the contract it actually ships against.
/// </para>
/// <para>
/// Every load answers synchronously from a scripted domain and the clock is
/// pinned, so nothing here depends on timing, ordering, a wall clock, or garbage
/// collection.
/// </para>
/// </remarks>
[TestFixture]
public sealed class TelemetrySelectedQueryStateTests
{
    private const string First = "q-first";
    private const string Second = "q-second";

    private sealed class Fixture : IDisposable
    {
        private readonly ServiceProvider _provider;

        private Fixture(ServiceProvider provider, IServiceScope scope, FakeExplorerTelemetryDomain domain)
        {
            _provider = provider;
            Scope = scope;
            Domain = domain;
            Preferences = scope.ServiceProvider.GetRequiredService<IExplorerShellPreferences>();
            Router = scope.ServiceProvider.GetRequiredService<IExplorerShellRouter>();
            Store = new ExplorerPluginAccessStore();
            Store.Set(TelemetryPluginKeys.PluginId, ExplorerPluginAccess.Allowed);
        }

        public IServiceScope Scope { get; }

        public FakeExplorerTelemetryDomain Domain { get; }

        public IExplorerShellPreferences Preferences { get; }

        public IExplorerShellRouter Router { get; }

        public ExplorerPluginAccessStore Store { get; }

        public static Fixture Create()
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddExplorerSession();

            var provider = services.BuildServiceProvider();
            var scope = provider.CreateScope();

            provider.GetRequiredService<IExplorerPreferenceCatalog>()
                .Register(TelemetryPluginKeys.SelectedQueryPreference);

            var domain = new FakeExplorerTelemetryDomain
            {
                Catalog = ExplorerTelemetrySample.Catalog(
                    ExplorerTelemetrySample.Query(queryId: First),
                    ExplorerTelemetrySample.Query(queryId: Second)),
            };

            return new Fixture(provider, scope, domain);
        }

        /// <summary>The area mount: it owns both the address and the remembered panel.</summary>
        public TelemetryWorkspace AreaMount() => new(
            Domain,
            Store,
            new FixedClock(ExplorerTelemetrySample.Now),
            pinnedToOwnTenant: false,
            Preferences,
            Router);

        /// <summary>
        /// The My Tenant section mount, which owns neither, so two mounts cannot
        /// overwrite one another's answer.
        /// </summary>
        public TelemetryWorkspace SectionMount() => new(
            Domain,
            Store,
            new FixedClock(ExplorerTelemetrySample.Now),
            pinnedToOwnTenant: true);

        public void Dispose()
        {
            Scope.Dispose();
            _provider.Dispose();
        }

        private sealed class FixedClock(DateTimeOffset now) : TimeProvider
        {
            public override DateTimeOffset GetUtcNow() => now;
        }
    }

    [Test]
    public async Task Selecting_a_panel_remembers_it_and_puts_it_in_the_address()
    {
        using var fixture = Fixture.Create();
        using var workspace = fixture.AreaMount();
        await workspace.InitializeAsync();

        await workspace.SelectQueryAsync(Second);

        Assert.Multiple(() =>
        {
            Assert.That(workspace.Selected?.QueryId, Is.EqualTo(Second));
            Assert.That(
                fixture.Router.Current.Parameters.GetValueOrEmpty(
                    TelemetryPluginKeys.SelectedQueryParameter),
                Is.EqualTo(Second),
                "the panel a caller is looking at can be linked to, not only described");
            Assert.That(
                fixture.Preferences.GetOrDefault(TelemetryPluginKeys.SelectedQueryPreference, string.Empty),
                Is.EqualTo(Second));
        });
    }

    [Test]
    public async Task A_remembered_panel_is_reopened_on_the_next_visit()
    {
        using var fixture = Fixture.Create();

        using (var first = fixture.AreaMount())
        {
            await first.InitializeAsync();
            await first.SelectQueryAsync(Second);
        }

        using var second = fixture.AreaMount();
        await second.InitializeAsync();

        Assert.That(
            second.Selected?.QueryId,
            Is.EqualTo(Second),
            "a return visit opens the panel the caller left, not the first one in the catalogue");
    }

    [Test]
    public async Task An_address_naming_a_panel_wins_over_the_remembered_one()
    {
        // A link somebody sent must show what they saw, not what the recipient
        // left open. That is the whole division of labour between the two
        // halves of the state contract.
        using var fixture = Fixture.Create();

        using (var first = fixture.AreaMount())
        {
            await first.InitializeAsync();
            await first.SelectQueryAsync(Second);
        }

        fixture.Router.NavigateTo(
            ExplorerRoute.Home.WithParameter(TelemetryPluginKeys.SelectedQueryParameter, First));

        using var linked = fixture.AreaMount();
        await linked.InitializeAsync();

        Assert.That(linked.Selected?.QueryId, Is.EqualTo(First));
    }

    [Test]
    public async Task A_remembered_panel_the_cluster_stopped_offering_falls_back_and_is_forgotten()
    {
        // A remembered value is a hint, never an authority. Forgetting it is the
        // half that matters on the second visit: without it the caller would be
        // silently corrected again on every later restore.
        using var fixture = Fixture.Create();

        using (var first = fixture.AreaMount())
        {
            await first.InitializeAsync();
            await first.SelectQueryAsync(Second);
        }

        fixture.Domain.Catalog = ExplorerTelemetrySample.Catalog(
            ExplorerTelemetrySample.Query(queryId: First));

        using var afterWithdrawal = fixture.AreaMount();
        await afterWithdrawal.InitializeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(afterWithdrawal.Selected?.QueryId, Is.EqualTo(First));
            Assert.That(
                fixture.Preferences.GetOrDefault(TelemetryPluginKeys.SelectedQueryPreference, string.Empty),
                Is.Empty,
                "a value that no longer resolves is forgotten rather than rejected again every visit");
        });
    }

    [Test]
    public async Task An_address_naming_a_panel_the_catalogue_does_not_offer_is_ignored()
    {
        using var fixture = Fixture.Create();
        fixture.Router.NavigateTo(
            ExplorerRoute.Home.WithParameter(TelemetryPluginKeys.SelectedQueryParameter, "typed-by-hand"));

        using var workspace = fixture.AreaMount();
        await workspace.InitializeAsync();

        Assert.That(
            workspace.Selected?.QueryId,
            Is.EqualTo(First),
            "a value that was never rendered cannot change the selection");
    }

    [Test]
    public async Task Re_selecting_the_open_panel_writes_no_navigation()
    {
        using var fixture = Fixture.Create();
        using var workspace = fixture.AreaMount();
        await workspace.InitializeAsync();
        await workspace.SelectQueryAsync(Second);

        var addressed = fixture.Router.Current;
        await workspace.SelectQueryAsync(Second);

        Assert.That(
            fixture.Router.Current,
            Is.SameAs(addressed),
            "selecting the panel already open must not push the address again");
    }

    [Test]
    public async Task The_tenant_section_mount_owns_neither_the_address_nor_the_remembered_panel()
    {
        // Two mounts of one workspace write one key would each keep overwriting
        // the other's answer, and a section of somebody else's surface has no
        // claim on the address at all.
        using var fixture = Fixture.Create();
        using var section = fixture.SectionMount();
        await section.InitializeAsync();

        await section.SelectQueryAsync(Second);

        Assert.Multiple(() =>
        {
            Assert.That(section.Selected?.QueryId, Is.EqualTo(Second), "the section still selects");
            Assert.That(
                fixture.Router.Current.Parameters.GetValueOrEmpty(
                    TelemetryPluginKeys.SelectedQueryParameter),
                Is.Empty);
            Assert.That(
                fixture.Preferences.GetOrDefault(TelemetryPluginKeys.SelectedQueryPreference, string.Empty),
                Is.Empty);
        });
    }

    [Test]
    public async Task A_refresh_keeps_the_panel_the_caller_is_on()
    {
        // The reconcile path runs on every catalogue read, so the restore added
        // for a first mount must not throw a caller back to the remembered panel
        // when they have since moved.
        using var fixture = Fixture.Create();
        using var workspace = fixture.AreaMount();
        await workspace.InitializeAsync();
        await workspace.SelectQueryAsync(Second);

        await workspace.RefreshAsync();

        Assert.That(workspace.Selected?.QueryId, Is.EqualTo(Second));
    }
}
