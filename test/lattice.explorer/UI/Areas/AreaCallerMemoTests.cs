using System.Collections.Immutable;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.Telemetry;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Session;
using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;
using Orleans.Lattice.Explorer.UI.Areas.Backups;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.UI.Areas.Data;
using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Explorer.UI.Areas.Telemetry;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Suggestions;
using Orleans.Lattice.Explorer.UI.Transport;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas;

/// <summary>
/// Issue #4019: everything an area remembers from the cluster is filed under the
/// caller - the sign-in, the endpoint and the asserted tenant - so when the
/// identity changes inside one circuit, the next caller is never served what was
/// read for the previous one. In each test alice reads, the circuit signs in as
/// bob (same tenant, same endpoint), and bob must see his own answer. Each fake
/// cluster answers per signed-in user, as the real one does.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class AreaCallerMemoTests
{
    private readonly FakeAuthSession _auth = new();
    private readonly FakeExplorerSession _session = new FakeExplorerSession(new FakeStateConnection())
        .Configured(new ExplorerConfiguration { Endpoint = "https://cluster.example:5001" });

    private string User => _auth.Username ?? "anonymous";

    [SetUp]
    public void SetUp() => _auth.SignIn("alice");

    [Test]
    public async Task The_apps_snapshot_is_probed_again_for_the_next_caller()
    {
        var workspace = Substitute.For<ILatticeAppWorkspace>();
        workspace.ListMyAppsAsync(Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(ImmutableArray.Create(new WorkspaceAppSummary { Slug = User + "-app", Version = "1.0.0" })));
        using var services = Circuit(collection => collection.AddKeyedSingleton(ShellFacades.Key, workspace));
        var access = new AppsAccess(new AppsFacades(services));

        var alice = await access.GetAsync();
        _auth.SignIn("bob");
        var stale = access.Current;
        var bob = await access.GetAsync();

        Assert.Multiple(() =>
        {
            Assert.That(alice.MyApps.Select(app => app.Slug), Is.EqualTo(new[] { "alice-app" }));
            Assert.That(stale, Is.Null, "alice's snapshot is not bob's");
            Assert.That(bob.MyApps.Select(app => app.Slug), Is.EqualTo(new[] { "bob-app" }));
        });
    }

    [Test]
    public void The_staged_install_flows_are_forgotten_for_the_next_caller()
    {
        using var services = Circuit();
        var store = new AppInstallFlowStore(new AppsFacades(services));
        var key = new AppInstallFlowKey(null, "in-image", "crm", "1.0.0");

        var alice = store.GetOrCreate(key, source: null);
        _auth.SignIn("bob");

        Assert.Multiple(() =>
        {
            Assert.That(store.Flows, Is.Empty, "alice's staged install is not resumed for bob");
            Assert.That(store.GetOrCreate(key, source: null), Is.Not.SameAs(alice));
        });
    }

    [Test]
    public async Task The_cluster_verdict_and_usage_are_read_again_for_the_next_caller()
    {
        var admin = Substitute.For<ILatticeTreeAdmin>();
        admin.GetStorageUsageAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(_ => User == "alice"
                ? Task.FromResult(new ClusterStorageUsageSummary { TreeCount = 3 })
                : Task.FromException<ClusterStorageUsageSummary>(new LatticeAuthorizationDeniedException("bob holds no telemetry grant")));
        using var services = Circuit(collection => collection.AddKeyedSingleton(ShellFacades.Key, admin));
        var facades = new ClusterFacades(services);
        using var area = new ClusterArea(facades, new ClusterTreeCatalog(facades, TimeProvider.System), new ClusterCommandSignals());

        var alice = await area.GetAvailabilityAsync(CancellationToken.None);
        _auth.SignIn("bob");
        var bob = await area.GetAvailabilityAsync(CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(alice.Kind, Is.EqualTo(AreaAvailabilityKind.Visible));
            Assert.That(bob.Kind, Is.EqualTo(AreaAvailabilityKind.Hidden), "alice's verdict does not show bob the area");
            Assert.That(await area.GetHomeStatusAsync(CancellationToken.None), Is.Null, "alice's storage usage is not bob's");
        });
    }

    [Test]
    public async Task The_cluster_tree_list_is_read_again_for_the_next_caller_within_its_freshness_window()
    {
        ListTreesPerUser();
        using var services = Circuit();
        var catalog = new ClusterTreeCatalog(new ClusterFacades(services), new ManualTimeProvider());

        var alice = await catalog.GetAsync(refresh: false, CancellationToken.None);
        _auth.SignIn("bob");
        var bob = await catalog.GetAsync(refresh: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(alice.Select(tree => tree.TreeId), Is.EqualTo(new[] { "alice-tree" }));
            Assert.That(bob.Select(tree => tree.TreeId), Is.EqualTo(new[] { "bob-tree" }), "alice's list is not served to bob inside 30 s");
        });
    }

    [Test]
    public async Task The_schema_tree_list_is_read_again_for_the_next_caller_within_its_freshness_window()
    {
        ListTreesPerUser();
        using var services = Circuit();
        var catalog = new SchemaTreeCatalog(new SchemaFacades(services), new ManualTimeProvider());

        var alice = await catalog.GetAsync(refresh: false, CancellationToken.None);
        _auth.SignIn("bob");
        var bob = await catalog.GetAsync(refresh: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(alice, Is.EqualTo(new[] { "alice-tree" }));
            Assert.That(bob, Is.EqualTo(new[] { "bob-tree" }), "alice's list is not served to bob inside 30 s");
        });
    }

    [Test]
    public async Task A_cached_suggestion_list_is_read_again_for_the_next_caller_within_its_freshness_window()
    {
        var status = Substitute.For<ILatticeReplicationStatus>();
        status.GetPeerStatusAsync(Arg.Any<ReplicationPeerStatusQuery>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(new ReplicationPeerStatusPage(User + "-region", [], null)));
        using var caller = new ShellCaller(_auth, _session);
        var source = new RegionSuggestionSource(status, caller, new ManualTimeProvider());

        var alice = await source.SuggestAsync(string.Empty, 5, CancellationToken.None);
        _auth.SignIn("bob");
        var bob = await source.SuggestAsync(string.Empty, 5, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(alice.Items.Select(item => item.Value), Is.EqualTo(new[] { "alice-region" }));
            Assert.That(bob.Items.Select(item => item.Value), Is.EqualTo(new[] { "bob-region" }), "alice's list is not offered to bob inside 30 s");
        });
    }

    [Test]
    public async Task The_access_verdict_and_catalogue_are_read_again_for_the_next_caller()
    {
        var admin = Substitute.For<ILatticeAuthAdmin>();
        admin.ListGroupsAsync(Arg.Any<AuthPageRequest>(), Arg.Any<CancellationToken>())
            .Returns(_ => User == "alice"
                ? Task.FromResult(new AuthGroupPage { Entries = [new AuthGroup { GroupId = "alice-group" }] })
                : Task.FromException<AuthGroupPage>(new LatticeAuthorizationDeniedException("bob is not an administrator")));
        using var services = Circuit(collection => collection
            .AddKeyedSingleton(ShellFacades.Key, admin)
            .AddScoped(provider => new AccessCatalog(admin, caller: ShellCaller.Of(provider))));
        using var scope = services.CreateScope();
        var area = new AccessArea(scope.ServiceProvider);
        var catalog = scope.ServiceProvider.GetRequiredService<AccessCatalog>();

        var alice = await area.GetAvailabilityAsync(CancellationToken.None);
        var aliceGroups = await catalog.GetGroupsAsync(CancellationToken.None);
        _auth.SignIn("bob");
        var bob = await area.GetAvailabilityAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(alice.Kind, Is.EqualTo(AreaAvailabilityKind.Visible));
            Assert.That(aliceGroups.Select(group => group.GroupId), Is.EqualTo(new[] { "alice-group" }));
            Assert.That(bob.Kind, Is.EqualTo(AreaAvailabilityKind.Hidden), "alice's verdict does not show bob the area");
            Assert.That(
                async () => await catalog.GetGroupsAsync(CancellationToken.None),
                Throws.InstanceOf<LatticeAuthorizationDeniedException>(),
                "alice's groups are not served to bob; his own read is refused");
        });
    }

    [Test]
    public async Task The_backups_verdict_is_probed_again_for_the_next_caller()
    {
        var control = Substitute.For<ILatticeBackupControl>();
        control.ProbeCapabilitiesAsync(Arg.Any<BackupScopeSelector>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(new BackupScopeCapabilities { Scope = call.Arg<BackupScopeSelector>(), CanList = User == "alice" }));
        using var caller = new ShellCaller(_auth, _session);
        var access = new BackupsAccess(control, caller: caller);

        var alice = await access.GetAvailabilityAsync(CancellationToken.None);
        _auth.SignIn("bob");
        var bob = await access.GetAvailabilityAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(alice.Kind, Is.EqualTo(AreaAvailabilityKind.Visible));
            Assert.That(bob.Kind, Is.EqualTo(AreaAvailabilityKind.Unavailable), "bob holds no backup grant");
        });
    }

    [Test]
    public async Task A_trees_administration_grant_is_probed_again_for_the_next_caller()
    {
        var admin = Substitute.For<ILatticeTreeAdmin>();
        admin.ProbeCapabilitiesAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(new LatticeTreeAdminCapabilities { TreeId = "orders", Schema = new Orleans.Lattice.Api.Schema.LatticeSchemaCapabilities { TreeId = "orders" }, CanAdministerTree = User == "alice" }));
        using var services = Circuit(collection => collection.AddKeyedSingleton(ShellFacades.Key, admin));
        var gate = new DataAdminGate(services);

        var alice = await gate.CanAdministerAsync("orders");
        _auth.SignIn("bob");
        var bob = await gate.CanAdministerAsync("orders");

        Assert.Multiple(() =>
        {
            Assert.That(alice, Is.True);
            Assert.That(bob, Is.False, "alice's grant on the tree is not drawn for bob");
        });
    }

    [Test]
    public async Task The_telemetry_catalogue_is_read_again_for_the_next_caller()
    {
        var telemetry = Substitute.For<ILatticeTelemetry>();
        telemetry.GetCatalogAsync(Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(new TelemetryQueryCatalog { Version = User == "alice" ? 1 : 2, Queries = [] }));
        using var cache = new TelemetryCatalogCache(telemetry, _auth, _session);

        var alice = await cache.GetAsync();
        _auth.SignIn("bob");
        var bob = await cache.GetAsync();

        Assert.Multiple(() =>
        {
            Assert.That(alice.Version, Is.EqualTo(1));
            Assert.That(bob.Version, Is.EqualTo(2));
        });
    }

    [Test]
    public void A_compliance_scan_is_not_shown_to_the_next_caller()
    {
        using var caller = new ShellCaller(_auth, _session);
        var ledger = new SchemaComplianceLedger(caller);

        ledger.Record("orders", new LatticeSchemaComplianceReport { TreeId = "orders", HasPolicy = true, ScannedCount = 1, CompliantCount = 1, NonCompliantCount = 0, RuleBreakdown = [] }, DateTimeOffset.UnixEpoch);
        var alice = ledger.Find("orders");
        _auth.SignIn("bob");

        Assert.Multiple(() =>
        {
            Assert.That(alice, Is.Not.Null);
            Assert.That(ledger.Find("orders"), Is.Null, "alice's scan result is not bob's");
        });
    }

    [Test]
    public async Task A_memo_is_read_again_after_the_connection_moves_to_another_endpoint()
    {
        var admin = Substitute.For<ILatticeTreeAdmin>();
        admin.ProbeCapabilitiesAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(new LatticeTreeAdminCapabilities { TreeId = "orders", Schema = new Orleans.Lattice.Api.Schema.LatticeSchemaCapabilities { TreeId = "orders" }, CanAdministerTree = _session.Current!.Endpoint.Contains("first", StringComparison.Ordinal) }));
        await _session.ApplyAsync(new ExplorerConfiguration { Endpoint = "https://first.example:5001" });
        using var services = Circuit(collection => collection.AddKeyedSingleton(ShellFacades.Key, admin));
        var gate = new DataAdminGate(services);

        var first = await gate.CanAdministerAsync("orders");
        await _session.ApplyAsync(new ExplorerConfiguration { Endpoint = "https://second.example:5001" });
        var second = await gate.CanAdministerAsync("orders");

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.True);
            Assert.That(second, Is.False, "the first cluster's grant is not drawn against the second");
        });
    }

    private ServiceProvider Circuit(Action<IServiceCollection>? configure = null)
    {
        var services = new ServiceCollection()
            .AddSingleton<IExplorerAuthSession>(_auth)
            .AddSingleton<IExplorerSession>(_session);
        configure?.Invoke(services);
        return services.BuildServiceProvider();
    }

    private void ListTreesPerUser() =>
        _session.Connection.ListTreesAsync(Arg.Any<CatalogRequest>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(new TreeCatalogPage
            {
                Entries = [new TreeCatalogEntry { TreeId = User + "-tree", Config = new TreeConfigSummary() }],
            }));
}
