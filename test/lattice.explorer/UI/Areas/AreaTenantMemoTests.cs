using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Api.Telemetry;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Tests.Connection;
using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Explorer.UI.Areas.Backups;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.UI.Areas.Replication;
using Orleans.Lattice.Explorer.UI.Areas.Telemetry;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas;

/// <summary>
/// Everything the Backups, Replication, Telemetry, Cluster and Access areas
/// remember from the cluster - each area's visibility verdict and each
/// circuit-scoped cache - is keyed on the tenant the circuit asserts: a tenant
/// switch asks the cluster again and never serves an answer read under the
/// tenant it left. Each fake cluster below answers per asserted tenant, as the
/// real one does now that every call carries it.
/// </summary>
[TestFixture]
public sealed class AreaTenantMemoTests
{
    private FakeActiveTenantProvider _provider = null!;
    private ShellAssertedTenant _tenant = null!;

    [SetUp]
    public void SetUp()
    {
        _provider = new FakeActiveTenantProvider("acme");
        _tenant = new ShellAssertedTenant(_provider);
    }

    [Test]
    public async Task The_backups_visibility_memo_re_probes_after_a_tenant_switch()
    {
        var control = Substitute.For<ILatticeBackupControl>();
        control.ProbeCapabilitiesAsync(Arg.Any<BackupScopeSelector>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(new BackupScopeCapabilities { Scope = call.Arg<BackupScopeSelector>(), CanList = _provider.AssertedTenant == "acme" }));
        var access = new BackupsAccess(control, _tenant);

        var acme = await access.GetAvailabilityAsync(CancellationToken.None);
        var cached = await access.GetAvailabilityAsync(CancellationToken.None);
        _provider.Set("globex");
        var globex = await access.GetAvailabilityAsync(CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(acme.Kind, Is.EqualTo(AreaAvailabilityKind.Visible));
            Assert.That(cached.Kind, Is.EqualTo(AreaAvailabilityKind.Visible));
            Assert.That(globex.Kind, Is.EqualTo(AreaAvailabilityKind.Unavailable), "globex holds no backup grant");
            await control.Received(2).ProbeCapabilitiesAsync(Arg.Any<BackupScopeSelector>(), Arg.Any<CancellationToken>());
        });
    }

    [Test]
    public async Task The_backups_health_monitoring_and_extension_answers_are_forgotten_on_a_tenant_switch()
    {
        var control = Substitute.For<ILatticeBackupControl>();
        control.IsHealthMonitoringAvailableAsync(Arg.Any<CancellationToken>()).Returns(true, false);
        var access = new BackupsAccess(control, _tenant);

        var acme = await access.IsHealthMonitoringAvailableAsync(CancellationToken.None);
        access.MarkExtensionsNotServed();
        _provider.Set("globex");

        Assert.Multiple(async () =>
        {
            Assert.That(acme, Is.True);
            Assert.That(access.ExtensionsServed, Is.Null, "the extension answer belonged to acme");
            Assert.That(await access.IsHealthMonitoringAvailableAsync(CancellationToken.None), Is.False);
        });
    }

    [Test]
    public async Task A_backed_up_trees_app_is_described_per_tenant()
    {
        var control = Substitute.For<ILatticeAppsControl>();
        control.DescribeAsync("crm", Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult<AppDescriptor?>(new AppDescriptor
            {
                Slug = "crm",
                Version = "1.0.0",
                Provenance = new AppProvenanceDescriptor { Source = "in-image", Publisher = "p" },
                Presentation = new AppPresentationDescriptor { DisplayName = _provider.AssertedTenant + " CRM" },
            }));
        using var services = new ServiceCollection().AddKeyedSingleton(ShellFacades.Key, control).BuildServiceProvider();
        var trees = new BackupAppTrees(services, _tenant);
        var name = BackupTreeName.Parse("a/crm/orders");

        var acme = await trees.FindAsync(name, CancellationToken.None);
        var cached = await trees.FindAsync(name, CancellationToken.None);
        _provider.Set("globex");
        var globex = await trees.FindAsync(name, CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(cached, Is.SameAs(acme));
            Assert.That(acme!.DisplayName, Is.EqualTo("acme CRM"));
            Assert.That(globex!.DisplayName, Is.EqualTo("globex CRM"), "acme's app is not described under globex");
            await control.Received(2).DescribeAsync("crm", Arg.Any<string?>(), Arg.Any<CancellationToken>());
        });
    }

    [Test]
    public async Task A_backup_operation_runs_pinned_to_its_tenant_and_is_listed_only_under_it()
    {
        var operations = new BackupOperations(TimeProvider.System, _tenant);
        var switched = new TaskCompletionSource();
        var release = new TaskCompletionSource();
        string? assertedAfterSwitch = "unset";

        var operation = operations.Start(BackupOperationKind.FullCapture, "Capture", ["Capture"], async (_, _) =>
        {
            switched.SetResult();
            await release.Task;
            assertedAfterSwitch = _tenant.AssertedTenant;
        });
        await switched.Task;
        _provider.Set("globex");
        var outsideTheOperation = _tenant.AssertedTenant;
        release.SetResult();
        await operation.Completion;

        Assert.Multiple(() =>
        {
            Assert.That(assertedAfterSwitch, Is.EqualTo("acme"), "the operation's later calls still assert the tenant it began in");
            Assert.That(outsideTheOperation, Is.EqualTo("globex"), "the pin never leaks out of the operation");
            Assert.That(operations.Recent, Is.Empty, "acme's operation is not listed under globex");
            Assert.That(operations.Find(operation.Id), Is.Null);
            Assert.That(operations.Latest(BackupOperationKind.FullCapture), Is.Null);
        });

        _provider.Set("acme");
        Assert.Multiple(() =>
        {
            Assert.That(operations.Recent, Is.EqualTo(new[] { operation }));
            Assert.That(operations.Find(operation.Id), Is.SameAs(operation));
            Assert.That(operations.Latest(BackupOperationKind.FullCapture), Is.SameAs(operation));
        });
    }

    [Test]
    public async Task The_replication_reads_are_read_again_under_a_new_tenant()
    {
        var acmeReport = new ReplicationConfigReport([]);
        var globexReport = new ReplicationConfigReport([]);
        var control = Substitute.For<ILatticeReplicationControl>();
        control.GetReplicationConfigAsync(Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(_provider.AssertedTenant == "acme" ? acmeReport : globexReport));
        using var services = new ServiceCollection()
            .AddKeyedSingleton(ShellFacades.Key, control)
            .AddSingleton(_tenant)
            .BuildServiceProvider();
        using var source = new ReplicationDataSource(services, TimeProvider.System, new ReplicationOptions());

        var acme = await source.GetConfigAsync(refresh: false, CancellationToken.None);
        _provider.Set("globex");
        var globex = await source.GetConfigAsync(refresh: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(acme.Value, Is.SameAs(acmeReport));
            Assert.That(globex.Value, Is.SameAs(globexReport), "acme's cached enrolment is not served under globex");
        });
    }

    [Test]
    public async Task The_telemetry_catalogue_is_read_again_under_a_new_tenant()
    {
        var telemetry = Substitute.For<ILatticeTelemetry>();
        telemetry.GetCatalogAsync(Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(new TelemetryQueryCatalog { Version = _provider.AssertedTenant == "acme" ? 1 : 2, Queries = [] }));
        using var cache = new TelemetryCatalogCache(telemetry, tenant: _tenant);

        var acme = await cache.GetAsync();
        _provider.Set("globex");
        var stale = cache.Current;
        var globex = await cache.GetAsync();

        Assert.Multiple(() =>
        {
            Assert.That(acme.Version, Is.EqualTo(1));
            Assert.That(stale, Is.Null, "acme's catalogue is not the current one under globex");
            Assert.That(globex.Version, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task The_cluster_visibility_memo_and_usage_are_read_again_under_a_new_tenant()
    {
        var admin = Substitute.For<ILatticeTreeAdmin>();
        admin.GetStorageUsageAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(new ClusterStorageUsageSummary { TreeCount = _provider.AssertedTenant == "acme" ? 3 : 7 }));
        var session = Substitute.For<IExplorerSession>();
        session.IsConfigured.Returns(true);
        using var services = new ServiceCollection()
            .AddKeyedSingleton(ShellFacades.Key, admin)
            .AddSingleton(session)
            .AddSingleton(_tenant)
            .BuildServiceProvider();
        var facades = new ClusterFacades(services);
        using var area = new ClusterArea(facades, new ClusterTreeCatalog(facades, TimeProvider.System), new ClusterCommandSignals());

        await area.GetAvailabilityAsync(CancellationToken.None);
        var acmeBadge = await area.GetDirectoryBadgeAsync(CancellationToken.None);
        _provider.Set("globex");
        var staleBadge = await area.GetDirectoryBadgeAsync(CancellationToken.None);
        await area.GetAvailabilityAsync(CancellationToken.None);
        var globexBadge = await area.GetDirectoryBadgeAsync(CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(acmeBadge, Is.EqualTo("3"));
            Assert.That(staleBadge, Is.Null, "acme's usage is not shown under globex");
            Assert.That(globexBadge, Is.EqualTo("7"));
            await admin.Received(2).GetStorageUsageAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>());
        });
    }

    [Test]
    public async Task The_access_verdict_and_catalogue_are_read_again_under_a_new_tenant()
    {
        var admin = Substitute.For<ILatticeAuthAdmin>();
        admin.ListGroupsAsync(Arg.Any<AuthPageRequest>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(new AuthGroupPage { Entries = [new AuthGroup { GroupId = _provider.AssertedTenant + "-group" }] }));
        using var services = new ServiceCollection()
            .AddKeyedSingleton(ShellFacades.Key, admin)
            .AddSingleton(_tenant)
            .BuildServiceProvider();
        var area = new AccessArea(services);
        var catalog = new AccessCatalog(admin, _tenant);

        await area.GetAvailabilityAsync(CancellationToken.None);
        await area.GetAvailabilityAsync(CancellationToken.None);
        var acmeGroups = await catalog.GetGroupsAsync(CancellationToken.None);
        _provider.Set("globex");
        await area.GetAvailabilityAsync(CancellationToken.None);
        var globexGroups = await catalog.GetGroupsAsync(CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(acmeGroups.Select(group => group.GroupId), Is.EqualTo(new[] { "acme-group" }));
            Assert.That(globexGroups.Select(group => group.GroupId), Is.EqualTo(new[] { "globex-group" }));
            await admin.Received(4).ListGroupsAsync(Arg.Any<AuthPageRequest>(), Arg.Any<CancellationToken>());
        });
    }
}
