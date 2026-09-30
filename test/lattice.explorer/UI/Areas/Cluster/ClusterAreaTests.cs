using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.UI;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Session;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

/// <summary>
/// The area contract: identity and directory position, fail-closed visibility on
/// the cluster-wide storage probe (remembered per circuit until the connection
/// changes), the Home status and badge, tree-name completions, the palette
/// commands, and registration.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ClusterAreaTests
{
    private readonly FakeExplorerSession _session = new FakeExplorerSession(new FakeStateConnection()).Configured(SessionTestContext.RemoteConfiguration());
    private readonly ILatticeTreeAdmin _admin = Substitute.For<ILatticeTreeAdmin>();
    private readonly ManualTimeProvider _time = new();

    [Test]
    public void It_is_the_cluster_wide_cluster_stop_at_position_ninety()
    {
        var area = CreateArea();

        Assert.Multiple(() =>
        {
            Assert.That(area.Key, Is.EqualTo("cluster"));
            Assert.That(area.DisplayName, Is.EqualTo("Cluster"));
            Assert.That(area.DirectoryOrder, Is.EqualTo(90));
            Assert.That(area.IsTenantScoped, Is.False);
            Assert.That(area.Completions, Is.InstanceOf<ClusterCompletionSource>());
        });
    }

    [Test]
    public async Task Without_a_tree_administration_facade_it_is_hidden()
    {
        var area = CreateArea(withAdmin: false);

        Assert.That(await area.GetAvailabilityAsync(CancellationToken.None), Is.EqualTo(AreaAvailability.Hidden));
    }

    [Test]
    public async Task Without_a_connection_it_says_why()
    {
        var area = CreateArea(session: new FakeExplorerSession(new FakeStateConnection()));

        var availability = await area.GetAvailabilityAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(availability.Kind, Is.EqualTo(AreaAvailabilityKind.Unavailable));
            Assert.That(availability.Reason, Is.EqualTo("Connect to a cluster to see its estate."));
        });
    }

    [Test]
    public async Task A_denied_probe_hides_it_and_the_verdict_is_remembered()
    {
        _admin.GetStorageUsageAsync(false, Arg.Any<CancellationToken>()).ThrowsAsync(new LatticeAuthorizationDeniedException("denied"));
        var area = CreateArea();

        var first = await area.GetAvailabilityAsync(CancellationToken.None);
        var second = await area.GetAvailabilityAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo(AreaAvailability.Hidden));
            Assert.That(second, Is.EqualTo(AreaAvailability.Hidden));
            Assert.That(_admin.ReceivedCalls().Count(), Is.EqualTo(1));
        });
    }

    [Test]
    public async Task A_facade_the_cluster_does_not_serve_says_so()
    {
        _admin.GetStorageUsageAsync(false, Arg.Any<CancellationToken>()).ThrowsAsync(new NotSupportedException());
        var area = CreateArea();

        var availability = await area.GetAvailabilityAsync(CancellationToken.None);

        Assert.That(availability.Reason, Is.EqualTo("This cluster does not serve tree administration."));
    }

    [Test]
    public async Task A_transient_failure_is_unavailable_and_not_remembered()
    {
        _admin.GetStorageUsageAsync(false, Arg.Any<CancellationToken>())
            .Returns(_ => throw new TimeoutException(), _ => Task.FromResult(new ClusterStorageUsageSummary { TreeCount = 3 }));
        var area = CreateArea();

        var first = await area.GetAvailabilityAsync(CancellationToken.None);
        var second = await area.GetAvailabilityAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first.Kind, Is.EqualTo(AreaAvailabilityKind.Unavailable));
            Assert.That(second, Is.EqualTo(AreaAvailability.Visible));
        });
    }

    [Test]
    public async Task A_granted_probe_shows_it_with_a_home_status_and_a_tree_count_badge()
    {
        _admin.GetStorageUsageAsync(false, Arg.Any<CancellationToken>())
            .Returns(new ClusterStorageUsageSummary { TreeCount = 1204, TotalBytes = 3L * 1024 * 1024 * 1024 });
        UseTrees("a/crm/orders", "orders", "t/acme/orders", "invoices");
        var area = CreateArea();

        var before = await area.GetHomeStatusAsync(CancellationToken.None);
        var beforeBadge = await area.GetDirectoryBadgeAsync(CancellationToken.None);
        var availability = await area.GetAvailabilityAsync(CancellationToken.None);
        var status = await area.GetHomeStatusAsync(CancellationToken.None);
        var badge = await area.GetDirectoryBadgeAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(before, Is.Null, "nothing is known before the probe");
            Assert.That(beforeBadge, Is.Null, "nor counted");
            Assert.That(availability, Is.EqualTo(AreaAvailability.Visible));
            Assert.That(status, Is.EqualTo("4 trees, plus 1,200 system trees, 3.0 GiB stored."), "the count the tree list shows, the rest named");
            Assert.That(badge, Is.EqualTo("4"), "the badge counts what /cluster/trees lists");
        });
    }

    [Test]
    public async Task With_nothing_unlisted_the_status_names_only_the_listed_trees()
    {
        _admin.GetStorageUsageAsync(false, Arg.Any<CancellationToken>())
            .Returns(new ClusterStorageUsageSummary { TreeCount = 2, TotalBytes = 2048 });
        UseTrees("orders", "invoices");
        var area = CreateArea();

        await area.GetAvailabilityAsync(CancellationToken.None);

        Assert.That(await area.GetHomeStatusAsync(CancellationToken.None), Is.EqualTo("2 trees, 2.0 KiB stored."));
    }

    [Test]
    public async Task When_the_tree_list_cannot_be_read_the_status_labels_the_total_and_there_is_no_badge()
    {
        _admin.GetStorageUsageAsync(false, Arg.Any<CancellationToken>())
            .Returns(new ClusterStorageUsageSummary { TreeCount = 22, TotalBytes = 2048 });
        _session.Connection.ListTreesAsync(Arg.Any<CatalogRequest>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new TimeoutException());
        var area = CreateArea();

        await area.GetAvailabilityAsync(CancellationToken.None);

        var status = await area.GetHomeStatusAsync(CancellationToken.None);
        var badge = await area.GetDirectoryBadgeAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(status, Is.EqualTo("22 trees including system trees, 2.0 KiB stored."));
            Assert.That(badge, Is.Null);
        });
    }

    [Test]
    public async Task Under_a_tenant_the_status_says_whose_trees_it_counts()
    {
        _admin.GetStorageUsageAsync(false, Arg.Any<CancellationToken>())
            .Returns(new ClusterStorageUsageSummary
            {
                TreeCount = 23,
                TotalBytes = 1 << 20,
                Trees =
                [
                    new TreeStorageUsageSnapshot { TreeId = "t/globex/orders", TotalBytes = 2048 },
                    new TreeStorageUsageSnapshot { TreeId = "t/acme/orders", TotalBytes = 4096 },
                    new TreeStorageUsageSnapshot { TreeId = "orders", TotalBytes = 8192 },
                ],
            });
        UseTrees("t/globex/factory-floor", "t/globex/orders");
        var tenant = Substitute.For<ILatticeActiveTenantProvider>();
        tenant.AssertedTenant.Returns("globex");
        var facades = Facades(tenant: new ShellAssertedTenant(tenant));
        var area = new ClusterArea(facades, new ClusterTreeCatalog(facades, _time), new ClusterCommandSignals());

        await area.GetAvailabilityAsync(CancellationToken.None);

        var status = await area.GetHomeStatusAsync(CancellationToken.None);
        var badge = await area.GetDirectoryBadgeAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(status, Is.EqualTo("2 trees of tenant globex, 2.0 KiB stored."), "only the tenant's own trees and storage (#4025)");
            Assert.That(badge, Is.EqualTo("2"));
        });
    }

    [Test]
    public void The_catalogue_is_narrowed_only_under_a_tenant_other_than_the_default()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ClusterTreeCatalog.NarrowingTenant(null), Is.Null);
            Assert.That(ClusterTreeCatalog.NarrowingTenant(string.Empty), Is.Null);
            Assert.That(ClusterTreeCatalog.NarrowingTenant("default"), Is.Null);
            Assert.That(ClusterTreeCatalog.NarrowingTenant("globex"), Is.EqualTo("globex"));
        });
    }

    [Test]
    public void A_tree_page_is_one_chain_node_for_the_whole_tree_id()
    {
        var area = CreateArea();

        Assert.Multiple(() =>
        {
            Assert.That(area.GetChainSpans(ExplorerAddress.Parse("/cluster/trees/t/acme/a/crm/orders")), Is.EqualTo(new[] { 1, 5 }));
            Assert.That(area.GetChainSpans(ExplorerAddress.Parse("/cluster/trees/t/acme/orders/resize?x=1")), Is.EqualTo(new[] { 1, 3, 1 }));
            Assert.That(area.GetChainSpans(ExplorerAddress.Parse("/cluster/trees/jobs/tools/overview")), Is.EqualTo(new[] { 1, 2, 1 }));
            Assert.That(area.GetChainSpans(ExplorerAddress.Parse("/cluster/trees/orders?tab=lifecycle")), Is.EqualTo(new[] { 1, 1 }));
            Assert.That(area.GetChainSpans(ExplorerAddress.Parse("/cluster/trees")), Is.Null);
            Assert.That(area.GetChainSpans(ExplorerAddress.Parse("/cluster/wal?tree=a%2Fb")), Is.Null);
            Assert.That(area.GetChainSpans(ExplorerAddress.Parse("/cluster")), Is.Null);
        });
    }

    [Test]
    public async Task A_new_connection_forgets_the_verdict()
    {
        _admin.GetStorageUsageAsync(false, Arg.Any<CancellationToken>())
            .Returns(_ => throw new LatticeAuthorizationDeniedException("denied"), _ => Task.FromResult(new ClusterStorageUsageSummary()));
        using var area = CreateArea();

        var before = await area.GetAvailabilityAsync(CancellationToken.None);
        await _session.ApplyAsync(SessionTestContext.RemoteConfiguration());
        var after = await area.GetAvailabilityAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(before, Is.EqualTo(AreaAvailability.Hidden));
            Assert.That(after, Is.EqualTo(AreaAvailability.Visible));
            Assert.That(_session.ConfigurationSubscribers, Is.EqualTo(1));
        });

        area.Dispose();
        Assert.That(_session.ConfigurationSubscribers, Is.Zero);
    }

    [Test]
    public void Cancellation_by_the_directory_propagates()
    {
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        _admin.GetStorageUsageAsync(false, Arg.Any<CancellationToken>()).ThrowsAsync(new OperationCanceledException(cancelled.Token));
        var area = CreateArea();

        Assert.That(async () => await area.GetAvailabilityAsync(cancelled.Token), Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task Its_palette_commands_target_their_pages_and_reach_them_through_the_relay()
    {
        var signals = new ClusterCommandSignals();
        var area = CreateArea(signals: signals);
        var heard = new List<string>();

        var reshard = area.Commands.Single(command => command.Id == ClusterArea.ReshardCommandId);
        var plan = area.Commands.Single(command => command.Id == ClusterArea.PlanWalMoveCommandId);
        await reshard.InvokeAsync!(CancellationToken.None);
        var held = signals.TryTake(ClusterArea.ReshardCommandId);
        signals.Requested += heard.Add;
        await plan.InvokeAsync!(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(reshard.Title, Is.EqualTo("Reshard tree..."));
            Assert.That(reshard.Detail, Is.EqualTo("Grow or shrink a tree's physical shard count, online."), "a reshard is no longer grow-only (#4076)");
            Assert.That(plan.Title, Is.EqualTo("Plan WAL move..."));
            Assert.That(reshard.Target, Is.EqualTo(ClusterAddresses.Trees));
            Assert.That(plan.Target, Is.EqualTo(ClusterAddresses.Wal()));
            Assert.That(held, Is.True, "an unheard request is held for the page");
            Assert.That(signals.TryTake(ClusterArea.ReshardCommandId), Is.False, "and taken once");
            Assert.That(heard, Is.EqualTo(new[] { ClusterArea.PlanWalMoveCommandId }));
            Assert.That(signals.TryTake(ClusterArea.PlanWalMoveCommandId), Is.False, "a heard request is not also held");
        });
    }

    [Test]
    public async Task Completions_match_logical_tree_names_best_first_and_link_their_pages()
    {
        UseTrees("a/crm/orders", "orders", "t/acme/orders-archive", "invoices");
        var area = CreateArea();

        var search = await area.Completions!.CompleteAsync(new AddressQuery("ord", AddressQueryMode.Search, ExplorerAddress.Home), CancellationToken.None);
        var address = await area.Completions.CompleteAsync(new AddressQuery("/cluster/trees/a/", AddressQueryMode.Address, ExplorerAddress.Home), CancellationToken.None);
        var app = await area.Completions.CompleteAsync(new AddressQuery("crm", AddressQueryMode.App, ExplorerAddress.Home), CancellationToken.None);
        var blank = await area.Completions.CompleteAsync(new AddressQuery(" ", AddressQueryMode.Search, ExplorerAddress.Home), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(search.Select(hit => hit.Label), Is.EqualTo(new[] { "orders", "a/crm/orders", "t/acme/orders-archive" }));
            Assert.That(search[1].Target, Is.EqualTo(ClusterAddresses.Tree("a/crm/orders")));
            Assert.That(search[1].Detail, Is.EqualTo("Cluster tree, app crm"));
            Assert.That(search[0].Detail, Is.EqualTo("Cluster tree"));
            Assert.That(address.Select(hit => hit.Label), Is.EqualTo(new[] { "a/crm/orders" }));
            Assert.That(app, Is.Empty);
            Assert.That(blank, Is.Empty);
        });
    }

    [Test]
    public async Task Completions_stop_at_the_query_limit_and_reuse_the_catalogue()
    {
        UseTrees([.. Enumerable.Range(0, 30).Select(index => $"tree-{index:00}")]);
        var area = CreateArea();

        var first = await area.Completions!.CompleteAsync(new AddressQuery("tree", AddressQueryMode.Search, ExplorerAddress.Home), CancellationToken.None);
        await area.Completions.CompleteAsync(new AddressQuery("tree-1", AddressQueryMode.Search, ExplorerAddress.Home), CancellationToken.None);
        _time.Advance(ClusterTreeCatalog.Freshness);
        await area.Completions.CompleteAsync(new AddressQuery("tree-2", AddressQueryMode.Search, ExplorerAddress.Home), CancellationToken.None);

        Assert.That(first, Has.Count.EqualTo(AddressQuery.MaximumResults));
        await _session.Connection.Received(2).ListTreesAsync(Arg.Any<CatalogRequest>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task The_catalogue_follows_every_page_and_needs_a_connection()
    {
        _session.Connection.ListTreesAsync(Arg.Is<CatalogRequest>(request => request.PageToken == null), Arg.Any<CancellationToken>())
            .Returns(new TreeCatalogPage { Entries = [ClusterTestContext.Tree("b")], NextPageToken = "next" });
        _session.Connection.ListTreesAsync(Arg.Is<CatalogRequest>(request => request.PageToken == "next"), Arg.Any<CancellationToken>())
            .Returns(new TreeCatalogPage { Entries = [ClusterTestContext.Tree("a")] });
        var catalog = new ClusterTreeCatalog(Facades(), _time);
        var disconnected = new ClusterTreeCatalog(Facades(session: new FakeExplorerSession(new FakeStateConnection())), _time);

        var trees = await catalog.GetAsync(refresh: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(trees.Select(tree => tree.TreeId), Is.EqualTo(new[] { "a", "b" }));
            Assert.That(catalog.Truncated, Is.False);
            Assert.That(async () => await disconnected.GetAsync(false, CancellationToken.None), Throws.InvalidOperationException);
        });
    }

    [Test]
    public void The_shell_registers_the_area_scoped_and_idempotently()
    {
        var services = new ServiceCollection().AddLatticeExplorerShell();
        services.AddLatticeExplorerShell();

        Assert.Multiple(() =>
        {
            Assert.That(services.Count(descriptor => descriptor.ServiceType == typeof(IExplorerArea) && descriptor.ImplementationType == typeof(ClusterArea)), Is.EqualTo(1));
            Assert.That(services.Single(descriptor => descriptor.ImplementationType == typeof(ClusterArea)).Lifetime, Is.EqualTo(ServiceLifetime.Scoped));
            Assert.That(services.Single(descriptor => descriptor.ServiceType == typeof(ClusterFacades)).Lifetime, Is.EqualTo(ServiceLifetime.Scoped));
            Assert.That(services.Single(descriptor => descriptor.ServiceType == typeof(ClusterTreeCatalog)).Lifetime, Is.EqualTo(ServiceLifetime.Scoped));
            Assert.That(services.Single(descriptor => descriptor.ServiceType == typeof(ClusterCommandSignals)).Lifetime, Is.EqualTo(ServiceLifetime.Scoped));
        });
    }

    [Test]
    public void The_facades_resolve_optionally_and_name_a_missing_one()
    {
        var facades = Facades(withAdmin: false);

        Assert.Multiple(() =>
        {
            Assert.That(facades.TreeAdmin, Is.Null);
            Assert.That(facades.ReplicationStatus, Is.Null);
            Assert.That(facades.IsConnected, Is.True);
            Assert.That(() => facades.RequireTreeAdmin(), Throws.InstanceOf<NotSupportedException>());
        });
    }

    [Test]
    public async Task At_the_default_tenant_the_status_and_badge_count_only_its_own_trees()
    {
        // #4025: the cluster hands the reserved default tenant every tenant's trees.
        _admin.GetStorageUsageAsync(false, Arg.Any<CancellationToken>())
            .Returns(new ClusterStorageUsageSummary
            {
                TreeCount = 9,
                TotalBytes = 1 << 20,
                Trees =
                [
                    new TreeStorageUsageSnapshot { TreeId = "orders", TotalBytes = 2048 },
                    new TreeStorageUsageSnapshot { TreeId = "t/acme/orders", TotalBytes = 4096 },
                    new TreeStorageUsageSnapshot { TreeId = "sys-tenant-registry", TotalBytes = 8192 },
                ],
            });
        UseTrees("orders", "invoices", "t/acme/orders", "t/globex/orders");
        var tenant = Substitute.For<ILatticeActiveTenantProvider>();
        tenant.AssertedTenant.Returns((string?)null);
        var facades = Facades(tenant: new ShellAssertedTenant(tenant));
        var area = new ClusterArea(facades, new ClusterTreeCatalog(facades, _time), new ClusterCommandSignals());

        await area.GetAvailabilityAsync(CancellationToken.None);

        var status = await area.GetHomeStatusAsync(CancellationToken.None);
        var badge = await area.GetDirectoryBadgeAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(status, Is.EqualTo("2 trees of tenant default, 2.0 KiB stored."));
            Assert.That(badge, Is.EqualTo("2"));
        });
    }

    [Test]
    public void It_follows_the_tenant_only_at_a_tenant_rooted_address()
    {
        var area = CreateArea();

        Assert.Multiple(() =>
        {
            Assert.That(area.IsTenantScopedAt(ClusterAddresses.Trees.WithTenant("acme")), Is.True);
            Assert.That(area.IsTenantScopedAt(ClusterAddresses.Trees), Is.False);
        });
    }
    private void UseTrees(params string[] ids) =>
        _session.Connection.ListTreesAsync(Arg.Any<CatalogRequest>(), Arg.Any<CancellationToken>())
            .Returns(new TreeCatalogPage { Entries = [.. ids.Select(id => ClusterTestContext.Tree(id))] });

    private ClusterFacades Facades(bool withAdmin = true, FakeExplorerSession? session = null, ShellAssertedTenant? tenant = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton<IExplorerSession>(session ?? _session);
        if (tenant is not null)
        {
            services.AddSingleton(tenant);
        }
        if (withAdmin)
        {
            services.AddKeyedSingleton(ShellFacades.Key, _admin);
        }

        return new ClusterFacades(services.BuildServiceProvider());
    }

    private ClusterArea CreateArea(bool withAdmin = true, FakeExplorerSession? session = null, ClusterCommandSignals? signals = null)
    {
        var facades = Facades(withAdmin, session);
        return new ClusterArea(facades, new ClusterTreeCatalog(facades, _time), signals ?? new ClusterCommandSignals());
    }
}
