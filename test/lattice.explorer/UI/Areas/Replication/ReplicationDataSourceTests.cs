using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Explorer.UI.Areas.Replication;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Session;
using static Orleans.Lattice.Explorer.Tests.UI.Areas.Replication.ReplicationTestData;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Replication;

/// <summary>
/// The data source: paging the status report to its end, bounding a report that never
/// ends, caching briefly, forgetting on a sign-in or connection change, classifying
/// faults, and failing closed when a facade is not registered.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ReplicationDataSourceTests
{
    private readonly ManualTimeProvider _time = new();
    private readonly FakeReplicationStatus _status = new();
    private readonly FakeReplicationControl _control = new();
    private readonly FakeAuthSession _auth = new();
    private readonly FakeExplorerSession _session = new(new FakeStateConnection());

    [Test]
    public async Task It_pages_the_status_report_to_its_end()
    {
        _status.Links.AddRange(Estate());
        _status.ServedPageSize = 2;
        using var data = Create();

        var read = await data.GetEstateAsync(refresh: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(read.Succeeded, Is.True);
            Assert.That(read.Value!.LocalRegionId, Is.EqualTo("eu-west"));
            Assert.That(read.Value.Links, Has.Count.EqualTo(6));
            Assert.That(read.Value.Truncated, Is.False);
            Assert.That(read.Value.ReadAt, Is.EqualTo(_time.GetUtcNow()));
            Assert.That(_status.Calls, Is.EqualTo(3));
            Assert.That(_status.Queries.Select(query => query.PageSize).Distinct(), Is.EqualTo(new[] { 1000 }));
            Assert.That(_status.Queries[1].ContinuationToken, Is.EqualTo("offset:2"));
        });
    }

    [Test]
    public async Task A_report_longer_than_the_page_ceiling_is_marked_truncated()
    {
        _status.Links.AddRange(Estate());
        _status.ServedPageSize = 1;
        using var data = Create(new ReplicationOptions { MaxPages = 2 });

        var read = await data.GetEstateAsync(refresh: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(read.Value!.Links, Has.Count.EqualTo(2));
            Assert.That(read.Value.Truncated, Is.True);
            Assert.That(_status.Calls, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task A_continuation_token_that_repeats_stops_the_read()
    {
        _status.Links.Add(Link("orders", "us-east"));
        _status.StuckToken = "again";
        using var data = Create();

        var read = await data.GetEstateAsync(refresh: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(_status.Calls, Is.EqualTo(2));
            Assert.That(read.Value!.Truncated, Is.True);
            Assert.That(read.Value.Links, Has.Count.EqualTo(1), "a page served twice adds no second copy of its links");
        });
    }

    [Test]
    public async Task A_read_is_reused_for_the_cache_lifetime_unless_refreshed()
    {
        _control.Trees.Add(Tree("orders"));
        using var data = Create();

        await data.GetEstateAsync(refresh: false, CancellationToken.None);
        await data.GetEstateAsync(refresh: false, CancellationToken.None);
        await data.GetConfigAsync(refresh: false, CancellationToken.None);
        await data.GetConfigAsync(refresh: false, CancellationToken.None);
        var reused = (_status.Calls, _control.Reads);

        await data.GetEstateAsync(refresh: true, CancellationToken.None);
        _time.Advance(new ReplicationOptions().CacheLifetime);
        await data.GetConfigAsync(refresh: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(reused, Is.EqualTo((1, 1)));
            Assert.That(_status.Calls, Is.EqualTo(2));
            Assert.That(_control.Reads, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task A_sign_in_or_a_connection_change_forgets_the_cache_and_says_so()
    {
        using var data = Create();
        var invalidated = 0;
        data.Invalidated += () => invalidated++;

        await data.GetEstateAsync(refresh: false, CancellationToken.None);
        _auth.SignIn("dana");
        await data.GetEstateAsync(refresh: false, CancellationToken.None);
        await _session.ApplyAsync(SessionTestContext.RemoteConfiguration());
        await data.GetEstateAsync(refresh: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(_status.Calls, Is.EqualTo(3));
            Assert.That(invalidated, Is.EqualTo(2));
        });

        data.Dispose();
        Assert.Multiple(() =>
        {
            Assert.That(_auth.AuthenticationSubscribers, Is.Zero);
            Assert.That(_session.ConfigurationSubscribers, Is.Zero);
        });
    }

    [Test]
    public async Task A_read_in_flight_when_the_cache_is_forgotten_is_not_cached()
    {
        _status.Gate = new TaskCompletionSource();
        using var data = Create();

        var pending = data.GetEstateAsync(refresh: false, CancellationToken.None);
        data.Invalidate();
        _status.Gate.SetResult();
        await pending;
        _status.Gate = null;
        await data.GetEstateAsync(refresh: false, CancellationToken.None);

        Assert.That(_status.Calls, Is.EqualTo(2), "the stale read must not stand in for the fresh identity's");
    }

    [Test]
    public async Task Faults_are_classified_and_a_cancellation_escapes()
    {
        using var data = Create();
        _status.Failure = new LatticeAuthorizationDeniedException();
        var denied = await data.GetEstateAsync(refresh: true, CancellationToken.None);
        _status.Failure = new NotSupportedException();
        var unserved = await data.GetEstateAsync(refresh: true, CancellationToken.None);
        _control.ReadFailure = new TimeoutException();
        var failed = await data.GetConfigAsync(refresh: true, CancellationToken.None);

        _status.Failure = null;
        _status.Gate = new TaskCompletionSource();
        using var cancellation = new CancellationTokenSource();
        var cancelled = data.GetEstateAsync(refresh: true, cancellation.Token);
        cancellation.Cancel();

        Assert.Multiple(() =>
        {
            Assert.That(denied.Fault!.Kind, Is.EqualTo(ReplicationFaultKind.Denied));
            Assert.That(unserved.Fault!.Kind, Is.EqualTo(ReplicationFaultKind.NotServed));
            Assert.That(failed.Fault!.Kind, Is.EqualTo(ReplicationFaultKind.Failed));
            Assert.That(failed.Fault.Message, Does.Contain(ReplicationDataSource.ConfigSubject));
            Assert.That(async () => await cancelled, Throws.InstanceOf<OperationCanceledException>());
        });
    }

    [Test]
    public async Task With_no_facade_registered_every_read_fails_closed_as_not_served()
    {
        await using var provider = new ServiceCollection().BuildServiceProvider();
        using var data = new ReplicationDataSource(provider, _time, new ReplicationOptions());

        var status = await data.GetEstateAsync(refresh: false, CancellationToken.None);
        var tree = await data.GetTreeLinksAsync("orders", CancellationToken.None);
        var config = await data.GetConfigAsync(refresh: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(data.HasStatus, Is.False);
            Assert.That(data.HasControl, Is.False);
            Assert.That(status.Fault!.Kind, Is.EqualTo(ReplicationFaultKind.NotServed));
            Assert.That(tree.Fault!.Kind, Is.EqualTo(ReplicationFaultKind.NotServed));
            Assert.That(config.Fault!.Kind, Is.EqualTo(ReplicationFaultKind.NotServed));
            Assert.That(async () => await data.EnableAsync("orders", LatticeMergeMode.LwwRegister, null, CancellationToken.None), Throws.InstanceOf<NotSupportedException>());
            Assert.That(async () => await data.DisableAsync("orders", CancellationToken.None), Throws.InstanceOf<NotSupportedException>());
        });
    }

    [Test]
    public async Task One_trees_links_are_read_afresh_with_the_tree_in_the_query()
    {
        _status.Links.AddRange(Estate());
        using var data = Create();

        var first = await data.GetTreeLinksAsync("a/crm/contacts", CancellationToken.None);
        await data.GetTreeLinksAsync("a/crm/contacts", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first.Value!.Links.Select(link => link.PeerRegionId), Is.EqualTo(new[] { "ap-south", "ap-south" }));
            Assert.That(_status.Queries.Select(query => query.TreeId), Is.EqualTo(new[] { "a/crm/contacts", "a/crm/contacts" }));
            Assert.That(async () => await data.GetTreeLinksAsync("", CancellationToken.None), Throws.ArgumentException);
        });
    }

    [Test]
    public async Task Enable_and_disable_go_through_the_facade_and_forget_the_cache()
    {
        using var data = Create();
        var invalidated = 0;
        data.Invalidated += () => invalidated++;

        var enabled = await data.EnableAsync("orders", LatticeMergeMode.OrSet, "  eu-north  ", CancellationToken.None);
        var enabledWithoutBootstrap = await data.EnableAsync("stock", LatticeMergeMode.LwwRegister, " ", CancellationToken.None);
        var disabled = await data.DisableAsync("orders", CancellationToken.None);
        _control.ChangeFailure = new LatticeAuthorizationDeniedException();

        Assert.Multiple(() =>
        {
            Assert.That(enabled.Mode, Is.EqualTo(LatticeMergeMode.OrSet));
            Assert.That(_control.Enables[0], Is.EqualTo(("orders", LatticeMergeMode.OrSet, (string?)"eu-north")));
            Assert.That(_control.Enables[1].Bootstrap, Is.Null);
            Assert.That(enabledWithoutBootstrap.BootstrapRequested, Is.False);
            Assert.That(disabled.AlreadyDisabled, Is.False);
            Assert.That(_control.Disables, Is.EqualTo(new[] { "orders" }));
            Assert.That(invalidated, Is.EqualTo(3));
            Assert.That(async () => await data.DisableAsync("orders", CancellationToken.None), Throws.InstanceOf<LatticeAuthorizationDeniedException>());
            Assert.That(invalidated, Is.EqualTo(4), "a failed change forgets the cache too");
            Assert.That(async () => await data.EnableAsync("", LatticeMergeMode.LwwRegister, null, CancellationToken.None), Throws.ArgumentException);
        });
    }

    [Test]
    public void It_rejects_null_dependencies()
    {
        var provider = new ServiceCollection().BuildServiceProvider();
        Assert.Multiple(() =>
        {
            Assert.That(() => new ReplicationDataSource(null!, _time, new ReplicationOptions()), Throws.ArgumentNullException);
            Assert.That(() => new ReplicationDataSource(provider, null!, new ReplicationOptions()), Throws.ArgumentNullException);
            Assert.That(() => new ReplicationDataSource(provider, _time, null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task At_the_default_tenant_the_estate_and_the_enrolment_list_only_its_own_trees()
    {
        _status.Links.AddRange([Link("factory-floor", "west"), Link("sys-replication-config", "west"), Link("t/acme/a/task-board/tasks", "west")]);
        _control.Trees.AddRange([Tree("factory-floor"), Tree("sys-replication-config"), Tree("t/acme/a/task-board/tasks")]);
        using var data = Create(tenancy: true);

        var estate = await data.GetEstateAsync(refresh: false, CancellationToken.None);
        var config = await data.GetConfigAsync(refresh: false, CancellationToken.None);
        var addressed = await data.GetTreeLinksAsync("t/acme/a/task-board/tasks", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(estate.Value!.Links.Select(link => link.TreeId), Is.EqualTo(new[] { "factory-floor" }));
            Assert.That(config.Value!.Trees.Select(tree => tree.TreeId), Is.EqualTo(new[] { "factory-floor" }));
            Assert.That(addressed.Value!.Links, Has.Count.EqualTo(1), "one tree's links, asked for by its address, are read as addressed");
        });
    }

    [Test]
    public async Task Under_a_tenant_the_estate_drops_other_tenants_and_system_trees_but_never_a_bare_name()
    {
        _status.Links.AddRange([Link("a/task-board/tasks", "west"), Link("t/acme/orders", "west"), Link("t/globex/orders", "west"), Link("sys-replication-config", "west")]);
        using var data = Create(tenancy: true, tenant: "acme");

        var estate = await data.GetEstateAsync(refresh: false, CancellationToken.None);

        Assert.That(estate.Value!.Links.Select(link => link.TreeId), Is.EqualTo(new[] { "a/task-board/tasks", "t/acme/orders" }), "the cluster names the tenant's own trees by their bare names");
    }

    [Test]
    public void A_link_is_listed_by_its_tree_ownership()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ReplicationDataSource.ListsLink(null, "t/acme/orders"), Is.True, "tenancy off");
            Assert.That(ReplicationDataSource.ListsLink("default", "factory-floor"), Is.True);
            Assert.That(ReplicationDataSource.ListsLink("default", "t/acme/orders"), Is.False);
            Assert.That(ReplicationDataSource.ListsLink("default", "sys-replication-config"), Is.False);
            Assert.That(ReplicationDataSource.ListsLink("acme", "a/task-board/tasks"), Is.True);
            Assert.That(ReplicationDataSource.ListsLink("acme", "t/acme/orders"), Is.True);
            Assert.That(ReplicationDataSource.ListsLink("acme", "t/globex/orders"), Is.False);
            Assert.That(ReplicationDataSource.ListsLink("acme", "_lattice_internal"), Is.False);
            Assert.That(() => ReplicationDataSource.ListsLink("acme", null!), Throws.ArgumentNullException);
        });
    }
    [Test]
    public async Task Without_tenancy_nothing_is_narrowed()
    {
        _control.Trees.AddRange([Tree("factory-floor"), Tree("t/acme/orders")]);
        using var data = Create();

        var config = await data.GetConfigAsync(refresh: false, CancellationToken.None);

        Assert.That(config.Value!.Trees, Has.Count.EqualTo(2));
    }

    [Test]
    public void Scoping_a_report_that_needs_no_narrowing_returns_it_unchanged()
    {
        var report = new ReplicationConfigReport([Tree("t/acme/orders")]);

        Assert.Multiple(() =>
        {
            Assert.That(ReplicationDataSource.ScopeToTenant(report, "acme"), Is.SameAs(report));
            Assert.That(ReplicationDataSource.ScopeToTenant(report, "globex").Trees, Is.Empty);
            Assert.That(() => ReplicationDataSource.ScopeToTenant(null!, "acme"), Throws.ArgumentNullException);
        });
    }

    private ReplicationDataSource Create(ReplicationOptions? options = null, bool tenancy = false, string? tenant = null)
    {
        var services = new ServiceCollection()
            .AddKeyedSingleton<ILatticeReplicationStatus>(ShellFacades.Key, _status)
            .AddKeyedSingleton<ILatticeReplicationControl>(ShellFacades.Key, _control)
            .AddSingleton<Orleans.Lattice.Explorer.Core.Authentication.IExplorerAuthSession>(_auth)
            .AddSingleton<Orleans.Lattice.Explorer.Core.Configuration.IExplorerSession>(_session);
        if (tenancy)
        {
            // With tenancy on, the reserved default tenant is the one a call that asserts nothing is for.
            services.AddSingleton(new ShellAssertedTenant(new Orleans.Lattice.Explorer.Tests.Connection.FakeActiveTenantProvider(tenant)));
        }

        return new ReplicationDataSource(services.BuildServiceProvider(), _time, options ?? new ReplicationOptions());
    }
}
