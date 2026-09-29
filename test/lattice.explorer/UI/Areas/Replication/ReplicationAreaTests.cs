using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Explorer.UI;
using Orleans.Lattice.Explorer.UI.Areas.Replication;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Session;
using static Orleans.Lattice.Explorer.Tests.UI.Areas.Replication.ReplicationTestData;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Replication;

/// <summary>
/// The area contract: fail-closed visibility from the facades' own answers, the Home
/// status line and directory badge, the palette commands, the address completions,
/// and the registration seam.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ReplicationAreaTests
{
    private readonly ManualTimeProvider _time = new();
    private readonly FakeReplicationStatus _status = new();
    private readonly FakeReplicationControl _control = new();

    [Test]
    public async Task It_is_visible_when_the_caller_can_read_peer_status()
    {
        var area = Create();
        var availability = await area.GetAvailabilityAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(area.Key, Is.EqualTo("replication"));
            Assert.That(area.DisplayName, Is.EqualTo("Replication"));
            Assert.That(area.DirectoryOrder, Is.EqualTo(60));
            Assert.That(((IExplorerArea)area).IsTenantScoped, Is.True);
            Assert.That(availability, Is.EqualTo(AreaAvailability.Visible));
        });
    }

    [Test]
    public async Task A_restricted_identity_that_manages_trees_but_cannot_read_status_still_sees_the_area()
    {
        _status.Failure = new LatticeAuthorizationDeniedException();
        _control.Trees.Add(Tree("orders"));

        Assert.That(await Create().GetAvailabilityAsync(CancellationToken.None), Is.EqualTo(AreaAvailability.Visible));
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task It_is_hidden_when_status_is_denied_and_no_tree_is_manageable(bool configThrows)
    {
        _status.Failure = new LatticeAuthorizationDeniedException();
        if (configThrows)
        {
            _control.ReadFailure = new InvalidOperationException("not connected");
        }

        Assert.That(await Create().GetAvailabilityAsync(CancellationToken.None), Is.EqualTo(AreaAvailability.Hidden));
    }

    [Test]
    public async Task It_is_hidden_when_no_replication_facade_is_registered()
    {
        await using var provider = new ServiceCollection().BuildServiceProvider();
        var data = new ReplicationDataSource(provider, _time, new ReplicationOptions());
        var area = new ReplicationArea(data, new ReplicationCompletionSource(data));

        var availability = await area.GetAvailabilityAsync(CancellationToken.None);
        var home = await area.GetHomeStatusAsync(CancellationToken.None);
        var badge = await area.GetDirectoryBadgeAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(availability, Is.EqualTo(AreaAvailability.Hidden));
            Assert.That(home, Is.Null);
            Assert.That(badge, Is.Null);
        });
    }

    [Test]
    public async Task Home_names_peers_links_and_what_is_stalled_or_lagging()
    {
        _status.Links.AddRange(Estate());

        Assert.That(await Create().GetHomeStatusAsync(CancellationToken.None),
            Is.EqualTo("3 peer regions, 6 links: 1 stalled, 1 lagging."));
    }

    [Test]
    public async Task Home_says_when_every_link_is_fine_or_there_are_none()
    {
        var none = await Create().GetHomeStatusAsync(CancellationToken.None);
        _status.Links.Add(Link("orders", "us-east"));
        var one = await Create().GetHomeStatusAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(none, Is.EqualTo("No replication links yet."));
            Assert.That(one, Is.EqualTo("1 peer region, 1 link, none stalled or lagging."));
        });
    }

    [Test]
    public async Task Home_falls_back_to_the_enrolled_tree_count_when_status_is_not_readable()
    {
        _status.Failure = new NotSupportedException();
        _control.Trees.Add(Tree("orders"));
        _control.Trees.Add(Tree("stock", enabled: false));

        Assert.That(await Create().GetHomeStatusAsync(CancellationToken.None), Is.EqualTo("1 tree enrolled."));
    }

    [Test]
    public async Task The_badge_counts_stalled_links_first_then_lagging_ones()
    {
        _status.Links.AddRange(Estate());
        var stalled = await Create().GetDirectoryBadgeAsync(CancellationToken.None);

        _status.Links.RemoveAll(link => link.Health == ReplicationLinkHealth.Stalled);
        var lagging = await Create().GetDirectoryBadgeAsync(CancellationToken.None);

        _status.Links.RemoveAll(link => link.Health == ReplicationLinkHealth.Lagging);
        var healthy = await Create().GetDirectoryBadgeAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(stalled, Is.EqualTo("1 stalled"));
            Assert.That(lagging, Is.EqualTo("1 lag"));
            Assert.That(healthy, Is.Null);
        });
    }

    [Test]
    public async Task The_badge_reuses_the_cached_read_on_every_navigation()
    {
        var area = Create();
        await area.GetAvailabilityAsync(CancellationToken.None);
        await area.GetDirectoryBadgeAsync(CancellationToken.None);
        await area.GetDirectoryBadgeAsync(CancellationToken.None);
        await area.GetHomeStatusAsync(CancellationToken.None);

        Assert.That(_status.Calls, Is.EqualTo(1));
    }

    [Test]
    public async Task The_commands_refresh_and_show_the_trees()
    {
        var area = Create();
        await area.GetAvailabilityAsync(CancellationToken.None);

        var refresh = area.Commands.Single(command => command.Id == ReplicationArea.RefreshCommandId);
        var trees = area.Commands.Single(command => command.Id == ReplicationArea.TreesCommandId);
        await refresh.InvokeAsync!(CancellationToken.None);
        await area.GetDirectoryBadgeAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(area.Commands.Select(command => command.Id), Is.EqualTo(new[] { "replication.refresh", "replication.trees" }));
            Assert.That(refresh.Target, Is.EqualTo(ReplicationAddresses.Estate));
            Assert.That(trees.Target, Is.EqualTo(ReplicationAddresses.Trees));
            Assert.That(trees.InvokeAsync, Is.Null);
            Assert.That(_status.Calls, Is.EqualTo(2), "refresh forgets the cached read");
        });
    }

    [Test]
    public async Task Search_completes_region_ids_and_replicated_trees()
    {
        _status.Links.AddRange(Estate());
        _control.Trees.Add(Tree("south-orders"));
        var area = Create();

        var south = await area.Completions!.CompleteAsync(new AddressQuery("south", AddressQueryMode.Search, ExplorerAddress.Home), CancellationToken.None);
        var local = await area.Completions!.CompleteAsync(new AddressQuery("eu-", AddressQueryMode.Search, ExplorerAddress.Home), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(south.Select(completion => (completion.Label, completion.Target.Format(), completion.Detail)), Is.EqualTo(new[]
            {
                ("ap-south", "/replication?region=ap-south", "Peer region"),
                ("south-orders", "/replication/trees/south-orders", "Replicated tree"),
            }));
            Assert.That(local.Single().Target, Is.EqualTo(ReplicationAddresses.Estate));
            Assert.That(local.Single().Detail, Is.EqualTo("This region"));
        });
    }

    [Test]
    public async Task App_mode_completes_the_trees_of_matching_apps_and_address_mode_completes_tree_paths()
    {
        _status.Links.AddRange(Estate());
        var area = Create();

        var apps = await area.Completions!.CompleteAsync(new AddressQuery("cr", AddressQueryMode.App, ExplorerAddress.Home), CancellationToken.None);
        var paths = await area.Completions!.CompleteAsync(new AddressQuery("/replication/trees/a/b", AddressQueryMode.Address, ExplorerAddress.Home), CancellationToken.None);
        var elsewhere = await area.Completions!.CompleteAsync(new AddressQuery("/data/a", AddressQueryMode.Address, ExplorerAddress.Home), CancellationToken.None);
        var tenant = await area.Completions!.CompleteAsync(new AddressQuery("orders", AddressQueryMode.Search, ExplorerAddress.Home.WithTenant("acme")), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(apps.Select(completion => completion.Label), Is.EqualTo(new[] { "a/crm/contacts" }));
            Assert.That(paths.Select(completion => completion.Label), Is.EqualTo(new[] { "a/billing/invoices" }));
            Assert.That(elsewhere, Is.Empty);
            Assert.That(tenant.Single().Target.Format(), Is.EqualTo("/t/acme/replication/trees/orders"));
        });
    }

    [Test]
    public async Task Completions_stop_at_the_query_limit()
    {
        for (var i = 0; i < 40; i++)
        {
            _status.Links.Add(Link($"tree-{i:00}", $"region-{i:00}"));
        }

        var results = await Create().Completions!.CompleteAsync(new AddressQuery("-", AddressQueryMode.Search, ExplorerAddress.Home), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(results, Has.Count.EqualTo(AddressQuery.MaximumResults));
            Assert.That(async () => await Create().Completions!.CompleteAsync(null!, CancellationToken.None), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void It_rejects_null_dependencies()
    {
        var data = new ReplicationDataSource(new ServiceCollection().BuildServiceProvider(), _time, new ReplicationOptions());
        Assert.Multiple(() =>
        {
            Assert.That(() => new ReplicationArea(null!, new ReplicationCompletionSource(data)), Throws.ArgumentNullException);
            Assert.That(() => new ReplicationArea(data, null!), Throws.ArgumentNullException);
            Assert.That(() => new ReplicationCompletionSource(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task The_shell_registers_the_area_once_and_it_resolves_in_a_scope()
    {
        var services = new ServiceCollection();
        services.AddSingleton<Microsoft.AspNetCore.Components.NavigationManager>(new TestNavigationManager());
        services.AddSingleton<Microsoft.JSInterop.IJSRuntime>(NSubstitute.Substitute.For<Microsoft.JSInterop.IJSRuntime>());
        services.AddSingleton<Orleans.Lattice.Explorer.Core.Configuration.IExplorerSession>(new FakeExplorerSession(new FakeStateConnection()));
        services.AddSingleton<Orleans.Lattice.Explorer.Core.Authentication.IExplorerAuthSession>(new FakeAuthSession());
        services.AddSingleton<ILatticeReplicationStatus>(_status);
        services.AddSingleton<ILatticeReplicationControl>(_control);
        services.AddLatticeExplorerShell();
        services.AddLatticeExplorerShell();

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
        await using var scope = provider.CreateAsyncScope();

        Assert.Multiple(() =>
        {
            Assert.That(services.Count(descriptor => descriptor.ServiceType == typeof(IExplorerArea) && descriptor.ImplementationType == typeof(ReplicationArea)), Is.EqualTo(1));
            Assert.That(scope.ServiceProvider.GetServices<IExplorerArea>().OfType<ReplicationArea>().Single().Completions,
                Is.SameAs(scope.ServiceProvider.GetRequiredService<ReplicationCompletionSource>()));
            Assert.That(scope.ServiceProvider.GetRequiredService<IReplicationPageVisibility>(), Is.InstanceOf<JsReplicationPageVisibility>());
            Assert.That(provider.GetRequiredService<ReplicationOptions>(), Is.Not.Null);
            Assert.That(scope.ServiceProvider.GetRequiredService<ReplicationDataSource>().HasStatus, Is.True);
        });
    }

    private ReplicationArea Create()
    {
        var provider = new ServiceCollection()
            .AddSingleton<ILatticeReplicationStatus>(_status)
            .AddSingleton<ILatticeReplicationControl>(_control)
            .BuildServiceProvider();
        var data = new ReplicationDataSource(provider, _time, new ReplicationOptions());
        return new ReplicationArea(data, new ReplicationCompletionSource(data));
    }
}
