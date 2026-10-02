using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NSubstitute;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Tests.UI.Areas.Tenancy;
using Orleans.Lattice.Explorer.UI.Areas.Data;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// What the Data directory does when part of the catalogue is out of reach:
/// an address that names no tree, a question asked before the first load, a head
/// serving no catalogue at all, a view catalogue the caller may not read, a grant
/// facade that is absent or faulting, and a view whose source tree this tenant
/// cannot see.
/// </summary>
/// <remarks>
/// Each of these is a fail-closed arm whose whole job is to return nothing rather
/// than to fail the directory, so none of them is observable from the rendered
/// page the sibling fixtures drive - the page looks the same as one listing a
/// genuinely empty catalogue. They are reached by asking the directory directly.
/// </remarks>
[TestFixture]
[FixtureLifeCycle(NUnit.Framework.LifeCycle.InstancePerTestCase)]
public sealed class DataDirectoryReachabilityTests : DataTestContext
{
    private DataDirectory Directory => Services.GetRequiredService<DataDirectory>();

    [Test]
    public async Task An_address_that_names_no_tree_resolves_to_nothing()
    {
        Client.WithTree("orders");

        Assert.That(await Directory.ResolveAsync(ExplorerAddress.Parse("/data")), Is.Null);
    }

    [Test]
    public void Resolving_a_null_address_is_refused()
    {
        Assert.That(async () => await Directory.ResolveAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task An_address_naming_an_unknown_tree_resolves_to_nothing()
    {
        Client.WithTree("orders");

        Assert.That(await Directory.ResolveAsync(ExplorerAddress.Parse("/data/absent")), Is.Null);
    }

    [Test]
    public void The_views_of_a_tree_are_empty_before_the_first_load()
    {
        var tree = new DataTreeEntry { LogicalId = "orders", StateId = "orders", Kind = DataTreeKind.Tree };

        Assert.Multiple(() =>
        {
            Assert.That(Directory.Loaded, Is.Null, "nothing has been loaded yet");
            Assert.That(Directory.ViewsOf(tree), Is.Empty);
        });
    }

    [Test]
    public void The_views_of_a_null_tree_are_refused()
    {
        Assert.That(() => Directory.ViewsOf(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task A_head_with_no_catalogue_reader_probes_false_rather_than_throwing()
    {
        Services.RemoveAll<ICatalogReader>();

        Assert.Multiple(async () =>
        {
            Assert.That(Directory.HasReader, Is.False);
            Assert.That(await Directory.ProbeAsync(CancellationToken.None), Is.False);
        });
    }

    [Test]
    public async Task A_caller_refused_the_view_catalogue_still_gets_the_trees()
    {
        Client.WithTree("orders");
        Client.Fault = call => call == nameof(ILatticeStateClient.ListViewsAsync)
            ? new LatticeStateApiException("Access to the view catalogue was denied.") { IsPermissionDenied = true }
            : null;

        var entries = await Directory.LoadAsync();

        Assert.Multiple(() =>
        {
            Assert.That(entries.Select(entry => entry.LogicalId), Does.Contain("orders"));
            Assert.That(entries.Any(entry => entry.Kind == DataTreeKind.View), Is.False, "views are simply absent");
        });
    }

    [Test]
    public async Task A_head_that_does_not_offer_the_view_catalogue_still_gets_the_trees()
    {
        Client.WithTree("orders");
        Client.Fault = call => call == nameof(ILatticeStateClient.ListViewsAsync)
            ? new NotSupportedException("The view catalogue is not offered by this cluster.")
            : null;

        var entries = await Directory.LoadAsync();

        Assert.That(entries.Select(entry => entry.LogicalId), Does.Contain("orders"));
    }

    [Test]
    public async Task A_tenant_whose_head_offers_no_grant_facade_lists_only_its_own_trees()
    {
        UseDataTenancy("acme");
        Services.RemoveAll<ILatticeTenantGrantAdmin>();
        Services.RemoveAllKeyed<ILatticeTenantGrantAdmin>(ShellFacades.Key);
        Client.WithTree("t/acme/orders");

        var entries = await Directory.LoadAsync();

        Assert.Multiple(() =>
        {
            Assert.That(entries.Any(entry => entry.IsShared), Is.False);
            Assert.That(Directory.SharingNote, Is.Not.Null, "the listing says why nothing is shared");
        });
    }

    [Test]
    public async Task A_grant_facade_whose_transport_is_not_configured_lists_only_the_tenants_own_trees()
    {
        // The facade is registered but cannot be built - the shape the Explorer hits
        // when a head wires a facade whose state connection it does not serve. That
        // is a resolution failure, not a refusal, and it must still fail closed to
        // the owned trees rather than fail the directory.
        UseDataTenancy("acme");
        Services.AddKeyedSingleton<ILatticeTenantGrantAdmin>(
            ShellFacades.Key,
            (_, _) => throw new InvalidOperationException("This head serves no tenant administration facade."));
        Client.WithTree("t/acme/orders");

        var entries = await Directory.LoadAsync();

        Assert.Multiple(() =>
        {
            Assert.That(entries.Select(entry => entry.LogicalId), Does.Contain("orders"));
            Assert.That(entries.Any(entry => entry.IsShared), Is.False);
            Assert.That(Directory.SharingNote, Is.Not.Null, "the listing says why nothing is shared");
        });
    }

    [Test]
    public async Task A_grant_listing_that_fails_falls_back_to_the_owned_trees_with_a_note()
    {
        UseDataTenancy("acme");
        Grants.Fail(nameof(ILatticeTenantGrantAdmin.ListGrantsAsync), FakeTenancyCluster.Denied());
        Client.WithTree("t/acme/orders");

        var entries = await Directory.LoadAsync();

        Assert.Multiple(() =>
        {
            Assert.That(entries.Select(entry => entry.LogicalId), Does.Contain("orders"));
            Assert.That(Directory.SharingNote, Is.Not.Null);
        });
    }

    [Test]
    public async Task A_grant_listing_cancelled_by_the_circuit_ending_does_not_become_a_sharing_note()
    {
        // The circuit's own lifetime ending is not evidence about sharing: it must
        // propagate so the load is abandoned, not be recorded as "nothing is shared".
        UseDataTenancy("acme");

        var entered = new TaskCompletionSource();
        var grants = Substitute.For<ILatticeTenantGrantAdmin>();
        grants.ListGrantsAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                entered.TrySetResult();
                return NeverAnswers(call.Arg<CancellationToken>());
            });

        Services.AddKeyedSingleton(ShellFacades.Key, grants);
        Client.WithTree("t/acme/orders");

        var directory = Directory;
        var load = directory.LoadAsync();
        await entered.Task;

        directory.Dispose();

        Assert.That(
            async () => await load,
            Throws.InstanceOf<OperationCanceledException>(),
            "the load is abandoned rather than reported as an empty share set");
    }

    private static async Task<TenantGrantReport> NeverAnswers(CancellationToken cancellationToken)
    {
        await Task.Delay(Timeout.Infinite, cancellationToken);
        return new TenantGrantReport { TenantId = "acme", Issued = [], Received = [] };
    }

    [Test]
    public async Task A_view_whose_source_tree_this_tenant_cannot_see_is_left_out()
    {
        // Fail closed: with tenancy on, a view the catalogue names but whose source
        // tree is not in this tenant's visible set belongs to another tenant.
        UseDataTenancy("acme");
        Client.WithTree("t/acme/orders");
        Client.Views.Add(new ViewStateSummary { ViewName = "by-customer", SourceTreeId = "t/other/orders" });

        var entries = await Directory.LoadAsync();

        Assert.That(
            entries.Any(entry => entry.Kind == DataTreeKind.View),
            Is.False,
            "a view whose source tree is invisible here is not listed");
    }

    [Test]
    public async Task A_view_whose_source_tree_this_tenant_can_see_is_listed_against_it()
    {
        UseDataTenancy("acme");
        Client.WithTree("t/acme/orders");
        Client.Views.Add(new ViewStateSummary { ViewName = "by-customer", SourceTreeId = "t/acme/orders" });

        var entries = await Directory.LoadAsync();
        var tree = entries.Single(entry => entry.Kind == DataTreeKind.Tree);

        Assert.Multiple(() =>
        {
            Assert.That(entries.Any(entry => entry.Kind == DataTreeKind.View), Is.True);
            Assert.That(Directory.ViewsOf(tree).Select(view => view.ViewName), Does.Contain("by-customer"));
        });
    }
}
