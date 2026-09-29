using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.UI.Areas.Data;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Suggestions;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// The shared tree and key sources, over the Data area's tenant-scoped catalogue
/// and data reader: logical ids only, the active tenant's trees only, views left
/// out, a bounded key scan whose cursor is released, and a fail-closed note when
/// nothing can be read.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DataSuggestionSourcesTests : DataTestContext
{
    [Test]
    public async Task Trees_are_offered_by_logical_id_and_only_the_active_tenants()
    {
        UseDataTenancy("acme");
        Client.WithTree("t/acme/orders").WithTree("t/acme/a/crm/contacts").WithTree("t/globex/billing");
        var source = Services.GetRequiredService<ExplorerSuggestions>().Trees;

        var answer = await source.SuggestAsync(string.Empty, 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(answer.Items.Select(item => item.Value), Is.EqualTo(new[] { "a/crm/contacts", "orders" }));
            Assert.That(answer.Items[0].Detail, Is.EqualTo("App tree"));
        });
    }

    [Test]
    public async Task The_projection_is_reused_while_the_catalogue_is_unchanged()
    {
        Client.WithTree("orders").WithTree("billing");
        var source = Services.GetRequiredService<ExplorerSuggestions>().Trees;

        var first = await source.SuggestAsync("orders", 1, CancellationToken.None);
        var second = await source.SuggestAsync("orders", 1, CancellationToken.None);

        Assert.That(second.Items.Single(), Is.SameAs(first.Items.Single()), "no suggestion is rebuilt per keystroke");
    }

    [Test]
    public async Task A_tree_source_without_a_readable_catalogue_is_unavailable()
    {
        Client.Fault = call => call == "ListTreesAsync" ? new InvalidOperationException("down") : null;
        var source = new TreeSuggestionSource(Services.GetRequiredService<DataDirectory>(), Services.GetRequiredService<ExplorerTenancy>());

        var answer = await source.SuggestAsync("o", 5, CancellationToken.None);

        Assert.That(answer.UnavailableReason, Is.EqualTo(TreeSuggestionSource.UnavailableReason));
    }

    [Test]
    public async Task Keys_are_a_bounded_prefix_scan_whose_cursor_is_released()
    {
        Client.WithTree("orders", keys: 30, prefix: "order/");
        var reader = Services.GetRequiredService<Orleans.Lattice.Explorer.Core.Data.IDataReader>();
        var source = new DataKeySuggestionSource(reader, "orders");

        var answer = await source.SuggestAsync("order/001", 3, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(answer.Items.Select(item => item.Value), Is.EqualTo(new[] { "order/0010", "order/0011", "order/0012" }));
            Assert.That(answer.Truncated, Is.True);
            Assert.That(source.StateId, Is.EqualTo("orders"));
        });
    }

    [Test]
    public async Task A_key_source_over_an_unreadable_tree_is_unavailable()
    {
        Client.Fault = _ => new InvalidOperationException("down");
        var reader = Services.GetRequiredService<Orleans.Lattice.Explorer.Core.Data.IDataReader>();

        var answer = await new DataKeySuggestionSource(reader, "orders").SuggestAsync("k", 3, CancellationToken.None);

        Assert.That(answer.UnavailableReason, Is.EqualTo(DataKeySuggestionSource.UnavailableReason));
    }

    [Test]
    public async Task The_hub_builds_each_source_once_and_its_tenant_source_says_when_tenancy_is_off()
    {
        var hub = Services.GetRequiredService<ExplorerSuggestions>();

        var tenants = await hub.Tenants.SuggestAsync(string.Empty, 5, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(hub.Trees, Is.SameAs(hub.Trees));
            Assert.That(hub.Regions, Is.SameAs(hub.Regions));
            Assert.That(hub.ClusterTrees, Is.SameAs(hub.ClusterTrees));
            Assert.That(hub.Principals, Is.SameAs(hub.Principals));
            Assert.That(hub.GroupsOrStoredGroups, Is.Not.SameAs(hub.Groups));
            Assert.That(hub.Subjects(Orleans.Lattice.Auth.LatticeSubjectSelectorKind.Group), Is.SameAs(hub.Groups));
            Assert.That(hub.Subjects(Orleans.Lattice.Auth.LatticeSubjectSelectorKind.User), Is.SameAs(hub.Users));
            Assert.That(tenants.UnavailableReason, Does.StartWith("Tenancy is off"));
        });
    }

    [Test]
    public async Task The_tenant_source_lists_the_reachable_tenants_and_marks_the_active_one()
    {
        UseTenancy("acme", accessible: ["acme", "globex"]);

        var answer = await Services.GetRequiredService<ExplorerSuggestions>().Tenants.SuggestAsync(string.Empty, 5, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(answer.Items.Select(item => item.Value), Is.EqualTo(new[] { "acme", "globex" }));
            Assert.That(answer.Items[0].Detail, Is.EqualTo(TenantSuggestionSource.ActiveDetail));
            Assert.That(answer.Items[1].Detail, Is.Null);
        });
    }
}
