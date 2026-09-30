using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Tests.UI.Areas.Tenancy;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Data;
using Orleans.Lattice.Explorer.UI.Suggestions;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// Trees another tenant shares with this one through a grant it approved: listed
/// after the tenant's own trees, marked with their owner and access, filterable,
/// opened read-only by their granted id under the tenant's own root, and left out
/// - with a note - when the caller cannot list the tenant's grants.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DataSharedTreesPageTests : DataTestContext
{
    [Test]
    public void An_approved_grant_is_listed_after_the_owned_trees_marked_and_linked_under_the_tenants_root()
    {
        ShareAcmeOrdersWithGlobex(TenantGrantLifecycleState.Active);

        var cut = RenderAt("t/globex/data");

        cut.WaitUntil(() =>
        {
            var rows = Rows(cut);
            Assert.That(rows, Has.Count.EqualTo(2));
            Assert.That(rows[0].QuerySelector("th a")!.TextContent, Is.EqualTo("orders"), "the tenant's own tree comes first");
            var shared = rows[1];
            var link = shared.QuerySelector("th a")!;
            Assert.That(link.TextContent, Is.EqualTo("t/acme/orders"));
            Assert.That(link.GetAttribute("href"), Is.EqualTo("t/globex/data/t/acme/orders"));
            Assert.That(shared.TextContent, Does.Contain("Shared tree").And.Contain("acme").And.Contain("Read only"));
            Assert.That(cut.FindAll(".lt-data-note"), Is.Empty);
        });
    }

    [Test]
    [TestCase(TenantGrantLifecycleState.Pending)]
    [TestCase(TenantGrantLifecycleState.Rejected)]
    [TestCase(TenantGrantLifecycleState.Revoked)]
    public void An_offered_rejected_or_revoked_grant_shares_nothing(TenantGrantLifecycleState state)
    {
        ShareAcmeOrdersWithGlobex(state);

        var cut = RenderAt("t/globex/data");

        cut.WaitUntil(() =>
        {
            Assert.That(Rows(cut), Has.Count.EqualTo(1));
            Assert.That(cut.Markup, Does.Not.Contain("t/acme/orders"));
        });
    }

    [Test]
    public void A_caller_who_cannot_list_the_grants_sees_the_owned_trees_only_with_a_note()
    {
        ShareAcmeOrdersWithGlobex(TenantGrantLifecycleState.Active);
        Grants.Fail(nameof(FakeTenancyCluster.ListGrantsAsync), FakeTenancyCluster.Denied());

        var cut = RenderAt("t/globex/data");

        cut.WaitUntil(() =>
        {
            Assert.That(Rows(cut), Has.Count.EqualTo(1));
            Assert.That(cut.Find(".lt-data-note").TextContent, Is.EqualTo("Trees other tenants share with tenant globex are not listed: you cannot list its grants. Only its own trees are shown."));
            Assert.That(cut.Markup, Does.Not.Contain("t/acme/orders").And.Not.Contain("not permitted"));
        });
    }

    [Test]
    public void A_bare_scope_that_shares_nothing_is_left_out_with_a_note()
    {
        UseDataTenancy("globex");
        Client.WithTree("t/globex/orders");
        Grants.WithTenant("globex").WithGrant("acme", "globex", "orders", TenantGrantLifecycleState.Active);

        var cut = RenderAt("t/globex/data");

        cut.WaitUntil(() =>
        {
            Assert.That(Rows(cut), Has.Count.EqualTo(1));
            Assert.That(cut.Find(".lt-data-note").TextContent, Does.StartWith("1 approved grant names a scope outside"));
        });
    }

    [Test]
    public void The_shared_filter_shows_only_shared_rows_and_the_trees_filter_only_owned_ones()
    {
        ShareAcmeOrdersWithGlobex(TenantGrantLifecycleState.Active);
        var cut = RenderAt("t/globex/data");
        cut.WaitUntil(() => Assert.That(Rows(cut), Has.Count.EqualTo(2)));

        cut.FindAll(".lt-data-segmented button").Single(button => button.TextContent == "Shared with this tenant").Click();
        cut.WaitUntil(() => Assert.That(Rows(cut).Single().QuerySelector("th a")!.TextContent, Is.EqualTo("t/acme/orders")));

        cut.FindAll(".lt-data-segmented button").Single(button => button.TextContent == "Trees").Click();
        cut.WaitUntil(() => Assert.That(Rows(cut).Single().QuerySelector("th a")!.TextContent, Is.EqualTo("orders")));
    }

    [Test]
    public void Without_tenancy_there_is_no_shared_filter_and_no_grant_is_read()
    {
        Client.WithTree("orders");

        var cut = RenderAt("data");

        cut.WaitUntil(() => Assert.That(Rows(cut), Has.Count.EqualTo(1)));
        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-data-segmented button").Select(button => button.TextContent), Is.EqualTo(new[] { "All", "Trees", "Views" }));
            Assert.That(cut.FindAll("th").Select(header => header.TextContent), Has.None.Contains("Shared by"));
            Assert.That(Grants.Calls, Does.Not.Contain(nameof(FakeTenancyCluster.ListGrantsAsync)));
        });
    }

    [Test]
    public void At_the_compact_band_four_kinds_become_a_select_and_a_shared_row_says_who_shares_it()
    {
        ShareAcmeOrdersWithGlobex(TenantGrantLifecycleState.Active);

        var cut = RenderAt("t/globex/data", compact: true);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll(".lt-data-segmented"), Is.Empty);
            Assert.That(Control(cut, "Show").QuerySelectorAll("option").Select(option => option.TextContent), Is.EqualTo(new[] { "All", "Trees", "Views", "Shared with this tenant" }));
            var shared = cut.FindAll(".lt-compact-row__secondary").Select(line => line.TextContent).Single(line => line.Contains("shared by", StringComparison.Ordinal));
            Assert.That(shared.Trim(), Does.EndWith("Shared tree - shared by acme"));
        });

        Control(cut, "Show").Change(DataDirectoryPage.SharedKinds);

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-compact-row__primary").Select(row => row.TextContent), Is.EqualTo(new[] { "t/acme/orders" })));
    }

    [Test]
    public void A_shared_prefix_is_a_row_without_a_workspace_link()
    {
        UseDataTenancy("globex");
        Grants.WithTenant("globex").WithGrant("acme", "globex", "t/acme/archive/", TenantGrantLifecycleState.Active);

        var cut = RenderAt("t/globex/data");

        cut.WaitUntil(() =>
        {
            var row = Rows(cut).Single();
            Assert.That(row.QuerySelector("th")!.TextContent.Trim(), Is.EqualTo("t/acme/archive/"));
            Assert.That(row.QuerySelector("th a"), Is.Null);
            Assert.That(row.TextContent, Does.Contain("Shared prefix"));
        });
    }

    [Test]
    public void A_shared_tree_opens_read_only_and_reads_by_its_granted_id()
    {
        ShareAcmeOrdersWithGlobex(TenantGrantLifecycleState.Active);
        Client.TagIndexes.Add(new TagIndexStateSummary { IndexName = "by-region", TreeId = "tag-by-region" });
        Client.Covered["by-region"] = ["t/acme/orders"];
        AdministeredTrees.Add("tag-by-region");

        var cut = RenderAt("t/globex/data/t/acme/orders");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("t/acme/orders"));
            Assert.That(cut.FindAll(".lt-data-heading .lt-pill").Select(pill => pill.TextContent.Trim()), Is.EqualTo(new[] { "Shared tree", "Shared by acme", "Read only" }));
            Assert.That(cut.Find(".lt-data-meta").TextContent, Does.Contain("Tenant acme shares this tree").And.Contain("administration is not offered"));
            Assert.That(Rows(cut), Has.Count.EqualTo(2));
        });
        Assert.That(Client.Calls, Has.Some.StartsWith("ScanEntriesAsync"));

        Navigation.NavigateTo("t/globex/data/t/acme/orders?tab=tag-indexes&index=by-region");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-data-entry h2").TextContent, Is.EqualTo("Tag index by-region")));
        Assert.That(cut.FindAll("button").Select(button => button.TextContent.Trim()), Has.None.EqualTo("Reconcile"), "a shared tree offers no administration");
    }

    [Test]
    public async Task A_tree_under_a_shared_prefix_resolves_against_the_grant_and_nothing_else_does()
    {
        UseDataTenancy("globex");
        Grants.WithTenant("globex").WithGrant("acme", "globex", "t/acme/archive/", TenantGrantLifecycleState.Active, TenantGrantAccess.ReadWrite);
        var directory = Services.GetRequiredService<DataDirectory>();

        var covered = await directory.ResolveAsync(ExplorerAddressFor("/t/globex/data/t/acme/archive/2023"));
        var uncovered = await directory.ResolveAsync(ExplorerAddressFor("/t/globex/data/t/acme/orders"));
        var prefix = await directory.ResolveAsync(ExplorerAddressFor("/t/globex/data/t/acme/archive"));

        Assert.Multiple(() =>
        {
            Assert.That((covered!.StateId, covered.SharedBy, covered.AccessText), Is.EqualTo(("t/acme/archive/2023", "acme", "Read and write")));
            Assert.That(uncovered, Is.Null);
            Assert.That(prefix, Is.Null, "a prefix row is never opened as a tree");
        });
    }

    [Test]
    public async Task The_home_status_badge_completions_and_tree_pickers_include_shared_trees()
    {
        ShareAcmeOrdersWithGlobex(TenantGrantLifecycleState.Active);
        Grants.WithGrant("acme", "globex", "t/acme/archive/", TenantGrantLifecycleState.Active);

        var status = await Area.GetHomeStatusAsync(CancellationToken.None);
        var badge = await Area.GetDirectoryBadgeAsync(CancellationToken.None);
        var completions = await Area.Completions!.CompleteAsync(
            new Orleans.Lattice.Explorer.UI.Navigation.AddressQuery("t/acme", Orleans.Lattice.Explorer.UI.Navigation.AddressQueryMode.Search, ExplorerAddressFor("/t/globex/data")),
            CancellationToken.None);
        var suggestions = await Services.GetRequiredService<ExplorerSuggestions>().Trees.SuggestAsync("t/acme", 8, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(status, Is.EqualTo("1 tree and 0 views, and 2 shared with this tenant."));
            Assert.That(badge, Is.EqualTo("3"));
            Assert.That(completions.Select(completion => (completion.Label, completion.Detail)), Is.EqualTo(new[] { ("t/acme/orders", "Shared tree from tenant acme, read only") }), "a prefix names no tree to go to");
            Assert.That(suggestions.Items.Select(item => (item.Value, item.Detail)), Is.EqualTo(new[] { ("t/acme/orders", "Shared by acme, read only") }));
        });
    }

    private void ShareAcmeOrdersWithGlobex(TenantGrantLifecycleState state)
    {
        UseDataTenancy("globex");
        Client.WithTree("t/globex/orders").WithTree("t/acme/orders", keys: 2);
        Grants.WithTenant("globex").WithGrant("acme", "globex", "t/acme/orders", state);
    }

    private static Orleans.Lattice.Explorer.UI.Navigation.Address.ExplorerAddress ExplorerAddressFor(string address) =>
        Orleans.Lattice.Explorer.UI.Navigation.Address.ExplorerAddress.Parse(address);
}
