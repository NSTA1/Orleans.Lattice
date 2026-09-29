using Orleans.Lattice.Explorer.Shell.Areas.Access;
using Orleans.Lattice.Explorer.Shell.Navigation;
using Orleans.Lattice.Explorer.Shell.Navigation.Address;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Access;

/// <summary>
/// Address-line completion over the access catalogue: <c>group:</c> and
/// <c>rule:</c> narrowing, free text across both with prefix matches first, raw
/// addresses, the result limit, and the modes it leaves to others.
/// </summary>
[TestFixture]
public sealed class AccessCompletionSourceTests
{
    private FakeAuthAdmin _admin = null!;
    private AccessCompletionSource _source = null!;

    [SetUp]
    public void SetUp()
    {
        _admin = new FakeAuthAdmin()
            .WithGroup("ops", "Operations")
            .WithGroup("devops", "Developers")
            .WithGroup("sales", "Sales")
            .WithRule(AccessTestContext.Rule("ops-read"))
            .WithRule(AccessTestContext.Rule("orders-write", group: "sales"));
        _source = new AccessCompletionSource(new AccessCatalog(_admin));
    }

    [Test]
    public async Task A_group_prefix_completes_groups_to_their_pages()
    {
        var results = await CompleteAsync("group:ops");

        Assert.Multiple(() =>
        {
            Assert.That(results.Select(result => result.Label), Is.EqualTo(new[] { "group:ops", "group:devops" }), "prefix matches first");
            Assert.That(results[0].Target, Is.EqualTo(AccessRoutes.Group("ops")));
            Assert.That(results[0].Detail, Is.EqualTo("Operations"));
        });
    }

    [Test]
    public async Task A_rule_prefix_completes_rules_to_their_pages_by_tree()
    {
        var results = await CompleteAsync("rule:orders");

        Assert.Multiple(() =>
        {
            Assert.That(results.Select(result => result.Label), Is.EqualTo(new[] { "rule:orders-write" }));
            Assert.That(results[0].Target.Format(), Is.EqualTo("/access/rules/orders-write?tree=orders"));
            Assert.That(results[0].Detail, Is.EqualTo("Allow group:sales on orders"));
        });
    }

    [Test]
    public async Task Free_text_matches_groups_and_rules_by_id_or_display_name()
    {
        var results = await CompleteAsync("Develop");

        Assert.That(results.Select(result => result.Label), Is.EqualTo(new[] { "group:devops" }));
    }

    [Test]
    public async Task A_bare_prefix_lists_everything_of_that_kind()
    {
        var results = await CompleteAsync("rule:");

        Assert.That(results, Has.Count.EqualTo(2));
    }

    [Test]
    public async Task Empty_free_text_completes_nothing()
    {
        Assert.That(await CompleteAsync("  "), Is.Empty);
        Assert.That(_admin.Calls, Is.Empty, "nothing is read for an empty query");
    }

    [Test]
    public async Task A_raw_address_completes_the_groups_or_rules_under_it()
    {
        var groups = await CompleteAsync("/access/groups/sa", AddressQueryMode.Address);
        var rules = await CompleteAsync("/access/rules/ops", AddressQueryMode.Address);
        var other = await CompleteAsync("/data/orders", AddressQueryMode.Address);

        Assert.Multiple(() =>
        {
            Assert.That(groups.Select(result => result.Label), Is.EqualTo(new[] { "group:sales" }));
            Assert.That(rules.Select(result => result.Label), Is.EqualTo(new[] { "rule:ops-read" }));
            Assert.That(other, Is.Empty);
        });
    }

    [Test]
    [TestCase("App")]
    [TestCase("Tenant")]
    [TestCase("Command")]
    public async Task Chrome_modes_are_left_to_the_chrome(string mode)
    {
        Assert.That(await CompleteAsync("ops", Enum.Parse<AddressQueryMode>(mode)), Is.Empty);
    }

    [Test]
    public async Task Results_are_capped_at_the_query_limit()
    {
        for (var i = 0; i < 30; i++)
        {
            _admin.WithGroup($"team-{i:00}");
        }

        var results = await CompleteAsync("group:team");

        Assert.That(results, Has.Count.EqualTo(AddressQuery.MaximumResults));
    }

    [Test]
    public async Task The_catalogue_is_read_once_until_it_is_invalidated()
    {
        var catalog = new AccessCatalog(_admin);
        var source = new AccessCompletionSource(catalog);

        await source.CompleteAsync(new AddressQuery("group:o", AddressQueryMode.Search, ExplorerAddress.Home), CancellationToken.None);
        await source.CompleteAsync(new AddressQuery("group:s", AddressQueryMode.Search, ExplorerAddress.Home), CancellationToken.None);
        Assert.That(_admin.Calls.Count(call => call == nameof(FakeAuthAdmin.ListGroupsAsync)), Is.EqualTo(1));

        catalog.Invalidate();
        _admin.WithGroup("support");
        var results = await source.CompleteAsync(new AddressQuery("group:sup", AddressQueryMode.Search, ExplorerAddress.Home), CancellationToken.None);

        Assert.That(results.Select(result => result.Label), Is.EqualTo(new[] { "group:support" }));
    }

    [Test]
    public void A_failing_catalogue_faults_the_completion_so_the_chrome_reports_it()
    {
        _admin.Fail(nameof(FakeAuthAdmin.ListGroupsAsync), new InvalidOperationException("down"));

        Assert.ThrowsAsync<InvalidOperationException>(async () => await CompleteAsync("group:o"));
    }

    private async Task<IReadOnlyList<AddressCompletion>> CompleteAsync(string text, AddressQueryMode mode = AddressQueryMode.Search) =>
        await _source.CompleteAsync(new AddressQuery(text, mode, ExplorerAddress.Home), CancellationToken.None);
}
