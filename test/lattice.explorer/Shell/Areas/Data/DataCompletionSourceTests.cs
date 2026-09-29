using Orleans.Lattice.Explorer.Shell.Areas.Data;
using Orleans.Lattice.Explorer.Shell.Navigation;
using Orleans.Lattice.Explorer.Shell.Navigation.Address;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Data;

/// <summary>
/// The Data area's address-line completions: trees by logical-id prefix
/// (bounded), app trees after <c>a/</c>, literal <c>/data/...</c> addresses, and
/// key prefixes while a tree's keys are open.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DataCompletionSourceTests : DataTestContext
{
    [Test]
    public async Task A_search_prefix_matches_logical_ids_and_is_bounded()
    {
        for (var i = 0; i < 30; i++)
        {
            Client.WithTree($"orders-{i:D2}");
        }

        Client.WithTree("customers");

        var results = await Complete("ord", AddressQueryMode.Search);

        Assert.Multiple(() =>
        {
            Assert.That(results, Has.Count.EqualTo(AddressQuery.MaximumResults));
            Assert.That(results.All(result => result.Label.StartsWith("orders-", StringComparison.Ordinal)), Is.True);
            Assert.That(results[0].Target, Is.EqualTo(ExplorerAddress.ForTree("data", "orders-00")));
            Assert.That(results[0].Detail, Is.EqualTo("Tree"));
        });
    }

    [Test]
    public async Task After_a_slash_only_app_trees_are_offered_with_their_app()
    {
        Client.WithTree("a/crm/orders").WithTree("a/crm/customers").WithTree("a/billing/invoices").WithTree("crm-legacy");

        var results = await Complete("crm/", AddressQueryMode.App);

        Assert.Multiple(() =>
        {
            Assert.That(results.Select(result => result.Label), Is.EqualTo(new[] { "a/crm/customers", "a/crm/orders" }));
            Assert.That(results[0].Detail, Is.EqualTo("App tree of app crm"));
        });
    }

    [Test]
    public async Task A_literal_address_completes_trees_and_with_a_prefix_completes_keys()
    {
        Client.WithTree("orders", keys: 3, prefix: "order/").WithTree("other");

        var trees = await Complete("/data/or", AddressQueryMode.Address);
        var keys = await Complete("/data/orders?prefix=order%2F0", AddressQueryMode.Address);

        Assert.Multiple(() =>
        {
            Assert.That(trees.Select(result => result.Label), Is.EqualTo(new[] { "orders" }));
            Assert.That(keys.Select(result => result.Label), Is.EqualTo(new[] { "order/0000", "order/0001", "order/0002" }));
            Assert.That(keys[0].Target, Is.EqualTo(ExplorerAddress.ForTree("data", "orders").WithQuery("key", "order/0000")));
        });
    }

    [Test]
    public async Task In_the_keys_scope_a_search_offers_key_prefixes_before_trees()
    {
        Client.WithTree("orders", keys: 2, prefix: "k").WithTree("kiosks");

        var results = await Complete("k", AddressQueryMode.Search, ExplorerAddress.ForTree("data", "orders"));

        Assert.Multiple(() =>
        {
            Assert.That(results.Select(result => result.Label), Is.EqualTo(new[] { "k0000", "k0001", "kiosks" }));
            Assert.That(results[0].Detail, Is.EqualTo("key in orders"));
        });
    }

    [Test]
    public async Task Outside_the_keys_scope_and_for_other_areas_no_keys_are_read()
    {
        Client.WithTree("orders", keys: 2, prefix: "k");

        var history = await Complete("k", AddressQueryMode.Search, ExplorerAddress.ForTree("data", "orders").WithQuery("tab", "history"));
        var elsewhere = await Complete("/apps/crm", AddressQueryMode.Address);
        var empty = await Complete(string.Empty, AddressQueryMode.Search);

        Assert.Multiple(() =>
        {
            Assert.That(history, Is.Empty);
            Assert.That(elsewhere, Is.Empty);
            Assert.That(empty, Is.Empty);
            Assert.That(Client.Calls, Has.None.StartsWith("ScanEntriesAsync"));
        });
    }

    [Test]
    public async Task Tenant_rooted_literal_addresses_complete_within_the_tenant()
    {
        UseDataTenancy("acme");
        Client.WithTree("t/acme/orders").WithTree("t/globex/orders-secret");

        var results = await Complete("/t/acme/data/ord", AddressQueryMode.Address);

        Assert.Multiple(() =>
        {
            Assert.That(results.Select(result => result.Label), Is.EqualTo(new[] { "orders" }));
            Assert.That(results[0].Target.Tenant, Is.EqualTo("acme"));
        });
    }

    private async Task<IReadOnlyList<AddressCompletion>> Complete(string text, AddressQueryMode mode, ExplorerAddress? current = null) =>
        await Area.Completions!.CompleteAsync(new AddressQuery(text, mode, current ?? ExplorerAddress.Home), CancellationToken.None);
}
