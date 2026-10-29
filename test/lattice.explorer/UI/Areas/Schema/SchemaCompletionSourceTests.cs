using NSubstitute;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class SchemaCompletionSourceTests : SchemaTestContext
{
    [Test]
    public void Constructor_null_directory_is_rejected()
    {
        Assert.That(() => new SchemaCompletionSource(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void CompleteAsync_null_query_is_rejected()
    {
        var source = new SchemaCompletionSource(Directory);

        Assert.That(async () => await source.CompleteAsync(null!, CancellationToken.None), Throws.ArgumentNullException);
    }

    [Test]
    public async Task CompleteAsync_policy_only_tree_has_a_policy_detail_and_keeps_the_tenant()
    {
        UseTrees("orders");
        Schema.Policies["orders"] = SchemaTestData.Policy();
        var query = new AddressQuery("ORD", AddressQueryMode.Search, ExplorerAddress.Parse("/t/acme/schema"));

        var results = await new SchemaCompletionSource(Directory).CompleteAsync(query, CancellationToken.None);

        Assert.That(results, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(results[0].Label, Is.EqualTo("orders"));
            Assert.That(results[0].Detail, Is.EqualTo("Schema policy"));
            Assert.That(results[0].Target.Format(), Is.EqualTo("/t/acme/schema/orders"));
        });
    }

    [Test]
    public async Task CompleteAsync_limit_is_applied_to_matches_not_inspected_rows()
    {
        var matches = Enumerable.Range(0, AddressQuery.MaximumResults + 3).Select(i => $"match-{i:D2}").ToArray();
        UseTrees(["aaa-nonmatch", .. matches]);
        Schema.Policies["aaa-nonmatch"] = SchemaTestData.Policy();
        foreach (var tree in matches)
        {
            Schema.Policies[tree] = SchemaTestData.Policy();
        }

        var results = await new SchemaCompletionSource(Directory).CompleteAsync(
            new AddressQuery("match-", AddressQueryMode.Search, ExplorerAddress.Home), CancellationToken.None);

        Assert.That(results.Select(result => result.Label), Is.EqualTo(matches.Take(AddressQuery.MaximumResults)));
    }

    [Test]
    public async Task CompleteAsync_repeated_typing_reuses_the_directory_listing()
    {
        UseEstate();
        var source = new SchemaCompletionSource(Directory);
        await source.CompleteAsync(new AddressQuery("ord", AddressQueryMode.Search, ExplorerAddress.Home), CancellationToken.None);
        var reads = Schema.CountOf("GetPolicy");

        var results = await source.CompleteAsync(
            new AddressQuery("audit", AddressQueryMode.Search, ExplorerAddress.Home), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(results.Select(result => result.Label), Is.EqualTo(new[] { "audit" }));
            Assert.That(Schema.CountOf("GetPolicy"), Is.EqualTo(reads));
        });
        await Explorer.Connection.Received(1).ListTreesAsync(Arg.Any<CatalogRequest>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void CompleteAsync_caller_cancellation_is_propagated()
    {
        UseEstate();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var source = new SchemaCompletionSource(Directory);

        Assert.That(async () => await source.CompleteAsync(
            new AddressQuery("", AddressQueryMode.Search, ExplorerAddress.Home), cancellation.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }
}
