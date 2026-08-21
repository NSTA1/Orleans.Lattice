using NSubstitute;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeSchemaPolicyStore"/>: the round-trip through
/// the dogfooded <c>sys-schema-policy</c> tree (set / get / clear / list),
/// up-front policy compilation on set, and the argument / reserved-tree guards.
/// Exercised against an in-memory <see cref="ILattice"/> so no cluster is required.
/// </summary>
[TestFixture]
public sealed class LatticeSchemaPolicyStoreTests
{
    private static LatticeSchemaPolicyStore CreateStore()
    {
        var backing = new SortedDictionary<string, byte[]>(StringComparer.Ordinal);
        var lattice = InMemoryLatticeFake.Create(backing);
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(SchemaConstants.PolicyTree).Returns(lattice);
        return new LatticeSchemaPolicyStore(grainFactory);
    }

    private static LatticeSchemaPolicy JsonPolicy() => new(new[] { LatticeSchemaRule.Json() });

    [Test]
    public async Task SetPolicyAsync_then_GetPolicyAsync_round_trips_the_policy()
    {
        var store = CreateStore();

        await store.SetPolicyAsync("orders", JsonPolicy());
        var read = await store.GetPolicyAsync("orders");

        Assert.That(read, Is.Not.Null);
        Assert.That(read!.Rules, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task GetPolicyAsync_missing_tree_returns_null()
    {
        var store = CreateStore();

        Assert.That(await store.GetPolicyAsync("orders"), Is.Null);
    }

    [Test]
    public void SetPolicyAsync_null_policy_throws()
    {
        var store = CreateStore();

        Assert.That(() => store.SetPolicyAsync("orders", null!), Throws.ArgumentNullException);
    }

    [Test]
    public void SetPolicyAsync_empty_tree_throws()
    {
        var store = CreateStore();

        Assert.That(() => store.SetPolicyAsync("", JsonPolicy()), Throws.ArgumentException);
    }

    [Test]
    public void SetPolicyAsync_reserved_tree_throws()
    {
        var store = CreateStore();

        Assert.That(
            () => store.SetPolicyAsync("sys-schema-policy", JsonPolicy()),
            Throws.ArgumentException);
    }

    [Test]
    public void GetPolicyAsync_empty_tree_throws()
    {
        var store = CreateStore();

        Assert.That(() => store.GetPolicyAsync(""), Throws.ArgumentException);
    }

    [Test]
    public async Task ClearPolicyAsync_removes_an_existing_policy()
    {
        var store = CreateStore();
        await store.SetPolicyAsync("orders", JsonPolicy());

        var cleared = await store.ClearPolicyAsync("orders");

        Assert.That(cleared, Is.True);
        Assert.That(await store.GetPolicyAsync("orders"), Is.Null);
    }

    [Test]
    public async Task ClearPolicyAsync_missing_policy_returns_false()
    {
        var store = CreateStore();

        Assert.That(await store.ClearPolicyAsync("orders"), Is.False);
    }

    [Test]
    public void ClearPolicyAsync_empty_tree_throws()
    {
        var store = CreateStore();

        Assert.That(() => store.ClearPolicyAsync(""), Throws.ArgumentException);
    }

    [Test]
    public async Task ListPoliciesAsync_yields_every_stored_policy()
    {
        var store = CreateStore();
        await store.SetPolicyAsync("orders", JsonPolicy());
        await store.SetPolicyAsync("users", JsonPolicy());

        var keys = new List<string>();
        await foreach (var pair in store.ListPoliciesAsync())
        {
            keys.Add(pair.Key);
        }

        Assert.That(keys, Is.EquivalentTo(new[] { "orders", "users" }));
    }

    [Test]
    public async Task ListPoliciesAsync_empty_tree_yields_nothing()
    {
        var store = CreateStore();

        var any = false;
        await foreach (var _ in store.ListPoliciesAsync())
        {
            any = true;
        }

        Assert.That(any, Is.False);
    }
}
