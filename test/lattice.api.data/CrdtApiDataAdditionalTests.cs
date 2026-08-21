using System.Text;

namespace Orleans.Lattice.Api.Data.Tests;

/// <summary>
/// Extends <see cref="CrdtApiDataTests"/> coverage to the typed-CRDT facade verbs
/// the sibling suite does not exercise: the grow-only counter and set, the
/// remove-wins set, and the max/min registers. Each verb resolves the same
/// cluster <see cref="ILattice"/> grain and drives its typed accessor end-to-end,
/// so a write merges and the matching read reflects it. Reuses the shared
/// <see cref="CrdtApiDataClusterFixture"/> without modifying it.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class CrdtApiDataAdditionalTests
{
    private CrdtApiDataClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new CrdtApiDataClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_fixture is not null)
        {
            await _fixture.DisposeAsync();
        }
    }

    [Test]
    public async Task GCounterIncrementAsync_multiple_replicas_sums_all_increments()
    {
        const string tree = "crdt-gcounter";
        await _fixture.RegisterTreeAsync(tree);

        await _fixture.Api.GCounterIncrementAsync(tree, "g", "r1", 4);
        await _fixture.Api.GCounterIncrementAsync(tree, "g", "r2", 6);

        Assert.That(await _fixture.Api.GCounterGetAsync(tree, "g"), Is.EqualTo(10));
    }

    [Test]
    public async Task GSetAddAsync_keeps_every_added_element()
    {
        const string tree = "crdt-gset";
        await _fixture.RegisterTreeAsync(tree);

        await _fixture.Api.GSetAddAsync(tree, "s", [1]);
        await _fixture.Api.GSetAddAsync(tree, "s", [2]);

        var members = await _fixture.Api.GSetGetAsync(tree, "s");
        Assert.That(members.Select(b => b[0]), Is.EquivalentTo(new byte[] { 1, 2 }));
    }

    [Test]
    public async Task RwSetAddAsync_keeps_added_elements_and_removes_wins()
    {
        const string tree = "crdt-rwset";
        await _fixture.RegisterTreeAsync(tree);

        await _fixture.Api.RwSetAddAsync(tree, "s", [1], "r1");
        await _fixture.Api.RwSetAddAsync(tree, "s", [2], "r1");
        await _fixture.Api.RwSetRemoveAsync(tree, "s", [1], "r1");

        var members = await _fixture.Api.RwSetGetAsync(tree, "s");
        Assert.That(members, Has.Count.EqualTo(1));
        Assert.That(members[0], Is.EqualTo(new byte[] { 2 }));
    }

    [Test]
    public async Task MaxRegisterSetAsync_round_trips_the_stored_value()
    {
        const string tree = "crdt-maxreg";
        await _fixture.RegisterTreeAsync(tree);

        await _fixture.Api.MaxRegisterSetAsync(tree, "m", Encoding.UTF8.GetBytes("alpha"));

        var value = await _fixture.Api.MaxRegisterGetAsync(tree, "m");
        Assert.That(value, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(value!), Is.EqualTo("alpha"));
    }

    [Test]
    public async Task MinRegisterSetAsync_round_trips_the_stored_value()
    {
        const string tree = "crdt-minreg";
        await _fixture.RegisterTreeAsync(tree);

        await _fixture.Api.MinRegisterSetAsync(tree, "m", Encoding.UTF8.GetBytes("omega"));

        var value = await _fixture.Api.MinRegisterGetAsync(tree, "m");
        Assert.That(value, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(value!), Is.EqualTo("omega"));
    }
}
