using System.Text;

namespace Orleans.Lattice.Api.Data.Tests;

/// <summary>
/// Proves the typed-CRDT facade verbs on <see cref="ILatticeDataApi"/> drive each
/// primitive's accessor end-to-end against a real cluster: a write merges and the
/// matching read reflects it, with the byte-native primitives round-tripping raw
/// bytes and the OR-Map surface storing per-field concurrent values.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class CrdtApiDataTests
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
    public async Task pn_counter_sums_increments_and_decrements()
    {
        const string tree = "crdt-counter";
        await _fixture.RegisterTreeAsync(tree);

        await _fixture.Api.CounterIncrementAsync(tree, "c", "r1", 5);
        await _fixture.Api.CounterIncrementAsync(tree, "c", "r2", 3);
        await _fixture.Api.CounterDecrementAsync(tree, "c", "r1", 2);

        Assert.That(await _fixture.Api.CounterGetAsync(tree, "c"), Is.EqualTo(6));
    }

    [Test]
    public async Task or_set_keeps_added_elements_and_drops_removed()
    {
        const string tree = "crdt-orset";
        await _fixture.RegisterTreeAsync(tree);

        await _fixture.Api.SetAddAsync(tree, "s", [1], "r1");
        await _fixture.Api.SetAddAsync(tree, "s", [2], "r1");
        await _fixture.Api.SetRemoveAsync(tree, "s", [1]);

        var members = await _fixture.Api.SetGetAsync(tree, "s");
        Assert.That(members, Has.Count.EqualTo(1));
        Assert.That(members[0], Is.EqualTo(new byte[] { 2 }));
    }

    [Test]
    public async Task or_flag_converges_enable_wins()
    {
        const string tree = "crdt-orflag";
        await _fixture.RegisterTreeAsync(tree);

        await _fixture.Api.OrFlagEnableAsync(tree, "f", "r1");
        Assert.That(await _fixture.Api.OrFlagGetAsync(tree, "f"), Is.True);

        await _fixture.Api.OrFlagDisableAsync(tree, "f");
        Assert.That(await _fixture.Api.OrFlagGetAsync(tree, "f"), Is.False);
    }

    [Test]
    public async Task rw_flag_converges_disable_wins()
    {
        const string tree = "crdt-rwflag";
        await _fixture.RegisterTreeAsync(tree);

        await _fixture.Api.RwFlagEnableAsync(tree, "f", "r1");
        Assert.That(await _fixture.Api.RwFlagGetAsync(tree, "f"), Is.True);

        await _fixture.Api.RwFlagDisableAsync(tree, "f", "r1");
        Assert.That(await _fixture.Api.RwFlagGetAsync(tree, "f"), Is.False);
    }

    [Test]
    public async Task version_vector_records_per_replica_clocks()
    {
        const string tree = "crdt-vv";
        await _fixture.RegisterTreeAsync(tree);

        await _fixture.Api.VersionVectorTickAsync(tree, "v", "r1");
        await _fixture.Api.VersionVectorTickAsync(tree, "v", "r2");

        var vector = await _fixture.Api.VersionVectorGetAsync(tree, "v");
        Assert.That(vector.Keys, Is.EquivalentTo(new[] { "r1", "r2" }));
        Assert.That(vector["r1"], Does.Contain(":"));
    }

    [Test]
    public async Task mv_register_round_trips_the_value_bytes()
    {
        const string tree = "crdt-mvreg";
        await _fixture.RegisterTreeAsync(tree);

        await _fixture.Api.RegisterSetAsync(tree, "m", "r1", Encoding.UTF8.GetBytes("hello"));

        var values = await _fixture.Api.RegisterGetAsync(tree, "m");
        Assert.That(values, Has.Count.EqualTo(1));
        Assert.That(Encoding.UTF8.GetString(values[0]), Is.EqualTo("hello"));
    }

    [Test]
    public async Task sequence_preserves_insertion_order()
    {
        const string tree = "crdt-seq";
        await _fixture.RegisterTreeAsync(tree);

        await _fixture.Api.SequenceInsertAtAsync(tree, "q", 0, "r1", [1]);
        await _fixture.Api.SequenceInsertAtAsync(tree, "q", 1, "r1", [2]);
        await _fixture.Api.SequenceInsertAtAsync(tree, "q", 1, "r1", [3]);

        var list = await _fixture.Api.SequenceGetAsync(tree, "q");
        Assert.That(list.Select(b => b[0]), Is.EqualTo(new byte[] { 1, 3, 2 }));

        await _fixture.Api.SequenceRemoveAtAsync(tree, "q", 0);
        var afterRemove = await _fixture.Api.SequenceGetAsync(tree, "q");
        Assert.That(afterRemove.Select(b => b[0]), Is.EqualTo(new byte[] { 3, 2 }));
    }

    [Test]
    public async Task or_map_stores_and_removes_fields()
    {
        var tree = CrdtApiDataClusterFixture.MapTreeId;
        await _fixture.RegisterTreeAsync(tree);

        await _fixture.Api.MapSetAsync(tree, "doc", "title", "r1", Encoding.UTF8.GetBytes("v1"));
        await _fixture.Api.MapSetAsync(tree, "doc", "body", "r1", Encoding.UTF8.GetBytes("text"));

        var map = await _fixture.Api.MapGetAsync(tree, "doc");
        Assert.That(map.Keys, Is.EquivalentTo(new[] { "title", "body" }));
        Assert.That(Encoding.UTF8.GetString(map["title"][0]), Is.EqualTo("v1"));

        await _fixture.Api.MapRemoveAsync(tree, "doc", "body");
        var afterRemove = await _fixture.Api.MapGetAsync(tree, "doc");
        Assert.That(afterRemove.Keys, Is.EquivalentTo(new[] { "title" }));
    }
}
