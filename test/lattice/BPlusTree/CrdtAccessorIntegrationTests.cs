using System.Text;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

[TestFixture]
public class CrdtAccessorIntegrationTests
{
    private FourShardClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FourShardClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private async Task<ILattice> CreateTreeAsync()
    {
        var treeId = $"crdt-{Guid.NewGuid():N}";
        return await _fixture.CreateTreeAsync(treeId);
    }

    // ── OrSet ──────────────────────────────────────────────────

    [Test]
    public async Task OrSet_GetAsync_returns_empty_for_missing_key()
    {
        var tree = await CreateTreeAsync();
        var set = await tree.OrSet("missing").GetAsync();
        Assert.That(set.IsEmpty, Is.True);
    }

    [Test]
    public async Task OrSet_AddAsync_makes_element_a_member()
    {
        var tree = await CreateTreeAsync();
        var accessor = tree.OrSet("k");
        await accessor.AddAsync(Bytes("apple"), "r1");

        Assert.That(await accessor.ContainsAsync(Bytes("apple")), Is.True);
        var set = await accessor.GetAsync();
        Assert.That(set.Count, Is.EqualTo(1));
    }

    [Test]
    public async Task OrSet_AddAsync_multiple_elements_persists_all()
    {
        var tree = await CreateTreeAsync();
        var accessor = tree.OrSet("k");
        await accessor.AddAsync(Bytes("a"), "r1");
        await accessor.AddAsync(Bytes("b"), "r1");
        await accessor.AddAsync(Bytes("c"), "r1");

        var set = await accessor.GetAsync();
        Assert.That(set.Count, Is.EqualTo(3));
        Assert.That(set.Contains(Bytes("a")), Is.True);
        Assert.That(set.Contains(Bytes("b")), Is.True);
        Assert.That(set.Contains(Bytes("c")), Is.True);
    }

    [Test]
    public async Task OrSet_RemoveAsync_drops_element()
    {
        var tree = await CreateTreeAsync();
        var accessor = tree.OrSet("k");
        await accessor.AddAsync(Bytes("apple"), "r1");
        await accessor.RemoveAsync(Bytes("apple"));

        Assert.That(await accessor.ContainsAsync(Bytes("apple")), Is.False);
    }

    [Test]
    public async Task OrSet_AddAsync_assigns_monotonically_increasing_counters()
    {
        var tree = await CreateTreeAsync();
        var accessor = tree.OrSet("k");
        await accessor.AddAsync(Bytes("a"), "r1");
        await accessor.RemoveAsync(Bytes("a"));
        await accessor.AddAsync(Bytes("a"), "r1");

        Assert.That(await accessor.ContainsAsync(Bytes("a")), Is.True);
    }

    [Test]
    public async Task OrSet_MergeAsync_unions_remote_state()
    {
        var tree = await CreateTreeAsync();
        var accessor = tree.OrSet("k");
        await accessor.AddAsync(Bytes("a"), "r1");

        var remote = new OrSet();
        remote.Add(Bytes("b"), "r2", 1);
        await accessor.MergeAsync(remote);

        var set = await accessor.GetAsync();
        Assert.That(set.Contains(Bytes("a")), Is.True);
        Assert.That(set.Contains(Bytes("b")), Is.True);
    }

    [Test]
    public async Task OrSet_AddAsync_throws_on_null_element()
    {
        var tree = await CreateTreeAsync();
        Assert.That(async () => await tree.OrSet("k").AddAsync(null!, "r1"), Throws.ArgumentNullException);
    }

    [Test]
    public async Task OrSet_AddAsync_throws_on_empty_replica_id()
    {
        var tree = await CreateTreeAsync();
        Assert.That(async () => await tree.OrSet("k").AddAsync(Bytes("a"), ""), Throws.ArgumentException);
    }

    [Test]
    public async Task OrSet_RemoveAsync_throws_on_null_element()
    {
        var tree = await CreateTreeAsync();
        Assert.That(async () => await tree.OrSet("k").RemoveAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task OrSet_MergeAsync_throws_on_null_other()
    {
        var tree = await CreateTreeAsync();
        Assert.That(async () => await tree.OrSet("k").MergeAsync(null!), Throws.ArgumentNullException);
    }

    // ── PnCounter ──────────────────────────────────────────────

    [Test]
    public async Task PnCounter_ValueAsync_returns_zero_for_missing_key()
    {
        var tree = await CreateTreeAsync();
        Assert.That(await tree.PnCounter("missing").ValueAsync(), Is.EqualTo(0));
    }

    [Test]
    public async Task PnCounter_IncrementAsync_advances_value()
    {
        var tree = await CreateTreeAsync();
        var c = tree.PnCounter("k");
        await c.IncrementAsync("r1", 3);
        await c.IncrementAsync("r1", 4);
        Assert.That(await c.ValueAsync(), Is.EqualTo(7));
    }

    [Test]
    public async Task PnCounter_DecrementAsync_advances_value_negatively()
    {
        var tree = await CreateTreeAsync();
        var c = tree.PnCounter("k");
        await c.IncrementAsync("r1", 5);
        await c.DecrementAsync("r1", 2);
        Assert.That(await c.ValueAsync(), Is.EqualTo(3));
    }

    [Test]
    public async Task PnCounter_default_amount_is_one()
    {
        var tree = await CreateTreeAsync();
        var c = tree.PnCounter("k");
        await c.IncrementAsync("r1");
        await c.IncrementAsync("r1");
        await c.DecrementAsync("r1");
        Assert.That(await c.ValueAsync(), Is.EqualTo(1));
    }

    [Test]
    public async Task PnCounter_MergeAsync_takes_pointwise_max()
    {
        var tree = await CreateTreeAsync();
        var c = tree.PnCounter("k");
        await c.IncrementAsync("r1", 5);

        var remote = new PnCounter();
        remote.Increment("r1", 3); // lower than local — must not regress
        remote.Increment("r2", 4);
        await c.MergeAsync(remote);

        var state = await c.GetAsync();
        Assert.That(state.Increments["r1"], Is.EqualTo(5));
        Assert.That(state.Increments["r2"], Is.EqualTo(4));
        Assert.That(state.Value, Is.EqualTo(9));
    }

    [Test]
    public async Task PnCounter_IncrementAsync_throws_on_negative_amount()
    {
        var tree = await CreateTreeAsync();
        Assert.That(async () => await tree.PnCounter("k").IncrementAsync("r1", -1),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task PnCounter_DecrementAsync_throws_on_negative_amount()
    {
        var tree = await CreateTreeAsync();
        Assert.That(async () => await tree.PnCounter("k").DecrementAsync("r1", -1),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task PnCounter_IncrementAsync_throws_on_empty_replica_id()
    {
        var tree = await CreateTreeAsync();
        Assert.That(async () => await tree.PnCounter("k").IncrementAsync("", 1), Throws.ArgumentException);
    }

    [Test]
    public async Task PnCounter_MergeAsync_throws_on_null_other()
    {
        var tree = await CreateTreeAsync();
        Assert.That(async () => await tree.PnCounter("k").MergeAsync(null!), Throws.ArgumentNullException);
    }

    // ── VersionVector ──────────────────────────────────────────

    [Test]
    public async Task VersionVector_GetAsync_returns_empty_for_missing_key()
    {
        var tree = await CreateTreeAsync();
        var vv = await tree.VersionVector("missing").GetAsync();
        Assert.That(vv.Entries, Is.Empty);
    }

    [Test]
    public async Task VersionVector_TickAsync_advances_replica_clock()
    {
        var tree = await CreateTreeAsync();
        var vv = tree.VersionVector("k");
        await vv.TickAsync("r1");
        await vv.TickAsync("r1");

        var state = await vv.GetAsync();
        Assert.That(state.GetClock("r1"), Is.GreaterThan(HybridLogicalClock.Zero));
    }

    [Test]
    public async Task VersionVector_MergeAsync_unions_remote_entries()
    {
        var tree = await CreateTreeAsync();
        var vv = tree.VersionVector("k");
        await vv.TickAsync("r1");

        var remote = new VersionVector();
        remote.Tick("r2");
        await vv.MergeAsync(remote);

        var state = await vv.GetAsync();
        Assert.That(state.Entries.ContainsKey("r1"), Is.True);
        Assert.That(state.Entries.ContainsKey("r2"), Is.True);
    }

    [Test]
    public async Task VersionVector_TickAsync_throws_on_empty_replica_id()
    {
        var tree = await CreateTreeAsync();
        Assert.That(async () => await tree.VersionVector("k").TickAsync(""), Throws.ArgumentException);
    }

    [Test]
    public async Task VersionVector_MergeAsync_throws_on_null_other()
    {
        var tree = await CreateTreeAsync();
        Assert.That(async () => await tree.VersionVector("k").MergeAsync(null!), Throws.ArgumentNullException);
    }
}
