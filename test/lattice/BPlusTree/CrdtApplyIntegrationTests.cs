using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end integration coverage for the new
/// <see cref="ILattice.ApplyCrdtDeltaAsync(string, LatticeMergeMode, byte[], System.Threading.CancellationToken)"/>
/// surface. Exercises every closed-shape CRDT mode plus the
/// configuration-fault path for unregistered OR-Map shapes, and
/// verifies the post-merge byte[] row is observable through the
/// legacy <c>GetAsync</c> read path.
/// </summary>
[TestFixture]
[Category("Integration")]
public class CrdtApplyIntegrationTests
{
    private FourShardClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FourShardClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private Task<ILattice> CreateTreeAsync() =>
        _fixture.CreateTreeAsync($"crdtapply-{Guid.NewGuid():N}");

    // ── OrSet ──────────────────────────────────────────────────

    [Test]
    public async Task ApplyCrdtDeltaAsync_OrSet_makes_element_visible_through_GetAsync()
    {
        var tree = await CreateTreeAsync();
        var delta = new OrSetDelta
        {
            Adds = new[]
            {
                new OrSetDeltaDot { Element = Encoding.UTF8.GetBytes("apple"), ReplicaId = "r1", Counter = 1 },
            },
            Removes = Array.Empty<OrSetDeltaDot>(),
        };
        var deltaBytes = JsonLatticeSerializer<OrSetDelta>.Default.Serialize(delta);

        var version = await tree.ApplyCrdtDeltaAsync("k", LatticeMergeMode.OrSet, deltaBytes);

        Assert.That(version, Is.Not.EqualTo(HybridLogicalClock.Zero));
        var roundTrip = await tree.GetAsync("k");
        Assert.That(roundTrip, Is.Not.Null);
        var observed = JsonLatticeSerializer<OrSet>.Default.Deserialize(roundTrip!);
        Assert.That(observed.Contains(Encoding.UTF8.GetBytes("apple")), Is.True);
    }

    [Test]
    public async Task ApplyCrdtDeltaAsync_OrSet_sequential_applies_converge()
    {
        var tree = await CreateTreeAsync();

        var d1 = new OrSetDelta
        {
            Adds = new[] { new OrSetDeltaDot { Element = Encoding.UTF8.GetBytes("a"), ReplicaId = "r1", Counter = 1 } },
            Removes = Array.Empty<OrSetDeltaDot>(),
        };
        var v1 = await tree.ApplyCrdtDeltaAsync("k", LatticeMergeMode.OrSet, JsonLatticeSerializer<OrSetDelta>.Default.Serialize(d1));

        var d2 = new OrSetDelta
        {
            Adds = new[] { new OrSetDeltaDot { Element = Encoding.UTF8.GetBytes("b"), ReplicaId = "r2", Counter = 1 } },
            Removes = Array.Empty<OrSetDeltaDot>(),
        };
        var v2 = await tree.ApplyCrdtDeltaAsync("k", LatticeMergeMode.OrSet, JsonLatticeSerializer<OrSetDelta>.Default.Serialize(d2));

        Assert.That(v2, Is.Not.EqualTo(v1));
        var bytes = await tree.GetAsync("k");
        var observed = JsonLatticeSerializer<OrSet>.Default.Deserialize(bytes!);
        Assert.That(observed.Contains(Encoding.UTF8.GetBytes("a")), Is.True, "first delta must persist after second delta is applied");
        Assert.That(observed.Contains(Encoding.UTF8.GetBytes("b")), Is.True, "second delta must persist after fold into post-merge state");
    }

    // ── PnCounter ──────────────────────────────────────────────

    [Test]
    public async Task ApplyCrdtDeltaAsync_PnCounter_increments_accumulate()
    {
        var tree = await CreateTreeAsync();

        var d1 = new PnCounterDelta
        {
            Increments = new Dictionary<string, long>(StringComparer.Ordinal) { ["r1"] = 3 },
            Decrements = new Dictionary<string, long>(0, StringComparer.Ordinal),
        };
        await tree.ApplyCrdtDeltaAsync("c", LatticeMergeMode.PnCounter, JsonLatticeSerializer<PnCounterDelta>.Default.Serialize(d1));

        var d2 = new PnCounterDelta
        {
            Increments = new Dictionary<string, long>(StringComparer.Ordinal) { ["r2"] = 5 },
            Decrements = new Dictionary<string, long>(0, StringComparer.Ordinal),
        };
        await tree.ApplyCrdtDeltaAsync("c", LatticeMergeMode.PnCounter, JsonLatticeSerializer<PnCounterDelta>.Default.Serialize(d2));

        var bytes = await tree.GetAsync("c");
        var observed = JsonLatticeSerializer<PnCounter>.Default.Deserialize(bytes!);
        Assert.That(observed.Value, Is.EqualTo(8));
    }

    // ── MvRegister ─────────────────────────────────────────────

    [Test]
    public async Task ApplyCrdtDeltaAsync_MvRegister_records_dot_tagged_value()
    {
        var tree = await CreateTreeAsync();
        var delta = new MvRegisterDelta
        {
            Entries = new[]
            {
                new MvRegisterEntry { ReplicaId = "r1", Counter = 1, Value = Encoding.UTF8.GetBytes("v1") },
            },
            Context = new Dictionary<string, long>(StringComparer.Ordinal) { ["r1"] = 1 },
        };

        var version = await tree.ApplyCrdtDeltaAsync("m", LatticeMergeMode.MvRegister, JsonLatticeSerializer<MvRegisterDelta>.Default.Serialize(delta));

        Assert.That(version, Is.Not.EqualTo(HybridLogicalClock.Zero));
        var raw = await tree.GetAsync("m");
        Assert.That(raw, Is.Not.Null);
        var observed = JsonLatticeSerializer<MvRegister>.Default.Deserialize(raw!);
        Assert.That(observed.Entries, Has.Count.EqualTo(1));
        Assert.That(observed.Entries[0].Value, Is.EqualTo(Encoding.UTF8.GetBytes("v1")));
    }

    // ── VersionVector ──────────────────────────────────────────

    [Test]
    public async Task ApplyCrdtDeltaAsync_VersionVector_advances_frontier()
    {
        var tree = await CreateTreeAsync();
        var seed = new VersionVector();
        seed.Tick("r1");

        var delta = new VersionVectorDelta
        {
            Entries = new Dictionary<string, HybridLogicalClock>(seed.Entries, StringComparer.Ordinal),
        };

        await tree.ApplyCrdtDeltaAsync("v", LatticeMergeMode.VersionVector, JsonLatticeSerializer<VersionVectorDelta>.Default.Serialize(delta));

        var raw = await tree.GetAsync("v");
        Assert.That(raw, Is.Not.Null);
        var observed = JsonLatticeSerializer<VersionVector>.Default.Deserialize(raw!);
        Assert.That(observed.IsBottom, Is.False);
        Assert.That(observed.Entries, Contains.Key("r1"));
    }

    // ── Reject paths ───────────────────────────────────────────

    [Test]
    public void ApplyCrdtDeltaAsync_LwwRegister_is_rejected()
    {
        var tree = _fixture.Cluster.Client.GetGrain<ILattice>($"crdtapply-lww-{Guid.NewGuid():N}");
        var ex = Assert.ThrowsAsync<ArgumentException>(async () =>
            await tree.ApplyCrdtDeltaAsync("k", LatticeMergeMode.LwwRegister, new byte[] { 0x00 }));
        Assert.That(ex!.Message, Does.Contain("LwwRegister"));
    }

    [Test]
    public async Task ApplyCrdtDeltaAsync_OrMap_without_registered_shape_throws()
    {
        var tree = await CreateTreeAsync();
        // Tree was created without AddOrMapShape<TKey, TValue> - the leaf
        // must surface a clear configuration error rather than silently
        // applying the wrong typed dispatch.
        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await tree.ApplyCrdtDeltaAsync("k", LatticeMergeMode.OrMap, new byte[] { 0x7b, 0x7d })); // "{}"
        Assert.That(ex!.Message, Does.Contain("CrdtShape"));
        Assert.That(ex!.Message, Does.Contain("OrMap"));
    }

    [Test]
    public void ApplyCrdtDeltaAsync_null_key_throws()
    {
        var tree = _fixture.Cluster.Client.GetGrain<ILattice>($"crdtapply-null-key-{Guid.NewGuid():N}");
        Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await tree.ApplyCrdtDeltaAsync(null!, LatticeMergeMode.OrSet, new byte[] { 0x00 }));
    }

    [Test]
    public void ApplyCrdtDeltaAsync_null_delta_throws()
    {
        var tree = _fixture.Cluster.Client.GetGrain<ILattice>($"crdtapply-null-delta-{Guid.NewGuid():N}");
        Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await tree.ApplyCrdtDeltaAsync("k", LatticeMergeMode.OrSet, null!));
    }
}
