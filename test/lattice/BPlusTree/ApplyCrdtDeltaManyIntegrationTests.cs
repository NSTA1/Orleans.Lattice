using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end integration coverage for the batched typed-CRDT receiver
/// seam <see cref="IReplicationApplyGrain.ApplyCrdtDeltaManyAsync"/>: a run
/// of CRDT-mode deltas folds into the receiver's visible state in a single
/// grain call (the batch-path equivalent of N
/// <see cref="ILattice.ApplyCrdtDeltaAsync(string, LatticeMergeMode, byte[], System.Threading.CancellationToken)"/>
/// applies), preserving the source HLC and origin and converging
/// regardless of intra-batch ordering or re-delivery.
/// </summary>
[TestFixture]
[Category("Integration")]
public class ApplyCrdtDeltaManyIntegrationTests
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

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static byte[] OrSetAddDelta(string element, string replica, int counter) =>
        JsonLatticeSerializer<OrSetDelta>.Default.Serialize(new OrSetDelta
        {
            Adds = new[] { new OrSetDeltaDot { Element = Encoding.UTF8.GetBytes(element), ReplicaId = replica, Counter = counter } },
            Removes = Array.Empty<OrSetDeltaDot>(),
        });

    private static byte[] PnCounterIncDelta(string replica, long amount) =>
        JsonLatticeSerializer<PnCounterDelta>.Default.Serialize(new PnCounterDelta
        {
            Increments = new Dictionary<string, long>(StringComparer.Ordinal) { [replica] = amount },
            Decrements = new Dictionary<string, long>(StringComparer.Ordinal),
        });

    [Test]
    public async Task ApplyCrdtDeltaManyAsync_or_set_batch_makes_all_keys_visible()
    {
        const string tree = "acd-many-orset";
        await _fixture.CreateTreeAsync(tree);
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(tree);

        var items = new[]
        {
            new ApplyCrdtDeltaItem { Key = "a", Mode = LatticeMergeMode.OrSet, Delta = OrSetAddDelta("apple", "r1", 1), SourceHlc = Hlc(10), OriginClusterId = "site-x", SourceVectorClock = null },
            new ApplyCrdtDeltaItem { Key = "b", Mode = LatticeMergeMode.OrSet, Delta = OrSetAddDelta("banana", "r1", 1), SourceHlc = Hlc(20), OriginClusterId = "site-x", SourceVectorClock = null },
            new ApplyCrdtDeltaItem { Key = "c", Mode = LatticeMergeMode.OrSet, Delta = OrSetAddDelta("cherry", "r1", 1), SourceHlc = Hlc(30), OriginClusterId = "site-x", SourceVectorClock = null },
        };

        await apply.ApplyCrdtDeltaManyAsync(items);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(Contains(await lattice.GetAsync("a"), "apple"), Is.True);
            Assert.That(Contains(await lattice.GetAsync("b"), "banana"), Is.True);
            Assert.That(Contains(await lattice.GetAsync("c"), "cherry"), Is.True);
        });
    }

    [Test]
    public async Task ApplyCrdtDeltaManyAsync_same_key_deltas_fold_and_converge()
    {
        const string tree = "acd-many-fold";
        await _fixture.CreateTreeAsync(tree);
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(tree);

        var items = new[]
        {
            new ApplyCrdtDeltaItem { Key = "k", Mode = LatticeMergeMode.OrSet, Delta = OrSetAddDelta("one", "r1", 1), SourceHlc = Hlc(10), OriginClusterId = "site-x", SourceVectorClock = null },
            new ApplyCrdtDeltaItem { Key = "k", Mode = LatticeMergeMode.OrSet, Delta = OrSetAddDelta("two", "r2", 1), SourceHlc = Hlc(20), OriginClusterId = "site-y", SourceVectorClock = null },
            new ApplyCrdtDeltaItem { Key = "k", Mode = LatticeMergeMode.OrSet, Delta = OrSetAddDelta("three", "r3", 1), SourceHlc = Hlc(30), OriginClusterId = "site-z", SourceVectorClock = null },
        };

        await apply.ApplyCrdtDeltaManyAsync(items);

        var observed = JsonLatticeSerializer<OrSet>.Default.Deserialize((await lattice.GetAsync("k"))!);
        Assert.Multiple(() =>
        {
            Assert.That(observed.Contains(Encoding.UTF8.GetBytes("one")), Is.True);
            Assert.That(observed.Contains(Encoding.UTF8.GetBytes("two")), Is.True);
            Assert.That(observed.Contains(Encoding.UTF8.GetBytes("three")), Is.True);
        });
    }

    [Test]
    public async Task ApplyCrdtDeltaManyAsync_redelivered_batch_is_idempotent()
    {
        const string tree = "acd-many-idem";
        await _fixture.CreateTreeAsync(tree);
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(tree);

        var items = new[]
        {
            new ApplyCrdtDeltaItem { Key = "p", Mode = LatticeMergeMode.PnCounter, Delta = PnCounterIncDelta("r1", 4), SourceHlc = Hlc(10), OriginClusterId = "site-x", SourceVectorClock = null },
            new ApplyCrdtDeltaItem { Key = "p", Mode = LatticeMergeMode.PnCounter, Delta = PnCounterIncDelta("r2", 6), SourceHlc = Hlc(20), OriginClusterId = "site-y", SourceVectorClock = null },
        };

        await apply.ApplyCrdtDeltaManyAsync(items);
        await apply.ApplyCrdtDeltaManyAsync(items); // re-delivery folds idempotently

        var counter = JsonLatticeSerializer<PnCounter>.Default.Deserialize((await lattice.GetAsync("p"))!);
        Assert.That(counter.Value, Is.EqualTo(10));
    }

    [Test]
    public async Task ApplyCrdtDeltaManyAsync_preserves_source_hlc_on_visible_version()
    {
        const string tree = "acd-many-hlc";
        await _fixture.CreateTreeAsync(tree);
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var sourceHlc = Hlc(99_000, 7);

        await apply.ApplyCrdtDeltaManyAsync(new[]
        {
            new ApplyCrdtDeltaItem { Key = "k", Mode = LatticeMergeMode.OrSet, Delta = OrSetAddDelta("x", "r1", 1), SourceHlc = sourceHlc, OriginClusterId = "site-x", SourceVectorClock = null },
        });

        var versioned = await lattice.GetWithVersionAsync("k");
        Assert.That(versioned.Version, Is.EqualTo(sourceHlc));
    }

    [Test]
    public async Task ApplyCrdtDeltaManyAsync_with_empty_list_is_noop()
    {
        const string tree = "acd-many-empty";
        await _fixture.CreateTreeAsync(tree);
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);

        Assert.DoesNotThrowAsync(async () => await apply.ApplyCrdtDeltaManyAsync(Array.Empty<ApplyCrdtDeltaItem>()));
    }

    [Test]
    public async Task ApplyCrdtDeltaManyAsync_rejects_lww_register_mode_item()
    {
        const string tree = "acd-many-lww";
        await _fixture.CreateTreeAsync(tree);
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);

        Assert.ThrowsAsync<ArgumentException>(async () => await apply.ApplyCrdtDeltaManyAsync(new[]
        {
            new ApplyCrdtDeltaItem { Key = "k", Mode = LatticeMergeMode.LwwRegister, Delta = new byte[] { 1 }, SourceHlc = Hlc(10), OriginClusterId = "site-x", SourceVectorClock = null },
        }));
    }

    private static bool Contains(byte[]? row, string element)
    {
        if (row is null)
        {
            return false;
        }

        var set = JsonLatticeSerializer<OrSet>.Default.Deserialize(row);
        return set.Contains(Encoding.UTF8.GetBytes(element));
    }
}
