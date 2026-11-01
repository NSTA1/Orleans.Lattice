using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// A leaf row rebuilt from its WAL record by the projection stores an own
/// write with a null origin, exactly as a live own write stores it, and keeps
/// a peer's id (issue #5524). Before the fix the projection copied the record's
/// own-write stamp - the local cluster id on a replicated tree, an empty string
/// on a single-cluster host - so one row had two representations depending on
/// whether it had been rebuilt from the WAL.
/// </summary>
[TestFixture]
public sealed class BPlusLeafGrainProjectionOriginTests
{
    private const string TreeId = "tree-projection-origin";

    private sealed class FixedOrigin(string origin) : ILatticeOriginClusterIdResolver
    {
        public string Resolve(string treeId) => origin;
    }

    private static BPlusLeafGrain CreateLeaf(ILatticeOriginClusterIdResolver origins)
    {
        var services = new ServiceCollection().BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState> { State = { TreeId = TreeId } };

        var coordinator = Substitute.For<ILeafReplayCoordinatorGrain>();
        coordinator.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(0L));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(coordinator);

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions { WalPartitions = 1 },
            maxLeafKeys: 1_000_000,
            shardCount: 1,
            factory: grainFactory);

        return new BPlusLeafGrain(
            context,
            state,
            grainFactory,
            optionsResolver,
            TestMutationObservers.NoObservers(),
            origins);
    }

    private static LatticeMutation Set(string key, string? origin) => new()
    {
        TreeId = TreeId,
        Kind = MutationKind.Set,
        Key = key,
        Value = [1],
        Timestamp = new HybridLogicalClock { WallClockTicks = 100 },
        OriginClusterId = origin,
    };

    private static string? RowOrigin(BPlusLeafGrain leaf, string key)
    {
        Assert.That(leaf.CacheForTest.TryPeekRow(key, out var row, out _), Is.True, $"row '{key}' was not projected");
        return row.OriginClusterId;
    }

    [Test]
    public void A_record_stamped_with_the_local_cluster_id_projects_to_a_null_origin()
    {
        var leaf = CreateLeaf(new FixedOrigin("west"));

        ((ILeafProjection)leaf).Apply(Set("own", "west"));
        ((ILeafProjection)leaf).Apply(Set("peer", "east"));

        Assert.Multiple(() =>
        {
            Assert.That(RowOrigin(leaf, "own"), Is.Null);
            Assert.That(RowOrigin(leaf, "peer"), Is.EqualTo("east"));
        });
    }

    [Test]
    public void A_single_cluster_record_stamped_empty_projects_to_a_null_origin()
    {
        var leaf = CreateLeaf(TestOriginClusterIdResolver.Default());

        ((ILeafProjection)leaf).Apply(Set("own", string.Empty));

        Assert.That(RowOrigin(leaf, "own"), Is.Null);
    }

    private static LatticeMutation PreparedSet(Guid txId, string key, string? origin) => new()
    {
        TreeId = TreeId,
        Kind = MutationKind.Set,
        Key = key,
        Value = [7],
        Timestamp = new HybridLogicalClock { WallClockTicks = 100 },
        OriginClusterId = origin,
        TransactionId = txId,
        IsPrepared = true,
        AtomicBatchSize = 1,
        AtomicBatchIndex = 0,
    };

    private static LatticeMutation Commit(Guid txId) => new()
    {
        TreeId = TreeId,
        Kind = MutationKind.TxCommit,
        TransactionId = txId,
        Timestamp = new HybridLogicalClock { WallClockTicks = 101 },
    };

    [Test]
    public void A_replayed_own_saga_on_a_replicated_tree_commits_like_the_foreground_saga()
    {
        // Issue #5532. ApplyTxCommit preserves the prepared values' HLCs when they
        // carry an origin (the cross-cluster apply path) and re-stamps them with
        // the leaf clock otherwise (the foreground path). A foreground own prepare
        // stores no origin; its WAL record, on a replicated tree, carries the local
        // cluster id. A leaf replaying that record must still take the foreground
        // branch, or it rebuilds the committed row with a different timestamp from
        // the one it served.
        var txId = Guid.NewGuid();

        var replayed = CreateLeaf(new FixedOrigin("west"));
        ((ILeafProjection)replayed).Apply(PreparedSet(txId, "k", "west"));
        ((ILeafProjection)replayed).Apply(Commit(txId));

        var foreground = CreateLeaf(new FixedOrigin("west"));
        ((ILeafProjection)foreground).Apply(PreparedSet(txId, "k", null));
        ((ILeafProjection)foreground).Apply(Commit(txId));

        Assert.That(replayed.CacheForTest.TryPeekRow("k", out var replayedRow, out _), Is.True);
        Assert.That(foreground.CacheForTest.TryPeekRow("k", out var foregroundRow, out _), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(replayedRow.Timestamp, Is.EqualTo(foregroundRow.Timestamp), "the replayed commit took a different stamping branch");
            Assert.That(replayedRow.Timestamp, Is.Not.EqualTo(new HybridLogicalClock { WallClockTicks = 100 }), "an own saga must be re-stamped, not committed at its prepare's HLC");
            Assert.That(replayedRow.OriginClusterId, Is.Null);
        });
    }

    [Test]
    public void A_replayed_peer_saga_keeps_its_source_hlc()
    {
        // The cross-cluster branch is unchanged: a prepare stamped by a peer
        // commits at its source HLC.
        var txId = Guid.NewGuid();
        var leaf = CreateLeaf(new FixedOrigin("west"));
        ((ILeafProjection)leaf).Apply(PreparedSet(txId, "k", "east"));
        ((ILeafProjection)leaf).Apply(Commit(txId));

        Assert.That(leaf.CacheForTest.TryPeekRow("k", out var row, out _), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(row.Timestamp, Is.EqualTo(new HybridLogicalClock { WallClockTicks = 100 }));
            Assert.That(row.OriginClusterId, Is.EqualTo("east"));
        });
    }
}
