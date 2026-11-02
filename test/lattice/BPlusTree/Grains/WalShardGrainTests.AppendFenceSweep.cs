using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for the WAL append fence's orphaned-row sweep (#5634): a partition lists
/// every leaf it holds an entry for, delists it on retirement, and the sweep retires
/// the rows of a leaf that is gone - the rows a clear on a 10.2.0 silo leaves behind
/// - while leaving a live leaf and a recently listed one alone, and sweeping each tree
/// once per interval across the cluster.
/// </summary>
public partial class WalShardGrainTests
{
    private static readonly GrainType SweepLeafType = GrainType.Create("bplusleaf");

    private static async Task<IReadOnlyDictionary<Guid, long>> ListedAsync(IGrainStorage storage) =>
        await WalAppendEpochDirectory.ListAsync(storage, TreeId, ShardIndex, CancellationToken.None);

    private static WalAppendFenceSweeper Sweeper(IGrainStorage storage, WalShardGrain shard, DateTimeOffset now)
    {
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalShardGrain>($"{TreeId}/{ShardIndex}").Returns(shard);
        return new WalAppendFenceSweeper(
            factory,
            NullLogger<WalAppendFenceSweeper>.Instance,
            storage,
            optionsResolver: null,
            timeProvider: new VirtualTimeProvider(now))
        {
            LeafGrainTypeForTesting = SweepLeafType,
        };
    }

    /// <summary>
    /// Lists the leaf directly as well as through its claim (idempotent), so the sweep
    /// detectors do not depend on the claim's listing, which
    /// <see cref="A_claim_lists_the_leaf_and_its_retirement_delists_it"/> covers.
    /// </summary>
    private static Task ListExplicitlyAsync(IGrainStorage storage, Guid leaf) =>
        WalAppendEpochDirectory.AddAsync(storage, TreeId, ShardIndex, leaf, DateTime.UtcNow.Ticks);

    private static Task WriteLeafEpochRowAsync(IGrainStorage storage, Guid leaf, long epoch) =>
        WalAppendFenceRows.WriteAsync(
            storage,
            WalAppendFenceRows.LeafEpochGrainType,
            WalAppendFenceRows.LeafEpochKey(leaf),
            new GrainState<LeafAppendEpochState>(new LeafAppendEpochState { Epoch = epoch }));

    private static Task WriteLiveLeafRowAsync(IGrainStorage storage, Guid leaf) =>
        storage.WriteStateAsync(
            WalFloorHolderReader.LeafStateName,
            GrainId.Create(SweepLeafType, GrainIdKeyExtensions.CreateGuidKey(leaf)),
            new GrainState<LeafNodeState>(new LeafNodeState { TreeId = TreeId }));

    [Test]
    public async Task A_claim_lists_the_leaf_and_its_retirement_delists_it()
    {
        var storage = new EnumerableMemoryGrainStorage();
        var grain = await CreateFencedGrainAsync(storage);

        await grain.ClaimAppendEpochAsync(FenceLeaf, 1, CancellationToken.None);
        await grain.ClaimAppendEpochAsync(FenceLeaf, 2, CancellationToken.None);
        var afterClaims = await ListedAsync(storage);
        await grain.RetireAppendEpochAsync(FenceLeaf, 2, CancellationToken.None);
        var afterRetirement = await ListedAsync(storage);

        Assert.Multiple(() =>
        {
            Assert.That(afterClaims.Keys, Is.EqualTo(new[] { FenceLeaf }), "listed once, when its entry row was first written");
            Assert.That(afterRetirement, Is.Empty, "delisted once its entry is retired");
        });
    }

    [Test]
    public async Task A_retirement_delists_a_leaf_whose_entry_is_already_gone()
    {
        // A listing whose entry write never landed must still go, or every sweep would
        // find it again.
        var storage = new EnumerableMemoryGrainStorage();
        var grain = await CreateFencedGrainAsync(storage);
        await ListExplicitlyAsync(storage, FenceLeaf);

        await grain.RetireAppendEpochAsync(FenceLeaf, 0, CancellationToken.None);

        Assert.That(await ListedAsync(storage), Is.Empty);
    }

    [Test]
    public async Task The_sweep_retires_the_rows_a_10_2_0_clear_left_behind()
    {
        // A leaf claimed epoch 2 here while the gate was open, then was cleared on a
        // 10.2.0 silo: its leaf row is gone, but its entry, its listing and its epoch
        // row are all left.
        var storage = new EnumerableMemoryGrainStorage();
        var grain = await CreateFencedGrainAsync(storage);
        await WriteLeafEpochRowAsync(storage, FenceLeaf, 2);
        await grain.ClaimAppendEpochAsync(FenceLeaf, 2, CancellationToken.None);
        await ListExplicitlyAsync(storage, FenceLeaf);

        var retired = 0L;
        using var listener = MeterListening.StartForInstrument(LatticeMetrics.WalAppendFenceSweeps, l =>
            l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagOutcome && Equals(tag.Value, "retired"))
                    {
                        Interlocked.Add(ref retired, value);
                    }
                }
            }));

        var swept = await Sweeper(storage, grain, DateTimeOffset.UtcNow + TimeSpan.FromDays(2))
            .SweepTreeAsync(TreeId, partitions: 1, CancellationToken.None);
        var entry = await WalAppendFenceRows.ReadEntryAsync(storage, TreeId, ShardIndex, FenceLeaf);
        var epochRow = await WalAppendFenceRows.ReadLeafEpochAsync(storage, FenceLeaf);
        var listed = await ListedAsync(storage);
        var duplicate = await grain.ClaimAppendEpochAsync(FenceLeaf, 2, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(swept, Is.True);
            Assert.That(entry.RecordExists, Is.False, "the leaked entry is retired");
            Assert.That(epochRow.RecordExists, Is.False, "the leaked epoch row is deleted");
            Assert.That(listed, Is.Empty, "the leaf is delisted");
            Assert.That(grain.EpochFloorForTesting, Is.GreaterThanOrEqualTo(2), "the floor covers what was retired");
            Assert.That(duplicate.Accepted, Is.False, "a surviving duplicate is refused by the floor");
            Assert.That(Interlocked.Read(ref retired), Is.EqualTo(1));
        });
    }

    [Test]
    public async Task The_sweep_keeps_the_rows_of_a_live_leaf()
    {
        var storage = new EnumerableMemoryGrainStorage();
        var grain = await CreateFencedGrainAsync(storage);
        await WriteLeafEpochRowAsync(storage, FenceLeaf, 2);
        await WriteLiveLeafRowAsync(storage, FenceLeaf);
        await grain.ClaimAppendEpochAsync(FenceLeaf, 2, CancellationToken.None);
        await ListExplicitlyAsync(storage, FenceLeaf);

        var swept = await Sweeper(storage, grain, DateTimeOffset.UtcNow + TimeSpan.FromDays(2))
            .SweepTreeAsync(TreeId, partitions: 1, CancellationToken.None);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(swept, Is.True);
            Assert.That((await WalAppendFenceRows.ReadEntryAsync(storage, TreeId, ShardIndex, FenceLeaf)).RecordExists, Is.True);
            Assert.That((await WalAppendFenceRows.ReadLeafEpochAsync(storage, FenceLeaf)).RecordExists, Is.True);
            Assert.That((await ListedAsync(storage)).Keys, Does.Contain(FenceLeaf));
        });
    }

    [Test]
    public async Task The_sweep_leaves_a_recently_listed_leaf_alone()
    {
        // A leaf can claim before its own row is first persisted, so a listing younger
        // than the grace is not examined at all.
        var storage = new EnumerableMemoryGrainStorage();
        var grain = await CreateFencedGrainAsync(storage);
        await WriteLeafEpochRowAsync(storage, FenceLeaf, 2);
        await grain.ClaimAppendEpochAsync(FenceLeaf, 2, CancellationToken.None);
        await ListExplicitlyAsync(storage, FenceLeaf);

        var swept = await Sweeper(storage, grain, DateTimeOffset.UtcNow + WalAppendFenceSweeper.Grace - TimeSpan.FromHours(1))
            .SweepTreeAsync(TreeId, partitions: 1, CancellationToken.None);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(swept, Is.True);
            Assert.That((await WalAppendFenceRows.ReadEntryAsync(storage, TreeId, ShardIndex, FenceLeaf)).RecordExists, Is.True);
            Assert.That((await WalAppendFenceRows.ReadLeafEpochAsync(storage, FenceLeaf)).RecordExists, Is.True);
        });
    }

    [Test]
    public async Task A_tree_is_swept_once_per_interval_across_the_cluster()
    {
        var storage = new EnumerableMemoryGrainStorage();
        var grain = await CreateFencedGrainAsync(storage);
        var start = DateTimeOffset.UtcNow + TimeSpan.FromDays(2);

        var first = await Sweeper(storage, grain, start).SweepTreeAsync(TreeId, 1, CancellationToken.None);
        var otherSilo = await Sweeper(storage, grain, start + TimeSpan.FromMinutes(5)).SweepTreeAsync(TreeId, 1, CancellationToken.None);
        var nextInterval = await Sweeper(storage, grain, start + WalAppendFenceSweeper.Interval).SweepTreeAsync(TreeId, 1, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.True);
            Assert.That(otherSilo, Is.False, "another silo skips a tree swept within the interval");
            Assert.That(nextInterval, Is.True);
        });
    }
}
