using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for the WAL shard's key-filtered unstamped watermark (#5851, a5e2e2ef's
/// K1-K6; model mutations in <c>WalLiveAdvanceCore</c>, #5860): a split cut reads the
/// unstamped high for the reader's own key range. Every unstamped record is tabled by its
/// key or folded into a floor that bounds every reader; a record that may bear on other
/// keys, a record overwritten past the table's capacity and everything before a restart
/// all bound every reader; and the global watermark every stamped response reports is
/// unchanged.
/// </summary>
public partial class WalShardGrainTests
{
    private static Task<WalSplitCut> CutForAsync(WalShardGrain grain, string? low, string? high) =>
        grain.GetSplitCutAsync(default, low, high, CancellationToken.None);

    [Test]
    public async Task UnstampedSpanWriteUntabled()
    {
        // ReadPositionHonestUnstampedSpanWriteUntabled (K1, K2): an unstamped write in the
        // reader's range raises its cut; one outside it does not; an unbounded reader reads
        // the global watermark.
        var grain = await CreateFencedGrainAsync(new EnumerableMemoryGrainStorage());
        var inside = await grain.AppendAsync(MakeEntry("m"), CancellationToken.None);
        var outside = await grain.AppendAsync(MakeEntry("x"), CancellationToken.None);

        var reader = await CutForAsync(grain, "k", "p");
        var elsewhere = await CutForAsync(grain, "a", "c");
        var unbounded = await CutForAsync(grain, null, null);

        Assert.Multiple(() =>
        {
            Assert.That(reader.UnstampedWatermark, Is.EqualTo(inside), "an unstamped write keyed in the reader's range did not bound its cut");
            Assert.That(elsewhere.UnstampedWatermark, Is.LessThan(inside), "a stray unstamped write outside the reader's range capped its cut");
            Assert.That(unbounded.UnstampedWatermark, Is.EqualTo(outside), "an unbounded reader did not read the global watermark");
            Assert.That(grain.UnstampedWatermarkForTesting, Is.EqualTo(outside), "the global watermark every stamped response reports changed");
        });
    }

    [Test]
    public async Task UnstampedBatchWriteTabledByKey()
    {
        // K1 on the batch path: each unstamped record of a batch is tabled by its own key.
        var grain = await CreateFencedGrainAsync(new EnumerableMemoryGrainStorage());
        var batch = await grain.AppendBatchAsync(new[] { MakeEntry("b"), MakeEntry("y") }, CancellationToken.None);

        var low = await CutForAsync(grain, "a", "c");
        var high = await CutForAsync(grain, "x", null);
        var between = await CutForAsync(grain, "c", "x");

        Assert.Multiple(() =>
        {
            Assert.That(low.UnstampedWatermark, Is.EqualTo(batch[0]));
            Assert.That(high.UnstampedWatermark, Is.EqualTo(batch[1]));
            Assert.That(between.UnstampedWatermark, Is.LessThan(batch[0]),
                "a batch's records bounded a reader owning none of their keys");
        });
    }

    [Test]
    public async Task UnstampedMultiKeyRecordBoundsEveryReader()
    {
        // K3: a range delete, a transaction terminal or a record naming matched keys may bear
        // on keys other than its own, so it bounds every reader, whatever its key.
        var grain = await CreateFencedGrainAsync(new EnumerableMemoryGrainStorage());
        var range = await grain.AppendAsync(MakeEntry("x") with { Op = MutationKind.DeleteRange, EndExclusiveKey = "z" }, CancellationToken.None);
        var rangeCut = await CutForAsync(grain, "a", "c");
        var commit = await grain.AppendAsync(MakeEntry("x") with { Op = MutationKind.TxCommit, TransactionId = Guid.NewGuid() }, CancellationToken.None);
        var commitCut = await CutForAsync(grain, "a", "c");
        var matched = await grain.AppendAsync(MakeEntry("x") with { MatchedKeys = ["b"] }, CancellationToken.None);
        var matchedCut = await CutForAsync(grain, "a", "c");

        Assert.Multiple(() =>
        {
            Assert.That(rangeCut.UnstampedWatermark, Is.EqualTo(range), "a range delete keyed elsewhere did not bound the reader");
            Assert.That(commitCut.UnstampedWatermark, Is.EqualTo(commit), "a transaction terminal keyed elsewhere did not bound the reader");
            Assert.That(matchedCut.UnstampedWatermark, Is.EqualTo(matched), "a record naming matched keys did not bound the reader");
        });
    }

    [Test]
    public async Task UnstampedTableOverflowFoldsIntoFloor()
    {
        // K1: past the table's capacity the oldest record's offset folds into the floor,
        // which still bounds every reader.
        var grain = await CreateFencedGrainAsync(new EnumerableMemoryGrainStorage());
        var first = await grain.AppendAsync(MakeEntry("b"), CancellationToken.None);
        var before = await CutForAsync(grain, "x", null);
        for (var i = 0; i < WalShardGrain.UnstampedKeyTableCapacity; i++)
        {
            await grain.AppendAsync(MakeEntry("m"), CancellationToken.None);
        }

        var after = await CutForAsync(grain, "x", null);

        Assert.Multiple(() =>
        {
            Assert.That(before.UnstampedWatermark, Is.LessThan(first), "precondition: the first record was tabled, not floored");
            Assert.That(after.UnstampedWatermark, Is.GreaterThanOrEqualTo(first), "a record overwritten past capacity no longer bounds every reader");
        });
    }

    [Test]
    public async Task UnstampedFilterRestartFloor()
    {
        // K5 (ReadPositionHonestRestartForgetsUnstamped): the table is not persisted, so after
        // a restart every reader's filtered high is at least the recovered head minus one.
        var provider = new InMemoryWalStorageProvider();
        var storage = new EnumerableMemoryGrainStorage();
        var grain = await CreateFencedGrainAsync(storage, provider);
        await grain.AppendAsync(MakeEntry("b"), CancellationToken.None);
        var last = await grain.AppendAsync(MakeEntry("m"), CancellationToken.None);

        var restarted = await CreateFencedGrainAsync(storage, provider);
        var cut = await CutForAsync(restarted, "x", null);

        Assert.That(cut.UnstampedWatermark, Is.GreaterThanOrEqualTo(last), "a restart forgot an unstamped record a reader owning none of the tabled keys had not read");
    }
}
