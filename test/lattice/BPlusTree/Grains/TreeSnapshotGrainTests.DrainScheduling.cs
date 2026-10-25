using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class TreeSnapshotGrainTests
{
    [Test]
    public async Task Online_slice_bounds_each_merge_pass_before_completing_all_source_leaves()
    {
        var h = CreateCopyingSnapshot(leafCount: 5, leavesPerPass: 2, SnapshotMode.Online);
        var passes = new List<int>();
        h.DestShard.MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>()).Returns(call =>
        {
            var rows = call.ArgAt<Dictionary<string, LwwValue<byte[]>>>(0);
            passes.Add(rows.Count);
            h.MergedKeys.AddRange(rows.Keys);
            return Task.FromResult<SplitResult?>(null);
        });

        await h.Grain.DrainAllShardsOnlineAsync();

        Assert.Multiple(() =>
        {
            Assert.That(passes, Is.EqualTo(new[] { 2, 2, 1 }),
                "a slice may run multiple passes, but each pass must retain the configured source-leaf limit");
            Assert.That(h.MergedKeys, Is.EquivalentTo(NumberedLiveEntries(5).Keys),
                "the complete slice must retain all bounded-pass progress");
            Assert.That(h.State.State.NextShardIndex, Is.EqualTo(1));
        });
    }
}
