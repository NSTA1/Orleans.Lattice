using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Wal;

namespace Orleans.Lattice.Tests.Views;

public partial class ViewMaintainerAggregationDrainTests
{
    private static readonly int DefaultPartitions = new LatticeOptions().WalPartitions;

    [Test]
    public async Task Idle_drain_folds_lag_from_the_drains_own_head_probes()
    {
        var reader = Substitute.For<ICommitLogReader>();
        var h = Create(commitLogReader: reader);
        var heads = Enumerable.Range(0, DefaultPartitions).Select(p => (long)p).ToArray();
        ArmDrain(h, new WalDrainResult { PartitionHeads = heads });

        var applied = await h.Grain.DrainAsync();

        Assert.That(applied, Is.Zero);
        await reader.DidNotReceive().GetHeadOffsetAsync(
            Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Drain_that_read_entries_re_probes_heads_for_lag()
    {
        var reader = Substitute.For<ICommitLogReader>();
        var h = Create(commitLogReader: reader);
        var heads = Enumerable.Range(0, DefaultPartitions).Select(_ => 1L).ToArray();
        ArmDrain(h, new WalDrainResult { EntriesRead = 1, PartitionHeads = heads }, entries: [UserSet("a", 5)]);

        await h.Grain.DrainAsync();

        await reader.Received(DefaultPartitions).GetHeadOffsetAsync(
            SourceTreeId, Arg.Any<int>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Idle_drain_without_reported_heads_falls_back_to_probing()
    {
        var reader = Substitute.For<ICommitLogReader>();
        var h = Create(commitLogReader: reader);
        ArmDrain(h, new WalDrainResult());

        await h.Grain.DrainAsync();

        await reader.Received(DefaultPartitions).GetHeadOffsetAsync(
            SourceTreeId, Arg.Any<int>(), Arg.Any<CancellationToken>());
    }
}
