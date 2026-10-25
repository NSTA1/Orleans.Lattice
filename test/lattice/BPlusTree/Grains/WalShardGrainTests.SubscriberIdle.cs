using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class WalShardGrainTests
{
    [Test]
    public async Task An_idle_subscriber_reprobes_and_sees_a_late_abandoned_flush_in_order()
    {
        var provider = new LateLandingWalStorageProvider(new InMemoryWalStorageProvider(), gatedOffset: 0);
        var grain = await CreateGrainAsync(provider, new LatticeOptions
        {
            WalMaxBatchEntries = 1,
            WalMaxPendingBatches = 8,
            WalFlushTimeout = TimeSpan.FromMilliseconds(300),
        });
        var abandoned = grain.AppendAsync(MakeEntry("late"), CancellationToken.None);
        await provider.OffsetGated.Task.WaitAsync(TimeSpan.FromSeconds(15));
        await grain.AppendAsync(MakeEntry("later"), CancellationToken.None);
        Assert.That(async () => await abandoned, Throws.TypeOf<TimeoutException>());

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalShardGrain>($"{TreeId}/0").Returns(grain);
        var reader = new WalCommitLogReader(factory);
        var idle = await DrainFromAsync(reader, checkpoint: -1);
        Assert.That(idle.Offsets, Is.Empty);
        Assert.That(idle.Result.AdvancedOffsets, Is.Empty);

        provider.Open();
        await provider.Landed.Task.WaitAsync(TimeSpan.FromSeconds(15));
        var resumed = await DrainFromAsync(reader, checkpoint: -1);
        Assert.That(resumed.Result.FellOffLog, Is.False);
        Assert.That(resumed.Offsets, Is.EqualTo(new long[] { 0, 1 }));
    }
}
