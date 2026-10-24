using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class WalShardGrainTests
{
    private sealed class ReliefDrainProvider : IWalStorageProvider
    {
        private readonly InMemoryWalStorageProvider _inner = new();
        internal TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries,
            CancellationToken cancellationToken)
        {
            Entered.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        }

        public IAsyncEnumerable<WalEntry> ReadAsync(string treeId, int shardIndex, long fromOffsetExclusive,
            int maxEntries, CancellationToken cancellationToken)
            => _inner.ReadAsync(treeId, shardIndex, fromOffsetExclusive, maxEntries, cancellationToken);
        public Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => _inner.GetHighestOffsetAsync(treeId, shardIndex, cancellationToken);
        public Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => _inner.GetLowestOffsetAsync(treeId, shardIndex, cancellationToken);
        public Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken)
            => _inner.TrimAsync(treeId, shardIndex, throughOffsetInclusive, cancellationToken);
    }

    [TestCase(WalSaturationState.Saturated)]
    [TestCase(WalSaturationState.Throttled)]
    public async Task Flush_and_drain_relief_ignore_signal_pressure(WalSaturationState verdict)
    {
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(Arg.Any<string>()).Returns(verdict);
        var provider = new InMemoryWalStorageProvider();
        var grain = await CreateGrainAsync(provider,
            new LatticeOptions { WalMaxBatchEntries = 1 }, saturationSignal: signal);

        await grain.AppendAsync(MakeEntry("relief"), CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(await provider.GetHighestOffsetAsync(TreeId, ShardIndex, CancellationToken.None), Is.EqualTo(0));

        await grain.OnDeactivateAsync(new DeactivationReason(DeactivationReasonCode.ShuttingDown, "backpressure relief"),
            CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(5));

        Assert.That(signal.ReceivedCalls(), Is.Empty, "storage flush and drain cannot depend on the admission signal");
    }

    [TestCase(WalSaturationState.Saturated)]
    [TestCase(WalSaturationState.Throttled)]
    public async Task Drain_relief_settles_a_pending_storage_flush_without_a_signal_release(WalSaturationState verdict)
    {
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(Arg.Any<string>()).Returns(verdict);
        var provider = new ReliefDrainProvider();
        var grain = await CreateGrainAsync(provider,
            new LatticeOptions { WalMaxBatchEntries = 1 }, saturationSignal: signal);
        var append = grain.AppendAsync(MakeEntry("pending-relief"), CancellationToken.None);
        await provider.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await grain.OnDeactivateAsync(new DeactivationReason(DeactivationReasonCode.ShuttingDown, "backpressure relief"),
            CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(5));

        await Task.WhenAny(append, Task.Delay(TimeSpan.FromSeconds(5)));
        Assert.That(append.IsCompleted, Is.True, "drain settles the caller without any Healthy signal edge");
        Assert.That(async () => await append, Throws.InstanceOf<TimeoutException>());
        Assert.That(signal.ReceivedCalls(), Is.Empty);
    }
}
