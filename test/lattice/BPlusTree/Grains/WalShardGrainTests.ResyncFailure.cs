using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the post-failure resync's own failure (#3348). A failed
/// resync keeps the sticky fault latched; without a deactivation request
/// nothing retires the activation while traffic keeps it warm, so every
/// later append rethrows the latched fault forever.
/// </summary>
public partial class WalShardGrainTests
{
    private static async Task<(WalShardGrain Grain, IGrainContext Context)> CreateGrainWithContextAsync(
        IWalStorageProvider provider,
        LatticeOptions options)
    {
        var grainContext = Substitute.For<IGrainContext>();
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);
        var grain = new WalShardGrain(
            grainContext,
            monitor,
            TestOptionsResolver.Create(baseOptions: options),
            CreatePermissiveResolver(),
            CreatePermissiveClusterIdResolver(),
            CreateDefaultEncoder());
        await grain.InitializeForTestingAsync(TreeId, ShardIndex, provider, CancellationToken.None, 0);
        return (grain, grainContext);
    }

    private static LatticeOptions SingleSlotOptions() => new()
    {
        WalMaxBatchEntries = 1,
        WalMaxPendingBatches = 1,
        WalFlushTimeout = TimeSpan.FromSeconds(5),
    };

    [Test]
    public async Task FlushFailure_resync_failure_requests_deactivation_and_keeps_fault_latched()
    {
        var provider = new ResyncFailingWalStorageProvider(new InMemoryWalStorageProvider(), failResync: true);
        var (grain, context) = await CreateGrainWithContextAsync(provider, SingleSlotOptions());

        Assert.That(
            async () => await grain.AppendAsync(MakeEntry("a"), CancellationToken.None),
            Throws.InvalidOperationException.With.Message.EqualTo("flush-boom"));

        context.Received(1).Deactivate(
            Arg.Is<DeactivationReason>(r => r.ReasonCode == DeactivationReasonCode.ApplicationRequested),
            Arg.Any<CancellationToken>());
        Assert.That(
            async () => await grain.AppendAsync(MakeEntry("b"), CancellationToken.None),
            Throws.InvalidOperationException.With.Message.EqualTo("flush-boom"),
            "the latched fault must keep surfacing until the activation retires");
    }

    [Test]
    public async Task FlushFailure_successful_resync_does_not_request_deactivation()
    {
        var provider = new ResyncFailingWalStorageProvider(new InMemoryWalStorageProvider(), failResync: false);
        var (grain, context) = await CreateGrainWithContextAsync(provider, SingleSlotOptions());

        Assert.That(
            async () => await grain.AppendAsync(MakeEntry("a"), CancellationToken.None),
            Throws.InvalidOperationException);
        var offset = await grain.AppendAsync(MakeEntry("b"), CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(5));

        Assert.That(offset, Is.EqualTo(0L));
        context.DidNotReceive().Deactivate(Arg.Any<DeactivationReason>(), Arg.Any<CancellationToken>());
    }

    /// <summary>
    /// <see cref="IWalStorageProvider"/> double whose first flush faults
    /// and whose post-failure reconcile (the second reconcile call, the
    /// first being activation's) optionally throws.
    /// </summary>
    private sealed class ResyncFailingWalStorageProvider(InMemoryWalStorageProvider inner, bool failResync) : IWalStorageProvider
    {
        private int _flushCalls;
        private int _reconcileCalls;

        public Task ReconcileAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
        {
            if (Interlocked.Increment(ref _reconcileCalls) >= 2 && failResync)
            {
                throw new TimeoutException("resync-boom");
            }

            return Task.CompletedTask;
        }

        public Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
        {
            if (Interlocked.Increment(ref _flushCalls) == 1)
            {
                throw new InvalidOperationException("flush-boom");
            }

            return inner.AppendBatchAsync(treeId, shardIndex, entries, cancellationToken);
        }

        public IAsyncEnumerable<WalEntry> ReadAsync(string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, CancellationToken cancellationToken)
            => inner.ReadAsync(treeId, shardIndex, fromOffsetExclusive, maxEntries, cancellationToken);

        public Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => inner.GetHighestOffsetAsync(treeId, shardIndex, cancellationToken);

        public Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => inner.GetLowestOffsetAsync(treeId, shardIndex, cancellationToken);

        public Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken)
            => inner.TrimAsync(treeId, shardIndex, throughOffsetInclusive, cancellationToken);
    }
}