using Orleans.Lattice.BPlusTree;
using Orleans.Lattice;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

// WAL flush-deadline tests. A provider call that hangs indefinitely
// (e.g. against a partition left half-activated by a placement /
// reshard race) would, without a bound, never settle its in-flight
// slot - the slot would never leave the in-flight chain, the chain
// would saturate at WalMaxPendingBatches, and every subsequent append
// would back-pressure behind a flush that can never complete (a
// steady-state stall with no fault and no activation recycle). The
// LatticeOptions.WalFlushTimeout deadline converts that hang into a
// recoverable TimeoutException routed through the normal failure
// handler. The base fixture lives in WalShardGrainTests.cs.
public partial class WalShardGrainTests
{
    [Test]
    public async Task AppendAsync_hung_provider_flush_faults_with_timeout_when_deadline_elapses()
    {
        // The provider's flush hangs forever. With a short flush
        // deadline the grain must surface a TimeoutException to the
        // append caller rather than parking it indefinitely.
        var hanging = new HangingAppendWalStorageProvider(new InMemoryWalStorageProvider());
        var grain = await CreateGrainAsync(hanging, new LatticeOptions
        {
            WalMaxBatchEntries = 1,
            WalMaxPendingBatches = 1,
            WalFlushTimeout = TimeSpan.FromMilliseconds(100),
        });

        Assert.That(
            async () => await grain.AppendAsync(MakeEntry("a"), CancellationToken.None),
            Throws.TypeOf<TimeoutException>());
    }

    [Test]
    public async Task AppendAsync_hung_flush_does_not_wedge_the_in_flight_chain()
    {
        // The regression this whole fix targets: after a flush hangs
        // and trips the deadline, the in-flight chain must drain so a
        // subsequent append succeeds. Without the bound the first
        // flush's slot would pin the chain at the cap forever and this
        // second append would never complete.
        var inner = new InMemoryWalStorageProvider();
        var hanging = new HangingAppendWalStorageProvider(inner, hangCount: 1);
        var grain = await CreateGrainAsync(hanging, new LatticeOptions
        {
            WalMaxBatchEntries = 1,
            WalMaxPendingBatches = 1,
            WalFlushTimeout = TimeSpan.FromMilliseconds(100),
        });

        // First append hangs in the provider, trips the deadline, and
        // faults - the slot is removed and the tail is resynced.
        Assert.That(
            async () => await grain.AppendAsync(MakeEntry("a"), CancellationToken.None),
            Throws.TypeOf<TimeoutException>());

        // The chain has drained; the next append flows through to the
        // (now un-gated) inner provider and commits at offset 0.
        var offset = await grain.AppendAsync(MakeEntry("b"), CancellationToken.None)
            .WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(offset, Is.EqualTo(0L));
    }

    [Test]
    public async Task AppendAsync_infinite_flush_timeout_preserves_unbounded_await()
    {
        // With the deadline disabled the historical unbounded-await
        // behaviour holds: a gated flush parks the append until the
        // gate opens, with no spurious timeout.
        var gated = new GatedWalStorageProvider(new InMemoryWalStorageProvider());
        var grain = await CreateGrainAsync(gated, new LatticeOptions
        {
            WalMaxBatchEntries = 1,
            WalMaxPendingBatches = 1,
            WalFlushTimeout = Timeout.InfiniteTimeSpan,
        });

        var append = grain.AppendAsync(MakeEntry("a"), CancellationToken.None);

        // Give the flush a window to spuriously time out if the bound
        // were mis-applied; it must still be pending.
        await Task.Delay(150);
        Assert.That(append.IsCompleted, Is.False);

        gated.Open();
        var offset = await append.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(offset, Is.EqualTo(0L));
    }

    /// <summary>
    /// Provider whose first <paramref name="hangCount"/> append calls
    /// block forever (honouring the supplied cancellation token, so the
    /// grain's flush deadline can cancel them); subsequent calls forward
    /// to the inner provider. Models a partition whose provider call
    /// hangs against a half-activated backend.
    /// </summary>
    private sealed class HangingAppendWalStorageProvider(
        IWalStorageProvider inner,
        int hangCount = int.MaxValue) : IWalStorageProvider
    {
        private int _calls;

        public async Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
        {
            var ordinal = Interlocked.Increment(ref _calls);
            if (ordinal <= hangCount)
            {
                // Hang until cancelled by the grain's flush deadline.
                await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
                return;
            }
            await inner.AppendBatchAsync(treeId, shardIndex, entries, cancellationToken).ConfigureAwait(false);
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
