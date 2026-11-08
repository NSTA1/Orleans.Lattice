using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The per-silo bound on retained-byte WAL usage scans (issue #6017): the WAL
/// partition grain is where the scan runs, so it is where the bound is applied,
/// and a scan that cannot take a slot within the admission wait is refused with
/// the <c>-1</c> unavailable sentinel instead of queueing.
/// </summary>
public partial class WalShardGrainTests
{
    [Test]
    public async Task GetByteSizesAsync_many_concurrent_scans_never_exceed_the_silo_bound_and_the_excess_is_refused()
    {
        const int Bound = 2;
        const int Callers = 6;
        var inner = new InMemoryWalStorageProvider();
        var provider = new RetainedScanGatingProvider(inner);
        var limiter = new WalUsageScanLimiter(Bound, TimeSpan.FromMilliseconds(200));
        var grain = await CreateGrainAsync(provider: provider, usageScanLimiter: limiter);

        var calls = Enumerable.Range(0, Callers)
            .Select(_ => grain.GetByteSizesAsync(CancellationToken.None))
            .ToList();

        // The excess callers are refused once the admission wait lapses, while
        // the admitted scans are still parked inside the provider.
        await TestPoll.UntilAsync(
            () => calls.Count(c => c.IsCompleted) == Callers - Bound,
            "the callers beyond the bound to be refused within the admission wait",
            TimeSpan.FromSeconds(10));

        var refused = calls.Where(c => c.IsCompleted).Select(c => c.Result).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(provider.MaxInFlight, Is.LessThanOrEqualTo(Bound),
                "the provider must never see more retained-byte scans in flight than the silo bound");
            Assert.That(limiter.InFlight, Is.EqualTo(Bound));
            Assert.That(refused.Select(r => r.Retained), Is.All.EqualTo(-1L),
                "a refused scan reports the unavailable sentinel, not a figure");
        });

        provider.Release();
        var admitted = await Task.WhenAll(calls).WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Multiple(() =>
        {
            Assert.That(admitted.Count(r => r.Retained >= 0), Is.EqualTo(Bound),
                "the admitted scans complete with a real figure once the provider answers");
            Assert.That(limiter.InFlight, Is.Zero, "every admitted scan returns its slot");
            Assert.That(provider.MaxInFlight, Is.LessThanOrEqualTo(Bound));
        });
    }

    [Test]
    public async Task GetRetainedByteSizeAsync_without_a_free_slot_returns_minus_one_within_the_admission_wait()
    {
        var inner = new InMemoryWalStorageProvider();
        var provider = new RetainedScanGatingProvider(inner);
        var limiter = new WalUsageScanLimiter(1, TimeSpan.FromMilliseconds(100));
        var grain = await CreateGrainAsync(provider: provider, usageScanLimiter: limiter);

        var holder = grain.GetRetainedByteSizeAsync(CancellationToken.None);
        await TestPoll.UntilAsync(() => provider.InFlight == 1, "the first scan to hold the only slot", TimeSpan.FromSeconds(10));

        var refused = await grain.GetRetainedByteSizeAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(10));

        Assert.That(refused, Is.EqualTo(-1L));
        provider.Release();
        Assert.That(await holder, Is.GreaterThanOrEqualTo(0L));
    }

    /// <summary>
    /// Delegates to an in-memory provider, but parks every retained-byte scan
    /// until <see cref="Release"/> and records the peak number in flight.
    /// </summary>
    private sealed class RetainedScanGatingProvider(InMemoryWalStorageProvider inner) : IWalStorageProvider
    {
        private readonly IWalStorageProvider _inner = inner;
        private readonly TaskCompletionSource _gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _inFlight;
        private int _maxInFlight;

        public int InFlight => Volatile.Read(ref _inFlight);

        public int MaxInFlight => Volatile.Read(ref _maxInFlight);

        public void Release() => _gate.TrySetResult();

        public async Task<long> GetRetainedByteSizeAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
        {
            var now = Interlocked.Increment(ref _inFlight);
            int seen;
            while (now > (seen = Volatile.Read(ref _maxInFlight)))
            {
                Interlocked.CompareExchange(ref _maxInFlight, now, seen);
            }

            try
            {
                await _gate.Task.WaitAsync(cancellationToken);
                return await _inner.GetRetainedByteSizeAsync(treeId, shardIndex, cancellationToken);
            }
            finally
            {
                Interlocked.Decrement(ref _inFlight);
            }
        }

        public Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => _inner.GetHighestOffsetAsync(treeId, shardIndex, cancellationToken);

        public Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
            => _inner.AppendBatchAsync(treeId, shardIndex, entries, cancellationToken);

        public Task AppendEncodedBatchAsync(
            string treeId, int shardIndex, ReadOnlyMemory<ArraySegment<byte>> encodedEntries,
            ReadOnlyMemory<long> offsets, IWalRecordEncoder encoder, CancellationToken cancellationToken)
            => _inner.AppendEncodedBatchAsync(treeId, shardIndex, encodedEntries, offsets, encoder, cancellationToken);

        public IAsyncEnumerable<WalEntry> ReadAsync(
            string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, CancellationToken cancellationToken)
            => _inner.ReadAsync(treeId, shardIndex, fromOffsetExclusive, maxEntries, cancellationToken);

        public Task<WalShardEncodedPage> ReadEncodedAsync(
            string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries,
            IWalRecordEncoder encoder, CancellationToken cancellationToken)
            => _inner.ReadEncodedAsync(treeId, shardIndex, fromOffsetExclusive, maxEntries, encoder, cancellationToken);

        public IAsyncEnumerable<WalEntry> ReadFilteredAsync(
            string treeId, int shardIndex, long fromOffsetExclusive, long toOffsetInclusive, int maxEntries,
            WalKeyFilter filter, CancellationToken cancellationToken)
            => _inner.ReadFilteredAsync(treeId, shardIndex, fromOffsetExclusive, toOffsetInclusive, maxEntries, filter, cancellationToken);

        public Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => _inner.GetLowestOffsetAsync(treeId, shardIndex, cancellationToken);

        public Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken)
            => _inner.TrimAsync(treeId, shardIndex, throughOffsetInclusive, cancellationToken);

        public Task EvaluateCompactionAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => _inner.EvaluateCompactionAsync(treeId, shardIndex, cancellationToken);

        public Task ReconcileAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => _inner.ReconcileAsync(treeId, shardIndex, cancellationToken);

        public Task<long> GetPhysicalByteSizeAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => _inner.GetPhysicalByteSizeAsync(treeId, shardIndex, cancellationToken);
    }
}
