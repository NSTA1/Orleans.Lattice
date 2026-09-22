using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

// #3396: WAL append coalescing. A SetManyAsync fans out over shards and
// then over each shard's WAL partitions, so a batch reaches any one
// WalShardGrain already divided down to a handful of entries. Because the
// final entry of every batch unconditionally kicked a flush, each of those
// tiny slices paid a provider round trip of its own and append cost never
// amortised with load. WalAppendCoalescingInFlightThreshold suppresses that
// kick once N flushes are already outstanding.
//
// The base fixture (CreateGrainAsync, MakeEntry, TreeId, ShardIndex) lives
// in WalShardGrainTests.cs.
public partial class WalShardGrainTests
{
    private const int CoalescingEntriesPerBatch = 4;

    private static LatticeOptions CoalescingOptions(int threshold) => new()
    {
        WalAppendCoalescingInFlightThreshold = threshold,
        // Keep the per-batch caps well clear of anything these tests
        // accumulate, so the only variable under test is the
        // final-entry kick and not a cap-driven cutover.
        WalMaxBatchEntries = 4096,
        WalMaxBatchBytes = 32 * 1024 * 1024,
        WalMaxPendingBatches = 16,
    };

    [Test]
    public async Task AppendBatchAsync_with_coalescing_disabled_flushes_each_arrival_separately()
    {
        // The default-off contract. With the threshold at 0 the predicate
        // must be bit-identical to the historical unconditional kick:
        // every arrival opens its own flush, so five 4-entry batches
        // produce five 4-entry appends and nothing coalesces.
        var provider = new GatedCapturingWalStorageProvider(new InMemoryWalStorageProvider());
        var grain = await CreateGrainAsync(provider, CoalescingOptions(threshold: 0));

        var pending = await IssueCoalescingBatchesAsync(grain, provider, batchCount: 5);

        provider.Open();
        await Task.WhenAll(pending).WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Multiple(() =>
        {
            Assert.That(
                provider.BatchSizes,
                Has.Count.EqualTo(5),
                "Coalescing disabled must open one flush per arrival.");
            Assert.That(
                provider.BatchSizes,
                Is.All.EqualTo(CoalescingEntriesPerBatch),
                "Every flush must carry exactly the arriving batch, as it always has.");
        });
    }

    [Test]
    public async Task AppendBatchAsync_coalesces_arrivals_once_in_flight_reaches_threshold()
    {
        // The fix. With the threshold at 1 the first arrival opens a flush
        // and the remaining four are suppressed, accumulate into the
        // pending batch, and settle together as one larger append.
        var provider = new GatedCapturingWalStorageProvider(new InMemoryWalStorageProvider());
        var grain = await CreateGrainAsync(provider, CoalescingOptions(threshold: 1));

        var pending = await IssueCoalescingBatchesAsync(grain, provider, batchCount: 5);

        provider.Open();
        await Task.WhenAll(pending).WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Multiple(() =>
        {
            Assert.That(
                provider.BatchSizes,
                Has.Count.LessThan(5),
                "Suppressing the final-entry kick must produce fewer appends than arrivals.");
            Assert.That(
                provider.BatchSizes.Max(),
                Is.GreaterThan(CoalescingEntriesPerBatch),
                "At least one append must carry more than a single arrival's worth of entries.");
            Assert.That(
                provider.BatchSizes.Sum(),
                Is.EqualTo(5 * CoalescingEntriesPerBatch),
                "Coalescing must not lose or duplicate entries.");
        });
    }

    [Test]
    public async Task AppendBatchAsync_coalesced_arrivals_are_never_stranded()
    {
        // The safety property that makes enabling this by default sound.
        // A suppressed arrival owns no flush of its own, so it can only be
        // drained by the follow-on kick fired when an outstanding flush
        // settles. Suppression requires inFlight >= threshold >= 1, so such
        // a flush necessarily exists - but if that reasoning were ever
        // broken the pending batch would hang forever, which is exactly
        // what this test would catch.
        var provider = new GatedCapturingWalStorageProvider(new InMemoryWalStorageProvider());
        var grain = await CreateGrainAsync(provider, CoalescingOptions(threshold: 1));

        var pending = await IssueCoalescingBatchesAsync(grain, provider, batchCount: 6);

        provider.Open();

        Assert.DoesNotThrowAsync(
            async () => await Task.WhenAll(pending).WaitAsync(TimeSpan.FromSeconds(30)),
            "Every coalesced arrival must complete; a hang here means a pending batch was stranded.");

        var offsets = (await Task.WhenAll(pending)).SelectMany(o => o).OrderBy(o => o).ToArray();
        Assert.That(
            offsets,
            Is.EqualTo(Enumerable.Range(0, 6 * CoalescingEntriesPerBatch).Select(i => (long)i).ToArray()),
            "Coalescing must still assign dense ascending offsets across every arrival.");
    }

    [Test]
    public async Task AppendBatchAsync_coalescing_preserves_durable_entry_order()
    {
        // Coalescing changes how many entries share a flush window, and
        // must change nothing about what lands or in what order.
        var inner = new InMemoryWalStorageProvider();
        var provider = new GatedCapturingWalStorageProvider(inner);
        var grain = await CreateGrainAsync(provider, CoalescingOptions(threshold: 1));

        var pending = await IssueCoalescingBatchesAsync(grain, provider, batchCount: 4);

        provider.Open();
        await Task.WhenAll(pending).WaitAsync(TimeSpan.FromSeconds(30));

        var durable = new List<long>();
        await foreach (var entry in inner.ReadAsync(TreeId, ShardIndex, -1, int.MaxValue, CancellationToken.None))
        {
            durable.Add(entry.Offset);
        }

        Assert.That(
            durable,
            Is.EqualTo(Enumerable.Range(0, 4 * CoalescingEntriesPerBatch).Select(i => (long)i).ToArray()),
            "Every entry must be durable exactly once, in ascending offset order.");
    }

    /// <summary>
    /// Starts <paramref name="batchCount"/> concurrent
    /// <c>AppendBatchAsync</c> calls against a gated provider, ensuring the
    /// first has actually entered the provider (so exactly one flush is
    /// outstanding) before the rest are issued. The returned tasks stay
    /// pending until the caller opens the gate.
    /// </summary>
    private static async Task<Task<IReadOnlyList<long>>[]> IssueCoalescingBatchesAsync(
        WalShardGrain grain,
        GatedCapturingWalStorageProvider provider,
        int batchCount)
    {
        var pending = new Task<IReadOnlyList<long>>[batchCount];

        pending[0] = grain.AppendBatchAsync(MakeCoalescingBatch(0), CancellationToken.None);

        // Wait until the first flush is genuinely in flight inside the
        // provider; issuing the rest before that would race the very
        // in-flight depth the threshold is tested against.
        await provider.FirstCallEntered.WaitAsync(TimeSpan.FromSeconds(30));

        for (var i = 1; i < batchCount; i++)
        {
            pending[i] = grain.AppendBatchAsync(MakeCoalescingBatch(i), CancellationToken.None);
        }

        // Let the suppressed arrivals reach the pending batch before the
        // caller opens the gate.
        await Task.Delay(100);

        return pending;
    }

    private static WalRecord[] MakeCoalescingBatch(int batchIndex)
    {
        var entries = new WalRecord[CoalescingEntriesPerBatch];
        for (var i = 0; i < CoalescingEntriesPerBatch; i++)
        {
            entries[i] = MakeEntry($"b{batchIndex:D2}k{i:D2}");
        }
        return entries;
    }

    /// <summary>
    /// Blocks every append on a shared gate (so tests control in-flight
    /// depth deterministically) while recording the size of each flush the
    /// grain actually issues, and signalling when the first append has
    /// entered the provider.
    /// </summary>
    private sealed class GatedCapturingWalStorageProvider(IWalStorageProvider inner) : IWalStorageProvider
    {
        private readonly TaskCompletionSource _gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _firstCallEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly Lock _sync = new();

        public List<int> BatchSizes { get; } = new();

        public Task FirstCallEntered => _firstCallEntered.Task;

        public void Open() => _gate.TrySetResult();

        public async Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
        {
            lock (_sync)
            {
                BatchSizes.Add(entries.Count);
            }
            _firstCallEntered.TrySetResult();
            await _gate.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
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
