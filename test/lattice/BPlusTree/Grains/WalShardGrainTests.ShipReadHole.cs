using Orleans.Lattice;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class WalShardGrainTests
{
    // Regression for the cross-cluster first-batch cold-partition data-loss
    // bug (fix/replication-first-batch-cold-partition-drop). Under a large
    // first write burst, a single WAL shard can have MULTIPLE concurrent
    // in-flight flushes (WalMaxPendingBatches > 1). Each flush persists its
    // own contiguous offset window to the provider INDEPENDENTLY - there is
    // no chaining that guarantees the lower-offset flush lands before a
    // higher-offset one. So a higher window (e.g. offsets [1,2]) can be
    // durable on the provider while the lowest window (offset [0]) is still
    // in-flight, leaving a TRANSIENT prefix hole in the persisted log.
    //
    // The shipper (ReplicationShipperGrain) drains a partition via
    // ReadShippingAsync(fromSequence) and advances its durable per-partition
    // cursor to the returned page's NextSequence (= last returned offset + 1).
    // If ReadShippingAsync returns the higher offsets [1,2] while offset 0 is
    // still in-flight, the shipper ships [1,2], advances its cursor to 3, and
    // NEVER re-reads offset 0 once it finally persists - a permanent,
    // silent replication gap. This is the sender-side "cursor advanced past
    // an unsent entry" residual of #1076.
    //
    // The fix caps every WAL read at the durable-contiguous watermark (the
    // lowest in-flight flush start offset): entries at or beyond that offset
    // are not yet guaranteed durable-and-contiguous, so they must not be
    // exposed to a cursor-advancing reader until the hole below them fills.

    [Test]
    public async Task ReadShippingAsync_does_not_expose_offsets_above_an_in_flight_prefix_hole()
    {
        // WalMaxBatchEntries = 1 so each append flushes its own single-offset
        // window; WalMaxPendingBatches high so three flushes can be in flight
        // at once. The gate parks ONLY the flush that owns offset 0.
        var gate = new PrefixHoleGatingWalStorageProvider(new InMemoryWalStorageProvider(), gatedOffset: 0);
        var options = new LatticeOptions
        {
            WalMaxBatchEntries = 1,
            WalMaxPendingBatches = 8,
        };
        var grain = await CreateGrainAsync(gate, options);

        // Fire three appends WITHOUT awaiting. Whichever append is assigned
        // offset 0 parks inside the gated flush; the other two (offsets 1, 2)
        // flush through to the inner provider and become durable.
        var t0 = grain.AppendAsync(MakeEntry("k0"), CancellationToken.None);
        var t1 = grain.AppendAsync(MakeEntry("k1"), CancellationToken.None);
        var t2 = grain.AppendAsync(MakeEntry("k2"), CancellationToken.None);

        try
        {
            // Wait until the offset-0 flush has parked in the gate and the
            // inner provider durably holds offsets 1 and 2 - i.e. the
            // transient prefix hole [0] genuinely exists.
            await gate.OffsetGated.Task.WaitAsync(TimeSpan.FromSeconds(15));
            await WaitForInnerHighestOffsetAsync(gate, expected: 2L, TimeSpan.FromSeconds(15));

            // Sanity: offset 0 is NOT durable on the provider yet, but 1 and 2
            // are - a genuine prefix hole exposed to a raw provider read.
            var lowest = await gate.InnerLowestOffsetAsync();
            Assert.That(lowest, Is.EqualTo(1L),
                "Test setup invariant: the inner provider must hold offsets 1..2 with offset 0 still in-flight.");

            // A shipper draining this partition from sequence 0 must NOT be
            // handed offsets 1/2 while offset 0 is unfilled: doing so advances
            // its durable cursor past offset 0 and strands it forever.
            var page = await grain.ReadShippingAsync(0L, 256, CancellationToken.None);

            Assert.That(
                page.Entries.Select(e => e.Sequence),
                Does.Not.Contain(1L),
                "ReadShippingAsync must not expose offset 1 while offset 0 is still in-flight; "
                + "returning it advances the shipper's per-partition cursor past the unshipped offset 0 "
                + "and permanently strands it (cross-cluster first-batch data loss).");
            Assert.That(page.Entries, Is.Empty,
                "With the lowest offset still in-flight the durable-contiguous prefix is empty, "
                + "so the shipping read must return nothing until the hole fills.");
            Assert.That(page.NextSequence, Is.EqualTo(0L),
                "The shipping cursor must not advance while the prefix hole is unfilled.");
        }
        finally
        {
            // Release the parked flush so offset 0 lands and the grain drains
            // cleanly for teardown.
            gate.Open();
            await Task.WhenAll(t0, t1, t2).WaitAsync(TimeSpan.FromSeconds(15));
        }

        // Once the hole fills, the shipping read exposes the whole contiguous
        // log [0,1,2] - nothing was lost, only deferred.
        var recovered = await grain.ReadShippingAsync(0L, 256, CancellationToken.None);
        Assert.That(
            recovered.Entries.Select(e => e.Sequence),
            Is.EqualTo(new[] { 0L, 1L, 2L }),
            "After the in-flight flush lands, the shipping read must expose the full contiguous log.");
        Assert.That(recovered.NextSequence, Is.EqualTo(3L));
    }

    private static async Task WaitForInnerHighestOffsetAsync(
        PrefixHoleGatingWalStorageProvider gate,
        long expected,
        TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (await gate.InnerHighestOffsetAsync() >= expected)
            {
                return;
            }

            await Task.Delay(10);
        }

        Assert.Fail($"Inner provider did not reach highest offset {expected} within {timeout}.");
    }

    /// <summary>
    /// <see cref="IWalStorageProvider"/> decorator that parks the flush which
    /// owns a specific offset on a gate until the test calls <c>Open</c>,
    /// while letting every other flush through to the inner provider. This
    /// deterministically manufactures a transient persisted-log prefix hole:
    /// higher offsets become durable while the gated lower offset is still
    /// in-flight. The encoded flush path routes through
    /// <see cref="AppendBatchAsync"/> via the default
    /// <c>AppendEncodedBatchAsync</c> fallback, so gating the legacy overload
    /// gates the real flush.
    /// </summary>
    private sealed class PrefixHoleGatingWalStorageProvider(IWalStorageProvider inner, long gatedOffset) : IWalStorageProvider
    {
        private readonly TaskCompletionSource _gate = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal TaskCompletionSource OffsetGated { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Open() => _gate.TrySetResult();

        internal Task<long> InnerHighestOffsetAsync() =>
            inner.GetHighestOffsetAsync(TreeId, ShardIndex, CancellationToken.None);

        internal Task<long> InnerLowestOffsetAsync() =>
            inner.GetLowestOffsetAsync(TreeId, ShardIndex, CancellationToken.None);

        public async Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
        {
            var ownsGatedOffset = false;
            for (var i = 0; i < entries.Count; i++)
            {
                if (entries[i].Offset == gatedOffset)
                {
                    ownsGatedOffset = true;
                    break;
                }
            }

            if (ownsGatedOffset)
            {
                OffsetGated.TrySetResult();
                await _gate.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
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
