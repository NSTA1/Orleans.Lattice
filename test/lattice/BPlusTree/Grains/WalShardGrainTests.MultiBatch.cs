using Orleans.Lattice.BPlusTree.Grains;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

// Multi-batch in-flight flush concurrency tests (lifts the single-
// in-flight cap to LatticeOptions.WalMaxPendingBatches). The base
// fixture lives in WalShardGrainTests.cs; this partial only adds the
// tests that exercise the new concurrency surface.
public partial class WalShardGrainTests
{
    [Test]
    public async Task AppendAsync_default_cap_of_one_preserves_single_in_flight_batch_shape()
    {
        // With cap=1 (the default), exactly the same coalescing
        // behaviour as the pre-R-074 protocol must be observed: the
        // first append starts a flush of [a], the next three
        // accumulate behind the gate, and the follow-on flush captures
        // them as a single 3-entry batch.
        var gated = new GatedWalStorageProvider(new InMemoryWalStorageProvider());
        var capturing = new CapturingWalStorageProvider(gated);
        var grain = await CreateGrainAsync(capturing, new LatticeOptions
        {
            WalMaxPendingBatches = 1,
        });

        var t1 = grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        var t2 = grain.AppendAsync(MakeEntry("b"), CancellationToken.None);
        var t3 = grain.AppendAsync(MakeEntry("c"), CancellationToken.None);
        var t4 = grain.AppendAsync(MakeEntry("d"), CancellationToken.None);

        gated.Open();
        var offsets = await Task.WhenAll(t1, t2, t3, t4);

        Assert.Multiple(() =>
        {
            Assert.That(offsets, Is.EqualTo(new[] { 0L, 1L, 2L, 3L }));
            // cap=1 -> classic shape: [1, 3].
            Assert.That(capturing.BatchSizes, Is.EqualTo(new[] { 1, 3 }));
        });
    }

    [Test]
    public async Task AppendAsync_with_higher_cap_fans_out_multiple_flushes_concurrently()
    {
        // With WalMaxBatchEntries=1 (one entry per flush) and
        // WalMaxPendingBatches=4, four appends issued back-to-back
        // against a gated provider must all reach AppendBatchAsync
        // before any of them completes - i.e. the grain holds four
        // concurrent provider calls. With the old single-in-flight
        // protocol the second append would have been parked behind
        // the first flush's gate and the provider would only see one
        // pending call at a time.
        var gated = new ConcurrencyTrackingGatedProvider(new InMemoryWalStorageProvider());
        var grain = await CreateGrainAsync(gated, new LatticeOptions
        {
            WalMaxBatchEntries = 1,
            WalMaxPendingBatches = 4,
        });

        var t1 = grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        var t2 = grain.AppendAsync(MakeEntry("b"), CancellationToken.None);
        var t3 = grain.AppendAsync(MakeEntry("c"), CancellationToken.None);
        var t4 = grain.AppendAsync(MakeEntry("d"), CancellationToken.None);

        // Wait until every flush has entered the provider but is
        // still parked behind the gate. With cap=4 the grain must
        // have admitted all four concurrently.
        await gated.WaitForActiveAsync(4, TimeSpan.FromSeconds(5));

        Assert.That(gated.PeakActive, Is.EqualTo(4));

        gated.Open();
        var offsets = await Task.WhenAll(t1, t2, t3, t4);

        Assert.That(offsets, Is.EqualTo(new[] { 0L, 1L, 2L, 3L }));
    }

    [Test]
    public async Task AppendAsync_cap_at_two_applies_back_pressure_when_chain_is_full()
    {
        // With cap=2 and one-entry batches, the first two appends
        // fan out; the third append cuts over while the chain is
        // full, awaits the oldest in-flight, and only then admits
        // its own batch. The provider must never see more than two
        // concurrent calls.
        var gated = new ConcurrencyTrackingGatedProvider(new InMemoryWalStorageProvider());
        var grain = await CreateGrainAsync(gated, new LatticeOptions
        {
            WalMaxBatchEntries = 1,
            WalMaxPendingBatches = 2,
        });

        var t1 = grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        var t2 = grain.AppendAsync(MakeEntry("b"), CancellationToken.None);
        var t3 = grain.AppendAsync(MakeEntry("c"), CancellationToken.None);

        // First two must reach the provider; third is blocked on the
        // cap. Allow time for the cap-loop to settle into the wait.
        await gated.WaitForActiveAsync(2, TimeSpan.FromSeconds(5));
        await Task.Delay(50);

        Assert.That(gated.PeakActive, Is.LessThanOrEqualTo(2));

        gated.Open();
        var offsets = await Task.WhenAll(t1, t2, t3);

        Assert.Multiple(() =>
        {
            Assert.That(offsets, Is.EqualTo(new[] { 0L, 1L, 2L }));
            Assert.That(gated.PeakActive, Is.LessThanOrEqualTo(2));
        });
    }

    [Test]
    public async Task AppendAsync_multi_batch_failure_faults_failed_and_later_windows_and_pending()
    {
        // With cap=3 and one-entry batches, fan three appends out to
        // the provider; the third call throws. The contract: every
        // TCS in the failed window AND every later in-flight TCS
        // (none here - the failure is in the last slot) AND every
        // pending TCS (the fourth append, which races in while the
        // chain is in flight) is faulted with the underlying
        // exception. Then a subsequent append must succeed against
        // the provider's authoritative tail.
        var inner = new InMemoryWalStorageProvider();
        var failingThird = new FailNthAppendProvider(inner, failOn: 3, message: "boom-3");
        var grain = await CreateGrainAsync(failingThird, new LatticeOptions
        {
            WalMaxBatchEntries = 1,
            WalMaxPendingBatches = 3,
        });

        var t1 = grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        var t2 = grain.AppendAsync(MakeEntry("b"), CancellationToken.None);
        var t3 = grain.AppendAsync(MakeEntry("c"), CancellationToken.None);

        // t1 + t2 commit cleanly against the provider; t3 throws.
        var o1 = await t1;
        var o2 = await t2;
        Assert.That(o1, Is.EqualTo(0L));
        Assert.That(o2, Is.EqualTo(1L));

        Assert.That(async () => await t3, Throws.InvalidOperationException.With.Message.EqualTo("boom-3"));

        // After the grain has resynced from the provider, the next
        // append resumes at offset 2 (provider committed offsets 0..1
        // before the failure).
        var resume = await grain.AppendAsync(MakeEntry("e"), CancellationToken.None);
        Assert.That(resume, Is.EqualTo(2L));

        var head = await inner.GetHighestOffsetAsync(TreeId, ShardIndex, CancellationToken.None);
        Assert.That(head, Is.EqualTo(2L));
    }

    [Test]
    public async Task OnDeactivateAsync_drains_every_in_flight_slot()
    {
        // With cap=3 and gate-held flushes, deactivate must wait for
        // every in-flight provider call before returning. After the
        // gate opens and OnDeactivateAsync returns, every append's
        // TCS must be completed and the provider must hold every
        // entry.
        var gated = new GatedWalStorageProvider(new InMemoryWalStorageProvider());
        var grain = await CreateGrainAsync(gated, new LatticeOptions
        {
            WalMaxBatchEntries = 1,
            WalMaxPendingBatches = 3,
        });

        var t1 = grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        var t2 = grain.AppendAsync(MakeEntry("b"), CancellationToken.None);
        var t3 = grain.AppendAsync(MakeEntry("c"), CancellationToken.None);

        // Schedule deactivation before opening the gate; the
        // deactivation must not return until every flush settles.
        var deactivate = Task.Run(() => grain.OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ApplicationRequested, "test"),
            CancellationToken.None));

        // Give the deactivation a moment to enter its drain loop, then
        // release the gate.
        await Task.Delay(50);
        gated.Open();

        await deactivate;
        var offsets = await Task.WhenAll(t1, t2, t3);

        Assert.That(offsets, Is.EqualTo(new[] { 0L, 1L, 2L }));
    }

    /// <summary>
    /// Provider double that tracks concurrent in-flight
    /// <c>AppendBatchAsync</c> calls and blocks every one on a shared
    /// gate. <see cref="WaitForActiveAsync"/> spins until the desired
    /// concurrency level is observed; <see cref="PeakActive"/> records
    /// the maximum concurrency reached over the test's lifetime.
    /// </summary>
    private sealed class ConcurrencyTrackingGatedProvider(IWalStorageProvider inner) : IWalStorageProvider
    {
        private readonly TaskCompletionSource _gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _active;
        private int _peak;

        public int PeakActive => Volatile.Read(ref _peak);

        public void Open() => _gate.TrySetResult();

        public async Task WaitForActiveAsync(int target, TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow + timeout;
            while (Volatile.Read(ref _active) < target)
            {
                if (DateTime.UtcNow > deadline)
                {
                    Assert.Fail($"Timed out waiting for {target} concurrent provider calls; observed {Volatile.Read(ref _active)}.");
                }
                await Task.Delay(10);
            }
        }

        public async Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
        {
            var now = Interlocked.Increment(ref _active);
            // Race-tolerant max update.
            while (true)
            {
                var p = Volatile.Read(ref _peak);
                if (now <= p) { break; }
                if (Interlocked.CompareExchange(ref _peak, now, p) == p) { break; }
            }
            try
            {
                await _gate.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
                await inner.AppendBatchAsync(treeId, shardIndex, entries, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                Interlocked.Decrement(ref _active);
            }
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

    /// <summary>
    /// Provider that forwards to an inner provider for every call
    /// except the <c>failOn</c>-th, which throws an
    /// <see cref="InvalidOperationException"/>. Counts are 1-based.
    /// </summary>
    private sealed class FailNthAppendProvider(IWalStorageProvider inner, int failOn, string message) : IWalStorageProvider
    {
        private int _calls;

        public Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
        {
            var n = Interlocked.Increment(ref _calls);
            if (n == failOn)
            {
                throw new InvalidOperationException(message);
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