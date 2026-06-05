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
        // behaviour as the pre-multi-batch protocol must be observed:
        // the first append starts a flush of [a], the next three
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
        // the provider; the slot owning offset 2 (the third entry)
        // throws. The contract: every TCS in the failed window AND
        // every later in-flight TCS (none here - the failure is in
        // the last slot) AND every pending TCS (the fourth append,
        // which races in while the chain is in flight) is faulted
        // with the underlying exception. Then a subsequent append
        // must succeed against the provider's authoritative tail.
        //
        // The failure trigger is offset-based, not call-ordinal-
        // based: with three flushes fanned out concurrently the
        // provider-call arrival order at AppendBatchAsync is
        // threadpool-scheduling-dependent (any sync work between
        // FlushAsync's Task.Yield and the provider call perturbs
        // it), so a call-ordinal trigger was flaky. The slot owning
        // a given offset is stable by construction - it is decided
        // synchronously inside the grain turn that admitted the
        // append.
        var inner = new InMemoryWalStorageProvider();
        var failingThird = new FailOnOffsetAppendProvider(inner, failOnOffset: 2, message: "boom-3");
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
        // Historically this test held three flushes gated, deactivated,
        // then opened the gate to verify the drain waited for every
        // in-flight slot before returning. With the drain-CTS link the
        // drain now actively cancels every in-flight provider call at
        // drain entry so a co-operative provider gives
        // up promptly - and the gated provider's
        // `_gate.Task.WaitAsync(cancellationToken)` observes the
        // cancellation, surfacing every flush as
        // <see cref="TimeoutException"/> rather than waiting for the
        // gate to open. The "drained-before-returning" contract still
        // holds (the deactivation does not return with appends still
        // parked on their TCS), but the form of the drain is now
        // "actively cancel and fault" rather than "passively await".
        // The post-drain assertion is updated to reflect the new
        // contract: every in-flight slot's TCS settles (with the
        // typed fault), and OnDeactivateAsync returns promptly without
        // the test having to open the gate first.
        var gated = new GatedWalStorageProvider(new InMemoryWalStorageProvider());
        var grain = await CreateGrainAsync(gated, new LatticeOptions
        {
            WalMaxBatchEntries = 1,
            WalMaxPendingBatches = 3,
        });

        var t1 = grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        var t2 = grain.AppendAsync(MakeEntry("b"), CancellationToken.None);
        var t3 = grain.AppendAsync(MakeEntry("c"), CancellationToken.None);

        // Deactivate; the drain signals the per-activation drain CTS
        // which cancels the gated provider's await, every flush faults
        // through the normal failure handler, and the chain settles.
        await grain.OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ApplicationRequested, "test"),
            CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(10));

        // Every append's TCS must be terminal (faulted with
        // TimeoutException via the FlushAsync deadline catch). The
        // contract: no caller observes a hung TCS after the drain.
        Assert.That(async () => await t1, Throws.InstanceOf<TimeoutException>());
        Assert.That(async () => await t2, Throws.InstanceOf<TimeoutException>());
        Assert.That(async () => await t3, Throws.InstanceOf<TimeoutException>());

        // Open the gate so the abandoned provider tasks complete
        // cleanly (otherwise their parked _gate.Task.WaitAsync awaits
        // would linger in GC's freachable queue beyond the test).
        gated.Open();
    }

    [Test]
    public async Task AppendAsync_steady_fan_in_during_in_flight_flush_pipelines_against_cap()
    {
        // U9p step 7 regression: with cap = 4 and the default per-batch
        // ceilings (so the per-batch caps are not the trigger), four
        // appends fanning in while the first flush is still gated MUST
        // pipeline into separate in-flight slots, not coalesce behind a
        // single one. Pre-step-7 the kick predicate keyed off
        // `_inFlight.Count == 0`, so callers 2..N parked on their TCS
        // and the cap was never reachable - which is precisely what the
        // step-6 telemetry observed against real Azure Tables
        // (`wal.append.in_flight = 0`, batch p50 = 8 entries at deep
        // queue). Post-step-7 the kick keys off
        // `_inFlight.Count < maxPending`, so the cap is reachable.
        var gated = new ConcurrencyTrackingGatedProvider(new InMemoryWalStorageProvider());
        var grain = await CreateGrainAsync(gated, new LatticeOptions
        {
            WalMaxPendingBatches = 4,
        });

        var t1 = grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        var t2 = grain.AppendAsync(MakeEntry("b"), CancellationToken.None);
        var t3 = grain.AppendAsync(MakeEntry("c"), CancellationToken.None);
        var t4 = grain.AppendAsync(MakeEntry("d"), CancellationToken.None);

        // The first append's flush is gated; callers 2..4 each find
        // _inFlight.Count < 4 and kick their own flush. All four must
        // observe the provider concurrently before any of them
        // completes - i.e. the cap is reachable under steady fan-in
        // even though no per-batch cap is hit.
        await gated.WaitForActiveAsync(4, TimeSpan.FromSeconds(5));
        Assert.That(gated.PeakActive, Is.EqualTo(4));

        gated.Open();
        var offsets = await Task.WhenAll(t1, t2, t3, t4);
        Assert.That(offsets, Is.EqualTo(new[] { 0L, 1L, 2L, 3L }));
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
    /// Provider that forwards to an inner provider for every batch
    /// whose offset window does not contain <c>failOnOffset</c>, and
    /// throws an <see cref="InvalidOperationException"/> for the
    /// <em>first</em> batch that does. Subsequent batches covering
    /// the same offset (e.g. the grain's post-failure resync followed
    /// by a retry append) pass through to <paramref name="inner"/>.
    /// <para>
    /// Before throwing, the provider waits until every offset
    /// strictly below <c>failOnOffset</c> is committed in the inner
    /// provider, so the predecessor slots' <c>FlushAsync</c>
    /// coroutines have already <c>TrySetResult</c>-ed their TCSs by
    /// the time the grain's failure handler latches
    /// <c>_stickyFailure</c>. The wait-for-predecessors ordering
    /// matches the test's stated intent ("t1+t2 commit cleanly; t3
    /// throws") and is stable under concurrent multi-batch flushes,
    /// where the threadpool-driven arrival order at
    /// <c>AppendBatchAsync</c> is otherwise non-deterministic.
    /// </para>
    /// </summary>
    private sealed class FailOnOffsetAppendProvider(IWalStorageProvider inner, long failOnOffset, string message) : IWalStorageProvider
    {
        private int _fired;

        public async Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
        {
            var fails = false;
            for (var i = 0; i < entries.Count; i++)
            {
                if (entries[i].Offset == failOnOffset)
                {
                    fails = true;
                    break;
                }
            }
            if (fails && Interlocked.CompareExchange(ref _fired, 1, 0) == 0)
            {
                // Wait until every offset strictly below failOnOffset
                // is committed in the inner provider before throwing.
                var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
                while (DateTime.UtcNow < deadline)
                {
                    var head = await inner.GetHighestOffsetAsync(treeId, shardIndex, cancellationToken).ConfigureAwait(false);
                    if (head >= failOnOffset - 1)
                    {
                        break;
                    }
                    await Task.Delay(5, cancellationToken).ConfigureAwait(false);
                }
                throw new InvalidOperationException(message);
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