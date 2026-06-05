using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Tests for the deactivation drain budget (the AC1 half of the
/// drain-wedge-under-storage-account-back-pressure investigation).
/// The drain path must settle within a bounded window of the SIGTERM
/// regardless of whether the underlying storage provider is healthy:
/// (1) the drain-cancellation token signals every in-flight provider
///     call at drain entry so a co-operative provider gives up
///     promptly,
/// (2) the per-shard <c>LatticeOptions.WalDrainBudget</c> ceiling
///     force-faults any slot that has not unlinked within the budget
///     so the activation can finish tearing down, and
/// (3) the matching counter / histogram
///     (orleans.lattice.wal.shard.drain.budget.expirations,
///     orleans.lattice.wal.shard.drain.budget.force_faulted_slots)
///     attribute the trip per <c>(tree, shard)</c>.
/// </summary>
public partial class WalShardGrainTests
{
    /// <summary>
    /// Captures every measurement reported on the <see cref="LatticeMetrics.Meter"/>
    /// instrument set, scoped to a single test via <see cref="IDisposable"/>.
    /// Local copy so this partial file is self-contained against any
    /// future re-ordering of the sibling diagnostic file.
    /// </summary>
    private sealed class DrainBudgetMeterCapture : IDisposable
    {
        private readonly MeterListener _listener;
        public ConcurrentBag<(string Name, double Value, KeyValuePair<string, object?>[] Tags)> Records { get; } = new();

        public DrainBudgetMeterCapture()
        {
            _listener = new MeterListener
            {
                InstrumentPublished = (inst, l) =>
                {
                    if (ReferenceEquals(inst.Meter, LatticeMetrics.Meter))
                    {
                        l.EnableMeasurementEvents(inst);
                    }
                }
            };
            _listener.SetMeasurementEventCallback<long>(
                (inst, value, tags, _) => Records.Add((inst.Name, value, tags.ToArray())));
            _listener.SetMeasurementEventCallback<double>(
                (inst, value, tags, _) => Records.Add((inst.Name, value, tags.ToArray())));
            _listener.Start();
        }

        public long Count(string instrumentName) =>
            Records.Where(r => r.Name == instrumentName).Sum(r => (long)r.Value);

        public (long Value, KeyValuePair<string, object?>[] Tags)? FirstFor(string instrumentName)
        {
            var hit = Records.FirstOrDefault(r => r.Name == instrumentName);
            return hit == default ? null : ((long)hit.Value, hit.Tags);
        }

        public void Dispose() => _listener.Dispose();
    }

    /// <summary>
    /// Provider whose <c>AppendBatchAsync</c> blocks until cancelled
    /// (honours the cancellation token by parking on a registered
    /// callback). Models a co-operative storage provider whose call
    /// the drain-cancellation token can break - the AC1 baseline case
    /// where the drain entry cancels the in-flight provider call and
    /// the chain settles naturally inside the drain budget.
    /// </summary>
    private sealed class CooperativeHangingProvider : IWalStorageProvider
    {
        public int AppendInvocations;

        public async Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref AppendInvocations);
            await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
        }

        public IAsyncEnumerable<WalEntry> ReadAsync(string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, CancellationToken cancellationToken)
            => DrainBudgetAsyncEnumerable.Empty();
        public Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken) => Task.FromResult(-1L);
        public Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken) => Task.FromResult(-1L);
        public Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    /// <summary>
    /// Provider whose <c>AppendBatchAsync</c> AND post-failure resync
    /// (<c>GetHighestOffsetAsync</c> after the activation read) block on
    /// a <see cref="TaskCompletionSource"/> that ignores the
    /// cancellation token. Models the saturating-Azure-Tables wedge
    /// where the SDK retry loop is in pre-attempt back-off; the
    /// per-flush <c>WalFlushTimeout</c> cancellation has no observable
    /// seam to observe, the FlushAsync catch handler's awaited resync
    /// hangs the same way, and the FlushAsync task itself never
    /// completes. The drain budget must force-fault the slot regardless.
    /// <para>
    /// The first <c>GetHighestOffsetAsync</c> call (the
    /// <c>OnActivateAsync</c> /
    /// <c>InitializeForTestingAsync</c> initialisation read) completes
    /// promptly so the test seam can stand the grain up; subsequent
    /// calls (the post-failure resync) hang.
    /// </para>
    /// </summary>
    private sealed class UncancellableHangingProvider : IWalStorageProvider
    {
        public readonly TaskCompletionSource Released =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int AppendInvocations;
        private int _getHighestInvocations;

        public async Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref AppendInvocations);
            // The wait does not observe the cancellation token, so the
            // grain's own bound on the await must be what releases the
            // slot. The parked task is never released by the test (the
            // grain abandons it on the deadline / drain budget) and is
            // harmlessly collected at process exit.
            await Released.Task.ConfigureAwait(false);
        }

        public IAsyncEnumerable<WalEntry> ReadAsync(string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, CancellationToken cancellationToken)
            => DrainBudgetAsyncEnumerable.Empty();

        public async Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
        {
            // First call (activation initialisation) returns promptly so
            // the test seam can stand the grain up. Subsequent calls
            // (the post-failure resync) hang without honouring
            // cancellation; the FlushAsync catch handler's awaited
            // HandleFlushFailureAsync would otherwise complete quickly
            // and the chain would drain naturally well inside the
            // budget. With this hang the FlushAsync task stays alive
            // past the budget and the force-fault path is the only
            // way out.
            if (Interlocked.Increment(ref _getHighestInvocations) == 1)
            {
                return -1L;
            }
            await Released.Task.ConfigureAwait(false);
            return -1L;
        }

        public Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken) => Task.FromResult(-1L);
        public Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private static class DrainBudgetAsyncEnumerable
    {
        public static async IAsyncEnumerable<WalEntry> Empty()
        {
            await Task.CompletedTask;
            yield break;
        }
    }

    [Test]
    public async Task OnDeactivateAsync_cancels_in_flight_cooperative_provider_call_at_drain_entry()
    {
        // Drive a flush on a cooperative provider that observes the
        // cancellation token; the drain-entry cancellation must release
        // the provider call BEFORE the per-flush WalFlushTimeout expires
        // (or the drain budget expires). The await on the parked
        // AppendAsync must therefore fault with TimeoutException routed
        // through the normal failure handler.
        var provider = new CooperativeHangingProvider();
        var grain = await CreateGrainAsync(provider, new LatticeOptions
        {
            WalMaxBatchEntries = 1,
            WalMaxPendingBatches = 1,
            // Keep the per-flush deadline LONG so the test proves the
            // drain-cancellation arm released the call (not the
            // per-flush WalFlushTimeout).
            WalFlushTimeout = TimeSpan.FromSeconds(60),
            WalFlushPreflightTimeout = Timeout.InfiniteTimeSpan,
            // Bound the drain so the test terminates deterministically.
            WalDrainBudget = TimeSpan.FromSeconds(5),
        });

        var append = grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        await Task.Delay(50);
        Assert.That(append.IsCompleted, Is.False);
        Assert.That(provider.AppendInvocations, Is.GreaterThanOrEqualTo(1));

        var deactivateStart = System.Diagnostics.Stopwatch.GetTimestamp();
        await grain.OnDeactivateAsync(new DeactivationReason(DeactivationReasonCode.ApplicationRequested, "test"), CancellationToken.None)
            .WaitAsync(TimeSpan.FromSeconds(10));
        var deactivateMs = System.Diagnostics.Stopwatch.GetElapsedTime(deactivateStart).TotalMilliseconds;

        Assert.That(deactivateMs, Is.LessThan(5_000d),
            "drain entry must cancel the cooperative provider call promptly; total drain must complete well inside the per-flush WalFlushTimeout");

        // The append's TCS must be faulted (the cooperative cancellation
        // path surfaces TimeoutException via the FlushAsync deadline
        // catch).
        Assert.That(async () => await append, Throws.InstanceOf<TimeoutException>());
    }

    [Test]
    public async Task OnDeactivateAsync_force_faults_in_flight_slot_when_drain_budget_expires()
    {
        // Drive a flush on a provider whose await ignores cancellation,
        // then deactivate. The drain-cancellation arm cannot release
        // the call (the SDK is parked in pre-attempt back-off); the
        // drain budget must trip and force-fault the slot so callers
        // are released and OnDeactivateAsync returns.
        using var capture = new DrainBudgetMeterCapture();
        var provider = new UncancellableHangingProvider();
        var grain = await CreateGrainAsync(provider, new LatticeOptions
        {
            WalMaxBatchEntries = 1,
            WalMaxPendingBatches = 1,
            // Tight drain budget so the test runs fast; the per-flush
            // WalFlushTimeout is intentionally longer than the drain
            // budget so the budget is what fires.
            WalDrainBudget = TimeSpan.FromMilliseconds(200),
            WalFlushTimeout = TimeSpan.FromSeconds(30),
            WalFlushPreflightTimeout = Timeout.InfiniteTimeSpan,
        });

        var append = grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        await Task.Delay(50);
        Assert.That(append.IsCompleted, Is.False);

        // The drain budget must fire well before the per-flush
        // WalFlushTimeout would; the deactivation returns within the
        // budget plus a small grace.
        await grain.OnDeactivateAsync(new DeactivationReason(DeactivationReasonCode.ApplicationRequested, "test"), CancellationToken.None)
            .WaitAsync(TimeSpan.FromSeconds(5));

        // The append's TCS must be faulted with TimeoutException; the
        // bench's FINAL accounting depends on this surface.
        Assert.That(async () => await append, Throws.InstanceOf<TimeoutException>());

        // Counter and histogram must fire exactly once each, both
        // tagged with the affected (tree, shard).
        Assert.That(capture.Count("orleans.lattice.wal.shard.drain.budget.expirations"), Is.EqualTo(1L),
            "drain-budget gate: expiration counter must fire exactly once per wedged drain");
        var forceFaulted = capture.FirstFor("orleans.lattice.wal.shard.drain.budget.force_faulted_slots");
        Assert.That(forceFaulted, Is.Not.Null,
            "drain-budget gate: force-faulted-slots histogram must fire when the drain budget expires");
        Assert.That(forceFaulted!.Value.Value, Is.GreaterThanOrEqualTo(1L),
            "drain-budget gate: at least one slot must be force-faulted on budget expiry");

        // Both observations must carry the (tree, shard) tags.
        var counterTags = capture.FirstFor("orleans.lattice.wal.shard.drain.budget.expirations")!.Value.Tags;
        Assert.That(counterTags.Any(t => t.Key == LatticeMetrics.TagTree && (string?)t.Value == TreeId), Is.True,
            "drain-budget gate: expiration counter must be tagged with the affected tree id");
        Assert.That(counterTags.Any(t => t.Key == LatticeMetrics.TagShard && (int?)t.Value == ShardIndex), Is.True,
            "drain-budget gate: expiration counter must be tagged with the affected shard index");
        Assert.That(forceFaulted.Value.Tags.Any(t => t.Key == LatticeMetrics.TagTree && (string?)t.Value == TreeId), Is.True,
            "drain-budget gate: force-faulted-slots histogram must be tagged with the affected tree id");
        Assert.That(forceFaulted.Value.Tags.Any(t => t.Key == LatticeMetrics.TagShard && (int?)t.Value == ShardIndex), Is.True,
            "drain-budget gate: force-faulted-slots histogram must be tagged with the affected shard index");

        // Tidy up the parked provider task so it does not leak into
        // GC's freachable queue beyond the test - releasing the TCS
        // lets the grain's abandoned WaitAsync complete naturally.
        provider.Released.TrySetResult();
    }

    [Test]
    public async Task OnDeactivateAsync_does_not_fire_drain_budget_counters_when_chain_drains_naturally()
    {
        // The healthy steady-state shape: no in-flight flushes at
        // deactivation, so the drain budget never has a slot to
        // force-fault. The counter must stay at zero.
        using var capture = new DrainBudgetMeterCapture();
        var grain = await CreateGrainAsync(options: new LatticeOptions
        {
            // Tight drain budget would still not fire because there
            // is nothing to drain.
            WalDrainBudget = TimeSpan.FromMilliseconds(100),
        });

        await grain.OnDeactivateAsync(new DeactivationReason(DeactivationReasonCode.None, "test"), CancellationToken.None)
            .WaitAsync(TimeSpan.FromSeconds(5));

        Assert.That(capture.Count("orleans.lattice.wal.shard.drain.budget.expirations"), Is.EqualTo(0L),
            "drain budget counter must stay at zero when no in-flight slots are present at deactivation");
        Assert.That(capture.FirstFor("orleans.lattice.wal.shard.drain.budget.force_faulted_slots"), Is.Null,
            "force-faulted-slots histogram must not fire when no in-flight slots are present at deactivation");
    }

    [Test]
    public async Task OnDeactivateAsync_infinite_drain_budget_preserves_unbounded_drain()
    {
        // With the budget disabled the deactivation honours the
        // historical unbounded-drain shape: the cooperative provider's
        // cancellation seam still releases the call promptly through
        // the drain-CTS path, but no force-fault counter ever fires.
        using var capture = new DrainBudgetMeterCapture();
        var provider = new CooperativeHangingProvider();
        var grain = await CreateGrainAsync(provider, new LatticeOptions
        {
            WalMaxBatchEntries = 1,
            WalMaxPendingBatches = 1,
            WalFlushTimeout = TimeSpan.FromSeconds(30),
            WalFlushPreflightTimeout = Timeout.InfiniteTimeSpan,
            WalDrainBudget = Timeout.InfiniteTimeSpan,
        });

        var append = grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        await Task.Delay(50);
        Assert.That(append.IsCompleted, Is.False);

        await grain.OnDeactivateAsync(new DeactivationReason(DeactivationReasonCode.ApplicationRequested, "test"), CancellationToken.None)
            .WaitAsync(TimeSpan.FromSeconds(10));

        // The cooperative provider released the call via the drain-CTS
        // link; the append surfaces TimeoutException.
        Assert.That(async () => await append, Throws.InstanceOf<TimeoutException>());

        // No force-fault: the chain drained naturally before any
        // budget arm could fire.
        Assert.That(capture.Count("orleans.lattice.wal.shard.drain.budget.expirations"), Is.EqualTo(0L),
            "infinite drain budget must never trip the expiration counter");
    }

    [Test]
    public async Task OnDeactivateAsync_returns_within_bounded_time_of_drain_budget_under_uncancellable_wedge()
    {
        // The primary acceptance contract: OnDeactivateAsync MUST return
        // within bounded time of the SIGTERM regardless of whether the
        // underlying provider is healthy. The previous tests assert the
        // TCS shape and counter fire; this test asserts the time bound
        // directly (which is the only property the host's FINAL
        // accounting actually cares about).
        var provider = new UncancellableHangingProvider();
        var grain = await CreateGrainAsync(provider, new LatticeOptions
        {
            WalMaxBatchEntries = 1,
            WalMaxPendingBatches = 1,
            WalDrainBudget = TimeSpan.FromMilliseconds(250),
            WalFlushTimeout = TimeSpan.FromSeconds(30),
            WalFlushPreflightTimeout = Timeout.InfiniteTimeSpan,
        });

        var append = grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        await Task.Delay(50);
        Assert.That(append.IsCompleted, Is.False);

        var startTicks = System.Diagnostics.Stopwatch.GetTimestamp();
        await grain.OnDeactivateAsync(new DeactivationReason(DeactivationReasonCode.ApplicationRequested, "test"), CancellationToken.None)
            .WaitAsync(TimeSpan.FromSeconds(5));
        var elapsedMs = System.Diagnostics.Stopwatch.GetElapsedTime(startTicks).TotalMilliseconds;

        // Bounded time contract: drain budget + generous threadpool
        // grace. Empirically the drain returns within ~50ms of the
        // budget; we assert 4x the budget as the upper bound so
        // overloaded CI workers do not flake the test.
        Assert.That(elapsedMs, Is.LessThan(1_000d),
            "OnDeactivateAsync must return within bounded time of the SIGTERM (budget + grace), regardless of provider health");

        provider.Released.TrySetResult();
    }

    [Test]
    public async Task OnDeactivateAsync_force_faults_every_in_flight_slot_when_drain_budget_expires_with_higher_cap()
    {
        // The histogram observation must record the actual number of
        // force-faulted slots, not just >= 1. With WalMaxPendingBatches
        // = 3 and three in-flight flushes, every one should be force-
        // faulted in a single drain expiration. This catches a
        // regression where the chain-clear logic only handles the head
        // slot.
        using var capture = new DrainBudgetMeterCapture();
        var provider = new UncancellableHangingProvider();
        var grain = await CreateGrainAsync(provider, new LatticeOptions
        {
            WalMaxBatchEntries = 1,
            WalMaxPendingBatches = 3,
            WalDrainBudget = TimeSpan.FromMilliseconds(200),
            WalFlushTimeout = TimeSpan.FromSeconds(30),
            WalFlushPreflightTimeout = Timeout.InfiniteTimeSpan,
        });

        var t1 = grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        var t2 = grain.AppendAsync(MakeEntry("b"), CancellationToken.None);
        var t3 = grain.AppendAsync(MakeEntry("c"), CancellationToken.None);
        // Yield long enough for all three flushes to be in flight.
        await Task.Delay(80);
        Assert.That(t1.IsCompleted, Is.False);
        Assert.That(t2.IsCompleted, Is.False);
        Assert.That(t3.IsCompleted, Is.False);

        await grain.OnDeactivateAsync(new DeactivationReason(DeactivationReasonCode.ApplicationRequested, "test"), CancellationToken.None)
            .WaitAsync(TimeSpan.FromSeconds(5));

        // Every parked append must surface a TimeoutException.
        Assert.That(async () => await t1, Throws.InstanceOf<TimeoutException>());
        Assert.That(async () => await t2, Throws.InstanceOf<TimeoutException>());
        Assert.That(async () => await t3, Throws.InstanceOf<TimeoutException>());

        // The counter fires exactly once per drain (not per slot).
        Assert.That(capture.Count("orleans.lattice.wal.shard.drain.budget.expirations"), Is.EqualTo(1L),
            "drain-budget gate: expiration counter must fire exactly once per drain regardless of slot count");

        // The histogram records the slot count force-faulted in this
        // drain. With three slots in flight, the recorded value must
        // be >= 1 (some slots may have settled naturally before the
        // budget tripped); a healthy implementation faults all three
        // in one shot.
        var forceFaulted = capture.FirstFor("orleans.lattice.wal.shard.drain.budget.force_faulted_slots");
        Assert.That(forceFaulted, Is.Not.Null);
        Assert.That(forceFaulted!.Value.Value, Is.GreaterThanOrEqualTo(1L),
            "drain-budget gate: at least one slot must be force-faulted; ideally all three");
        Assert.That(forceFaulted.Value.Value, Is.LessThanOrEqualTo(3L),
            "drain-budget gate: cannot force-fault more slots than were in flight");

        provider.Released.TrySetResult();
    }

    [Test]
    public async Task OnDeactivateAsync_force_faulted_TimeoutException_message_names_WalDrainBudget()
    {
        // The TimeoutException faulted onto each parked TCS must
        // identify the WalDrainBudget option by name so operators
        // grepping the silo log can attribute the trip without
        // source-walking.
        var provider = new UncancellableHangingProvider();
        var grain = await CreateGrainAsync(provider, new LatticeOptions
        {
            WalMaxBatchEntries = 1,
            WalMaxPendingBatches = 1,
            WalDrainBudget = TimeSpan.FromMilliseconds(150),
            WalFlushTimeout = TimeSpan.FromSeconds(30),
            WalFlushPreflightTimeout = Timeout.InfiniteTimeSpan,
        });

        var append = grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        await Task.Delay(50);

        await grain.OnDeactivateAsync(new DeactivationReason(DeactivationReasonCode.ApplicationRequested, "test"), CancellationToken.None)
            .WaitAsync(TimeSpan.FromSeconds(5));

        Assert.That(
            async () => await append,
            Throws.InstanceOf<TimeoutException>()
                .With.Message.Contains(nameof(LatticeOptions.WalDrainBudget)),
            "the surfaced TimeoutException must name WalDrainBudget so operators can attribute the trip");

        provider.Released.TrySetResult();
    }

    [Test]
    public async Task OnDeactivateAsync_drain_signals_drain_CTS_so_cooperative_provider_observes_cancellation_mid_flight()
    {
        // The drain-CTS link must reach the in-flight provider call so
        // a cooperative provider's await on its CancellationToken fires.
        // Asserts the provider observed cancellation (its cancellation
        // token was signalled) BEFORE the drain budget elapsed.
        var observedCancellation = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var provider = new TokenObservingProvider(observedCancellation);
        var grain = await CreateGrainAsync(provider, new LatticeOptions
        {
            WalMaxBatchEntries = 1,
            WalMaxPendingBatches = 1,
            // Long budget; the test asserts the cancellation arrives
            // promptly (well inside the budget), not at the budget
            // boundary.
            WalDrainBudget = TimeSpan.FromSeconds(5),
            WalFlushTimeout = TimeSpan.FromSeconds(60),
            WalFlushPreflightTimeout = Timeout.InfiniteTimeSpan,
        });

        var append = grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        await Task.Delay(50);
        Assert.That(append.IsCompleted, Is.False);

        // Start the deactivation; the drain entry must signal the
        // drain CTS which propagates through the linked deadline to
        // the provider's cancellation token.
        var deactivate = grain.OnDeactivateAsync(new DeactivationReason(DeactivationReasonCode.ApplicationRequested, "test"), CancellationToken.None);

        // The provider must observe cancellation within a small window
        // of the deactivation start, well before the drain budget.
        var observedTask = observedCancellation.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.That(async () => await observedTask, Throws.Nothing,
            "the drain-CTS link must reach the in-flight provider call promptly; without the link the provider would only see cancellation when its individual WalFlushTimeout fires");

        await deactivate.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(async () => await append, Throws.InstanceOf<TimeoutException>());
    }

    /// <summary>
    /// Provider whose <c>AppendBatchAsync</c> registers a callback on
    /// the supplied cancellation token; firing the callback signals
    /// the test that cancellation propagated. The await then surfaces
    /// <see cref="OperationCanceledException"/> so the grain's
    /// FlushAsync catch can convert it through the normal failure
    /// handler.
    /// </summary>
    private sealed class TokenObservingProvider(TaskCompletionSource<bool> observedCancellation) : IWalStorageProvider
    {
        public async Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
        {
            using var reg = cancellationToken.Register(() => observedCancellation.TrySetResult(true));
            await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
        }

        public IAsyncEnumerable<WalEntry> ReadAsync(string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, CancellationToken cancellationToken)
            => DrainBudgetAsyncEnumerable.Empty();
        public Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken) => Task.FromResult(-1L);
        public Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken) => Task.FromResult(-1L);
        public Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
