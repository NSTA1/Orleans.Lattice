using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Tests for the diagnostic pack: the <c>WalFlushPreflightTimeout</c>
/// deadline that bounds the synchronous setup + scheduler yield region of
/// <see cref="WalShardGrain"/>.<c>FlushAsync</c>, and the deactivation-time
/// <c>WalShardDeactivateInFlight</c> histogram that records the in-flight
/// slot count at <c>OnDeactivateAsync</c>.
/// </summary>
public partial class WalShardGrainTests
{
    /// <summary>
    /// Captures every measurement reported on the <see cref="LatticeMetrics.Meter"/>
    /// instrument set, scoped to a single test via <see cref="IDisposable"/>.
    /// Filters by instrument name on read so each test only pays attention to
    /// the counter/histogram it cares about.
    /// </summary>
    private sealed class MeterCapture : IDisposable
    {
        private readonly MeterListener _listener;
        public ConcurrentBag<(string Name, double Value, KeyValuePair<string, object?>[] Tags)> Records { get; } = new();

        public MeterCapture()
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
    /// Provider whose <c>AppendBatchAsync</c> never returns: blocks on a
    /// <see cref="TaskCompletionSource"/> that the test never sets. Models
    /// a partition wedged behind a stuck Azure-Tables call so the grain's
    /// in-flight chain accumulates and the deactivation hook can observe
    /// it.
    /// </summary>
    private sealed class HangingForeverWalStorageProvider : IWalStorageProvider
    {
        public readonly TaskCompletionSource Released =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
        {
            // Honour cancellation so the WalFlushTimeout deadline can wake
            // the await and the slot can drain at end-of-test; otherwise
            // the test process would hang at teardown.
            using var reg = cancellationToken.Register(() => Released.TrySetResult());
            await Released.Task.ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
        }

        public IAsyncEnumerable<WalEntry> ReadAsync(string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, CancellationToken cancellationToken)
            => AsyncEnumerable.Empty();

        public Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => Task.FromResult(-1L);

        public Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => Task.FromResult(-1L);

        public Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken)
            => Task.CompletedTask;

        private static class AsyncEnumerable
        {
            public static async IAsyncEnumerable<WalEntry> Empty()
            {
                await Task.CompletedTask;
                yield break;
            }
        }
    }

    [Test]
    public async Task OnDeactivateAsync_records_zero_inflight_when_chain_is_clean()
    {
        using var capture = new MeterCapture();
        var grain = await CreateGrainAsync();

        // No appends have run; _inFlight is empty.
        await grain.OnDeactivateAsync(new DeactivationReason(DeactivationReasonCode.None, "test"), CancellationToken.None)
            .WaitAsync(TimeSpan.FromSeconds(5));

        var observation = capture.FirstFor("orleans.lattice.wal.shard.deactivate.in_flight");
        Assert.That(observation, Is.Not.Null, "deactivate.in_flight observation must be emitted exactly once per deactivation");
        Assert.That(observation!.Value.Value, Is.EqualTo(0L));
        Assert.That(
            observation.Value.Tags.Any(t => t.Key == LatticeMetrics.TagTree && (string?)t.Value == TreeId),
            Is.True,
            "observation must be tagged with the deactivating tree id");
        Assert.That(
            observation.Value.Tags.Any(t => t.Key == LatticeMetrics.TagShard && (int?)t.Value == ShardIndex),
            Is.True,
            "observation must be tagged with the deactivating shard index");
    }

    [Test]
    public async Task OnDeactivateAsync_records_nonzero_inflight_when_provider_is_stuck()
    {
        using var capture = new MeterCapture();
        var provider = new HangingForeverWalStorageProvider();
        var grain = await CreateGrainAsync(
            provider,
            new LatticeOptions
            {
                WalMaxBatchEntries = 1,
                WalMaxPendingBatches = 1,
                // Disable preflight so the flush is committed in-flight
                // and the deactivation hook observes the pinned slot.
                WalFlushPreflightTimeout = Timeout.InfiniteTimeSpan,
                // Bound the flush so the grain can drain at end-of-test.
                WalFlushTimeout = TimeSpan.FromMilliseconds(200),
            });

        // Kick an append; the provider hangs so this append never returns.
        // The flush task carries the slot into _inFlight and parks on the
        // hanging provider. We don't await the append - we just need the
        // slot present at deactivation time.
        var append = grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        await Task.Delay(50);
        Assert.That(append.IsCompleted, Is.False, "append must still be in flight when we deactivate");

        // Deactivation must record the in-flight slot BEFORE attempting
        // to drain. The WalFlushTimeout will subsequently fire so the
        // drain itself completes within the test budget.
        var deactivate = grain.OnDeactivateAsync(new DeactivationReason(DeactivationReasonCode.None, "test"), CancellationToken.None);
        // Give the deactivation hook a tick to record the observation
        // before the drain task races to remove the slot.
        await Task.Delay(20);

        // Release the provider so the flush deadline can complete the drain.
        provider.Released.TrySetResult();
        try { await append.WaitAsync(TimeSpan.FromSeconds(5)); } catch { /* expected fault */ }
        await deactivate.WaitAsync(TimeSpan.FromSeconds(5));

        var observation = capture.FirstFor("orleans.lattice.wal.shard.deactivate.in_flight");
        Assert.That(observation, Is.Not.Null);
        Assert.That(observation!.Value.Value, Is.GreaterThanOrEqualTo(1L),
            "non-zero observation is the smoking-gun for the mid-call deactivation orphan hypothesis");
    }

    [Test]
    public async Task FlushAsync_infinite_preflight_preserves_historical_behaviour()
    {
        using var capture = new MeterCapture();
        var grain = await CreateGrainAsync(
            options: new LatticeOptions
            {
                WalFlushPreflightTimeout = Timeout.InfiniteTimeSpan,
            });

        var offset = await grain.AppendAsync(MakeEntry("a"), CancellationToken.None)
            .WaitAsync(TimeSpan.FromSeconds(5));

        Assert.That(offset, Is.EqualTo(0L));
        Assert.That(capture.Count("orleans.lattice.wal.flush.preflight.timeouts"), Is.EqualTo(0L),
            "infinite preflight must never trip the deadline counter");
    }

    // ----------------------------------------------------------------------
    // Diagnostic pack (per-shard FlushAsync lifecycle + reshard counters)
    // ----------------------------------------------------------------------

    [Test]
    public async Task StartFlush_increments_start_flush_calls_counter_per_invocation()
    {
        using var capture = new MeterCapture();
        var grain = await CreateGrainAsync();

        // A single AppendAsync triggers exactly one StartFlush on the
        // healthy path (one batch, no cap-cutover).
        var offset = await grain.AppendAsync(MakeEntry("a"), CancellationToken.None)
            .WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(offset, Is.EqualTo(0L));

        var startFlushCalls = capture.Count("orleans.lattice.wal.shard.start_flush.calls");
        Assert.That(startFlushCalls, Is.GreaterThanOrEqualTo(1L),
            "StartFlush counter diagnostic: at least one StartFlush invocation must be observed for a single AppendAsync");
        var sample = capture.FirstFor("orleans.lattice.wal.shard.start_flush.calls");
        Assert.That(sample, Is.Not.Null);
        Assert.That(
            sample!.Value.Tags.Any(t => t.Key == LatticeMetrics.TagTree && (string?)t.Value == TreeId),
            Is.True,
            "StartFlush counter diagnostic: start_flush.calls must be tagged with the affected tree id");
        Assert.That(
            sample.Value.Tags.Any(t => t.Key == LatticeMetrics.TagShard && (int?)t.Value == ShardIndex),
            Is.True,
            "StartFlush counter diagnostic: start_flush.calls must be tagged with the affected shard index");
    }

    [Test]
    public async Task StartFlush_records_pending_segments_histogram_per_invocation()
    {
        using var capture = new MeterCapture();
        var grain = await CreateGrainAsync();

        var offset = await grain.AppendAsync(MakeEntry("a"), CancellationToken.None)
            .WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(offset, Is.EqualTo(0L));

        var pendingSegments = capture.FirstFor("orleans.lattice.wal.shard.pending_segments");
        Assert.That(pendingSegments, Is.Not.Null,
            "StartFlush counter diagnostic: pending_segments observation must be emitted at StartFlush entry");
        Assert.That(pendingSegments!.Value.Value, Is.EqualTo(1L),
            "StartFlush counter diagnostic: a single-entry AppendAsync captures _pendingSegments.Count=1 at the moment StartFlush runs");
    }

    [Test]
    public void WalFlushStage_enum_layout_is_contract_with_StallWatchdog()
    {
        // lifecycle-stage diagnostic: the StallWatchdog reads WalFlushStage as a raw byte
        // via ClrMD and maps it to a stage name through a hardcoded
        // switch (Created=0, Yielded=1, ProviderCallIssued=2, ...). A
        // future enum renumber would silently mislabel every wedged slot
        // in the watchdog log; this contract test catches it.
        var enumType = typeof(Orleans.Lattice.BPlusTree.Grains.WalShardGrain).Assembly
            .GetType("Orleans.Lattice.BPlusTree.Grains.WalShardGrain+WalFlushStage", throwOnError: true)!;
        Assert.That(enumType.GetEnumUnderlyingType(), Is.EqualTo(typeof(byte)),
            "lifecycle-stage diagnostic: WalFlushStage underlying type must be byte (StallWatchdog reads it as byte)");
        var values = Enum.GetValues(enumType).Cast<object>()
            .ToDictionary(v => v.ToString()!, v => (byte)v);
        Assert.Multiple(() =>
        {
            Assert.That(values["Created"], Is.EqualTo((byte)0));
            Assert.That(values["Yielded"], Is.EqualTo((byte)1));
            Assert.That(values["ProviderCallIssued"], Is.EqualTo((byte)2));
            Assert.That(values["ProviderCallReturned"], Is.EqualTo((byte)3));
            Assert.That(values["AcksApplied"], Is.EqualTo((byte)4));
            Assert.That(values["FailureHandled"], Is.EqualTo((byte)5));
        });
    }

    [Test]
    public void InFlightFlush_carries_Stage_and_StageStartedTicks_fields()
    {
        // lifecycle-stage diagnostic: the StallWatchdog walks the heap for InFlightFlush
        // instances and reads Stage / StageStartedTicks by name via
        // ClrMD. A field rename (refactor or accidental) would silently
        // break the watchdog's wedged-slot attribution; this contract
        // test catches it.
        var inFlightType = typeof(Orleans.Lattice.BPlusTree.Grains.WalShardGrain).Assembly
            .GetType("Orleans.Lattice.BPlusTree.Grains.WalShardGrain+InFlightFlush", throwOnError: true)!;
        var stageField = inFlightType.GetField("Stage", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
        var stageStartedField = inFlightType.GetField("StageStartedTicks", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
        Assert.That(stageField, Is.Not.Null, "lifecycle-stage diagnostic: InFlightFlush must carry a Stage field of type WalFlushStage");
        Assert.That(stageStartedField, Is.Not.Null, "lifecycle-stage diagnostic: InFlightFlush must carry a StageStartedTicks field of type long");
        Assert.That(stageField!.FieldType.Name, Is.EqualTo("WalFlushStage"));
        Assert.That(stageStartedField!.FieldType, Is.EqualTo(typeof(long)));
    }
}
