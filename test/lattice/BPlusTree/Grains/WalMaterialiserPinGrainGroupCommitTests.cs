using System.Collections.Concurrent;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Interleaved reports and group-committed birth seeds on the pin store (issue #6166). Every
/// test drives the grain on one exclusive <see cref="TaskScheduler"/>, the turn model an
/// activation gives it: a call suspended on a storage write lets the next call start, as an
/// <c>[AlwaysInterleave]</c> method does.
/// </summary>
[TestFixture]
public sealed class WalMaterialiserPinGrainGroupCommitTests
{
    private const string Tree = "tree-6166";

    private static Task OnTurnAsync(TaskScheduler scheduler, Func<Task> body) =>
        Task.Factory.StartNew(body, CancellationToken.None, TaskCreationOptions.None, scheduler).Unwrap();

    private static MaterialiserPinReport Seed(string consumerId) =>
        new(consumerId, HybridLogicalClock.Zero, -1);

    private static WalMaterialiserPinGrain CreateGrain(
        FakePersistentState<WalMaterialiserPinState> legacy,
        int buckets,
        IGrainStorage? pinStorage,
        TimeSpan? pinLatencyThreshold = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("wal-materialiser-pin", Tree));
        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeOptions
        {
            WalMaterialiserPinBuckets = buckets,
            WalMaterialiserPinFlushIntervalMs = 0,
            WalSaturationMaterialiserPinLatencyThreshold = pinLatencyThreshold,
        });
        return new WalMaterialiserPinGrain(context, legacy, options, logger: null, pinStorage: pinStorage);
    }

    [Test]
    public async Task Seeds_arriving_while_a_seed_write_is_in_flight_share_the_next_write()
    {
        var turn = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;
        var legacy = new FakePersistentState<WalMaterialiserPinState>();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        legacy.BeforeWrite = () => gate.Task;
        var grain = CreateGrain(legacy, buckets: 1, pinStorage: null);
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        var first = OnTurnAsync(turn, () => grain.SeedManyAsync(new[] { Seed("leaf-0") }));
        await OnTurnAsync(turn, () => Task.CompletedTask);
        var later = Enumerable.Range(1, 6)
            .Select(i => OnTurnAsync(turn, () => grain.SeedManyAsync(new[] { Seed($"leaf-{i}") })))
            .ToArray();
        await OnTurnAsync(turn, () => Task.CompletedTask);
        Assert.That(first.IsCompleted, Is.False, "precondition: the first seed's write is still in flight");

        gate.SetResult();
        await first;
        await Task.WhenAll(later);

        Assert.Multiple(() =>
        {
            Assert.That(legacy.MaxConcurrentWrites, Is.EqualTo(1), "the shard never writes concurrently");
            Assert.That(legacy.WriteCount, Is.EqualTo(2), "the seeds that waited share one write rather than queueing one each");
            Assert.That(legacy.State.Pins.Keys, Is.EquivalentTo(Enumerable.Range(0, 7).Select(i => $"leaf-{i}")),
                "every seed is durable when it returns");
        });
    }

    [Test]
    public async Task A_report_merged_while_its_slot_is_being_written_is_still_made_durable()
    {
        var turn = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;
        var store = new GatedSlotStore();
        var legacy = new FakePersistentState<WalMaterialiserPinState>();
        var grain = CreateGrain(legacy, buckets: 8, pinStorage: store);
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        const string seeded = "leaf-seeded";
        var slot = WalMaterialiserPinRouting.BucketStateName(seeded, 8);
        var reported = Enumerable.Range(0, 1000).Select(i => $"leaf-{i}")
            .First(id => WalMaterialiserPinRouting.BucketStateName(id, 8) == slot);
        var frontier = new HybridLogicalClock { WallClockTicks = 500 };

        store.Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var seed = OnTurnAsync(turn, () => grain.SeedManyAsync(new[] { Seed(seeded) }));
        await OnTurnAsync(turn, () => Task.CompletedTask);
        var report = OnTurnAsync(turn, () => grain.ReportAsync(reported, frontier));
        await OnTurnAsync(turn, () => Task.CompletedTask);
        Assert.That(seed.IsCompleted, Is.False, "precondition: the seed's slot write is still in flight");

        store.Gate.SetResult();
        await seed;
        await report;

        Assert.That(store.Durable(slot)?.Pins.GetValueOrDefault(reported), Is.EqualTo(frontier),
            "the slot write the report overlapped must not clear the report's dirty mark");
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task A_report_waits_for_a_write_past_the_pin_latency_threshold_and_only_then(bool thresholdSet)
    {
        var turn = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;
        var store = new GatedSlotStore();
        var grain = CreateGrain(
            new FakePersistentState<WalMaterialiserPinState>(),
            buckets: 8,
            pinStorage: store,
            pinLatencyThreshold: thresholdSet ? TimeSpan.Zero : null);
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        await grain.ReportAsync("leaf-reported", new HybridLogicalClock { WallClockTicks = 100 });

        store.Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var seed = OnTurnAsync(turn, () => grain.SeedManyAsync(new[] { Seed("leaf-seeded") }));
        await OnTurnAsync(turn, () => Task.CompletedTask);
        var report = OnTurnAsync(turn, () => grain.ReportAsync("leaf-reported", new HybridLogicalClock { WallClockTicks = 100 }));
        await OnTurnAsync(turn, () => Task.CompletedTask);

        Assert.That(report.IsCompleted, Is.EqualTo(!thresholdSet),
            "a report answers at once unless the write in flight is past the threshold, which the reporting leaf must then observe");
        store.Gate.SetResult();
        await seed;
        await report;
    }

    /// <summary>A bucket store whose writes wait on <see cref="Gate"/> while it is set.</summary>
    private sealed class GatedSlotStore : IGrainStorage
    {
        private readonly ConcurrentDictionary<string, WalMaterialiserPinState> _slots = new(StringComparer.Ordinal);

        public TaskCompletionSource? Gate { get; set; }

        public WalMaterialiserPinState? Durable(string stateName) =>
            _slots.TryGetValue(stateName, out var state) ? state : null;

        public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            grainState.State = (T)(object)(_slots.TryGetValue(stateName, out var state) ? Clone(state) : new WalMaterialiserPinState());
            grainState.RecordExists = _slots.ContainsKey(stateName);
            return Task.CompletedTask;
        }

        public async Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            var copy = Clone((WalMaterialiserPinState)(object)grainState.State!);
            if (Gate is { } gate)
            {
                await gate.Task;
            }

            _slots[stateName] = copy;
            grainState.RecordExists = true;
        }

        public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            _slots.TryRemove(stateName, out _);
            return Task.CompletedTask;
        }

        private static WalMaterialiserPinState Clone(WalMaterialiserPinState state) => new()
        {
            Pins = new Dictionary<string, HybridLogicalClock>(state.Pins, StringComparer.Ordinal),
            Offsets = new Dictionary<string, long>(state.Offsets, StringComparer.Ordinal),
            OverrideHolds = new HashSet<string>(state.OverrideHolds ?? new HashSet<string>(), StringComparer.Ordinal),
            PersistedBucketCount = state.PersistedBucketCount,
        };
    }
}
