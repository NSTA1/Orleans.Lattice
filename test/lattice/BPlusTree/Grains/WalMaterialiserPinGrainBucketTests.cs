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
/// Unit tests for the bucketed durable persistence of
/// <see cref="WalMaterialiserPinGrain"/> added for issue #2014.
/// <para>
/// Orleans persists grain state as one whole blob, so a shard holding N
/// consumer pins previously rewrote all N of them to record a single leaf's
/// advance - the write amplification behind issue #2012. Bucketing splits the
/// persistence across several slots and rewrites only the ones whose contents
/// changed, while the activation still unions every slot in memory so the read
/// contract (<see cref="IWalMaterialiserPinGrain.GetPinsAsync"/>) and the WAL
/// GC's per-pass fan-in are both unchanged.
/// </para>
/// <para>
/// The compatibility properties are the point of this fixture: the default
/// bucket count of one must persist to the single legacy slot exactly as every
/// prior build did, raising the count must not lose a pre-bucketing pin, and
/// lowering it must not strand one - a stranded pin is invisible to the trim
/// floor, which is the only unsafe direction.
/// </para>
/// </summary>
[TestFixture]
public sealed class WalMaterialiserPinGrainBucketTests
{
    private const string Tree = "tree-2014";
    private const string ConsumerA = "_lattice_materialiser_tree-2014_leaf-A";
    private const string ConsumerB = "_lattice_materialiser_tree-2014_leaf-B";
    private const string ConsumerC = "_lattice_materialiser_tree-2014_leaf-C";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static GrainId PinGrainId() => GrainId.Create("wal-materialiser-pin", Tree);

    private static async Task<(WalMaterialiserPinGrain grain, FakePersistentState<WalMaterialiserPinState> legacy)> ActivateAsync(
        BucketStore store,
        int buckets)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(PinGrainId());

        // The injected IPersistentState models the legacy unsuffixed slot, which
        // Orleans reads before activation; seed it from the same store so the
        // dual-read is exercised end to end.
        var legacy = new FakePersistentState<WalMaterialiserPinState>();
        if (store.Snapshot(WalMaterialiserPinState.StateName) is { } persistedLegacy)
        {
            legacy.State = persistedLegacy;
        }

        // In production the injected IPersistentState is backed by the same
        // provider under the legacy slot name, so a write through it must be
        // visible to the next activation. Model that write-back explicitly; it
        // is not recorded in WrittenSlots, which tracks only direct
        // IGrainStorage traffic.
        legacy.OnWriteState = s => store.Replace(WalMaterialiserPinState.StateName, s);

        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeOptions
        {
            WalMaterialiserPinBuckets = buckets,
            WalMaterialiserPinFlushIntervalMs = 0,
        });

        var grain = new WalMaterialiserPinGrain(context, legacy, options, logger: null, pinStorage: store);
        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);
        return (grain, legacy);
    }

    [Test]
    public async Task Default_bucket_count_persists_to_the_legacy_slot_only()
    {
        var store = new BucketStore();
        var (grain, legacy) = await ActivateAsync(store, buckets: 1);

        await grain.ReportAsync(ConsumerA, Hlc(100));

        Assert.Multiple(() =>
        {
            Assert.That(legacy.State.Pins[ConsumerA], Is.EqualTo(Hlc(100)),
                "the default layout must write through the injected IPersistentState exactly as before");
            Assert.That(store.WrittenSlots, Is.Empty,
                "an un-bucketed host must not touch the storage provider directly at all");
        });
    }

    [Test]
    public async Task Bucketed_report_writes_only_the_consumers_own_bucket()
    {
        var store = new BucketStore();
        var (grain, _) = await ActivateAsync(store, buckets: 8);

        await grain.ReportAsync(ConsumerA, Hlc(100));
        store.WrittenSlots.Clear();
        await grain.ReportAsync(ConsumerB, Hlc(200));

        var expected = WalMaterialiserPinRouting.BucketStateName(ConsumerB, 8);
        Assert.That(store.WrittenSlots, Is.EquivalentTo(new[] { expected }),
            "advancing one consumer must rewrite that consumer's bucket, not every pin on the shard");
        Assert.That(store.WrittenSlots, Does.Not.Contain(WalMaterialiserPinState.StateName),
            "the legacy slot is a rollback anchor and must never be rewritten by a bucketed advance");
    }

    [Test]
    public async Task Bucketed_pins_round_trip_across_a_reactivation()
    {
        var store = new BucketStore();
        var (first, _) = await ActivateAsync(store, buckets: 8);
        await first.ReportAsync(ConsumerA, Hlc(100));
        await first.ReportAsync(ConsumerB, Hlc(200));
        await first.ReportAsync(ConsumerC, Hlc(300));

        var (second, _) = await ActivateAsync(store, buckets: 8);
        var pins = await second.GetPinsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(pins[ConsumerA], Is.EqualTo(Hlc(100)));
            Assert.That(pins[ConsumerB], Is.EqualTo(Hlc(200)));
            Assert.That(pins[ConsumerC], Is.EqualTo(Hlc(300)));
        });
    }

    [Test]
    public async Task Enabling_bucketing_keeps_reading_pre_bucketing_pins()
    {
        var store = new BucketStore();

        // A pin written by a build that predates bucketing lives in the legacy
        // slot and nowhere else.
        var (legacyGrain, _) = await ActivateAsync(store, buckets: 1);
        await legacyGrain.ReportAsync(ConsumerA, Hlc(100));
        store.Seed(WalMaterialiserPinState.StateName, ConsumerA, Hlc(100));

        var (bucketed, _) = await ActivateAsync(store, buckets: 8);
        var pins = await bucketed.GetPinsAsync();

        Assert.That(pins[ConsumerA], Is.EqualTo(Hlc(100)),
            "raising the bucket count must be self-healing: a pre-bucketing pin keeps counting toward the trim floor");
    }

    [Test]
    public async Task Lowering_the_bucket_count_consolidates_rather_than_stranding_pins()
    {
        var store = new BucketStore();
        var (wide, _) = await ActivateAsync(store, buckets: 16);
        await wide.ReportAsync(ConsumerA, Hlc(100));
        await wide.ReportAsync(ConsumerB, Hlc(200));
        await wide.ReportAsync(ConsumerC, Hlc(300));

        // Restart the same durable store under a narrower configuration.
        var (narrow, _) = await ActivateAsync(store, buckets: 2);
        var pins = await narrow.GetPinsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(pins[ConsumerA], Is.EqualTo(Hlc(100)));
            Assert.That(pins[ConsumerB], Is.EqualTo(Hlc(200)));
            Assert.That(pins[ConsumerC], Is.EqualTo(Hlc(300)));
        });

        // And the consolidation must be durable, not merely in memory: a third
        // activation reading only the narrow range must still see every pin.
        var (again, _) = await ActivateAsync(store, buckets: 2);
        var afterConsolidation = await again.GetPinsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(afterConsolidation[ConsumerA], Is.EqualTo(Hlc(100)),
                "a pin stranded in an out-of-range slot is invisible to the trim floor, which is the one genuinely unsafe direction");
            Assert.That(afterConsolidation[ConsumerB], Is.EqualTo(Hlc(200)));
            Assert.That(afterConsolidation[ConsumerC], Is.EqualTo(Hlc(300)));
        });
    }

    [Test]
    public async Task Failed_consolidation_leaves_the_wide_width_recorded_so_it_retries()
    {
        var store = new BucketStore();
        var (wide, _) = await ActivateAsync(store, buckets: 16);
        await wide.ReportAsync(ConsumerA, Hlc(100));
        await wide.ReportAsync(ConsumerB, Hlc(200));
        await wide.ReportAsync(ConsumerC, Hlc(300));

        // Crash every write during the narrowing consolidation.
        store.FailWrites = true;
        var (failed, _) = await ActivateAsync(store, buckets: 2);
        Assert.That(async () => await failed.GetPinsAsync(), Throws.Nothing,
            "a failed consolidation must be swallowed - the pins are still in memory and every later advance re-marks its bucket dirty");

        // Recovery: a fresh activation must still find the wide layout recorded
        // and therefore still read the slots the failed consolidation could not
        // move, rather than silently dropping them from the floor.
        store.FailWrites = false;
        var (recovered, _) = await ActivateAsync(store, buckets: 2);
        var pins = await recovered.GetPinsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(pins[ConsumerA], Is.EqualTo(Hlc(100)));
            Assert.That(pins[ConsumerB], Is.EqualTo(Hlc(200)));
            Assert.That(pins[ConsumerC], Is.EqualTo(Hlc(300)));
        });
    }

    [Test]
    public async Task Bucketed_merge_is_monotonic_max()
    {
        var store = new BucketStore();
        var (grain, _) = await ActivateAsync(store, buckets: 4);

        await grain.ReportAsync(ConsumerA, Hlc(200));
        await grain.ReportAsync(ConsumerA, Hlc(50));

        var (reactivated, _) = await ActivateAsync(store, buckets: 4);
        var pins = await reactivated.GetPinsAsync();

        Assert.That(pins[ConsumerA], Is.EqualTo(Hlc(200)),
            "a pin must never roll back: a lower report is coalesced, because a retreating floor lets the WAL GC over-trim");
    }

    [Test]
    public async Task Bucketed_offsets_round_trip_alongside_frontiers()
    {
        var store = new BucketStore();
        var (grain, _) = await ActivateAsync(store, buckets: 4);

        await grain.ReportManyAsync(new[]
        {
            new MaterialiserPinReport(ConsumerA, Hlc(100), 42),
            new MaterialiserPinReport(ConsumerB, Hlc(200), 7),
        });

        var (reactivated, _) = await ActivateAsync(store, buckets: 4);
        var offsets = await reactivated.GetPinOffsetsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(offsets[ConsumerA], Is.EqualTo(42));
            Assert.That(offsets[ConsumerB], Is.EqualTo(7));
        });
    }

    [Test]
    public async Task ClearAsync_removes_every_bucket_and_the_legacy_slot()
    {
        var store = new BucketStore();
        var (grain, _) = await ActivateAsync(store, buckets: 4);
        await grain.ReportAsync(ConsumerA, Hlc(100));
        await grain.ReportAsync(ConsumerB, Hlc(200));
        store.Seed(WalMaterialiserPinState.StateName, ConsumerC, Hlc(300));

        await grain.ClearAsync();

        var (reactivated, _) = await ActivateAsync(store, buckets: 4);
        var pins = await reactivated.GetPinsAsync();

        Assert.That(pins, Is.Empty,
            "tree deletion is the one path that must clear the legacy slot too, or a deleted tree's pins would pin its WAL forever");
    }

    /// <summary>
    /// In-memory <see cref="IGrainStorage"/> standing in for the durable
    /// "lattice" provider, recording which slots were written so a test can
    /// assert that an advance rewrote only the bucket it had to.
    /// </summary>
    private sealed class BucketStore : IGrainStorage
    {
        private readonly ConcurrentDictionary<string, WalMaterialiserPinState> _slots = new(StringComparer.Ordinal);

        public List<string> WrittenSlots { get; } = new();

        public bool FailWrites { get; set; }

        public WalMaterialiserPinState? Snapshot(string stateName) =>
            _slots.TryGetValue(stateName, out var state) ? Clone(state) : null;

        public void Seed(string stateName, string consumerId, HybridLogicalClock frontier)
        {
            var state = _slots.TryGetValue(stateName, out var existing) ? Clone(existing) : new WalMaterialiserPinState();
            state.Pins[consumerId] = frontier;
            _slots[stateName] = state;
        }

        /// <summary>
        /// Replaces a slot wholesale without recording it as direct provider
        /// traffic. Models the injected <c>IPersistentState</c> writing back to
        /// the same provider under the legacy slot name.
        /// </summary>
        public void Replace(string stateName, WalMaterialiserPinState state) =>
            _slots[stateName] = Clone(state);

        public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            if (_slots.TryGetValue(stateName, out var state))
            {
                grainState.State = (T)(object)Clone(state);
                grainState.RecordExists = true;
            }
            else
            {
                grainState.State = (T)(object)new WalMaterialiserPinState();
                grainState.RecordExists = false;
            }

            return Task.CompletedTask;
        }

        public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            if (FailWrites)
            {
                return Task.FromException(new InvalidOperationException("durable pin store unavailable"));
            }

            WrittenSlots.Add(stateName);
            _slots[stateName] = Clone((WalMaterialiserPinState)(object)grainState.State!);
            grainState.RecordExists = true;
            return Task.CompletedTask;
        }

        public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            _slots.TryRemove(stateName, out _);
            grainState.RecordExists = false;
            return Task.CompletedTask;
        }

        private static WalMaterialiserPinState Clone(WalMaterialiserPinState source) => new()
        {
            Pins = new Dictionary<string, HybridLogicalClock>(source.Pins, StringComparer.Ordinal),
            Offsets = new Dictionary<string, long>(source.Offsets, StringComparer.Ordinal),
            PersistedBucketCount = source.PersistedBucketCount,
        };
    }
}
