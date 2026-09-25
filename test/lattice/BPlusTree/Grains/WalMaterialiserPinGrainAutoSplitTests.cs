using System.Collections.Concurrent;
using System.Reflection;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Storage;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #3576: the durable pin store must stay under the storage provider's
/// entity limit for large trees at default options.
/// <para>
/// Orleans persists grain state as one blob, and Azure Table rejects a blob
/// over 983,040 bytes. With the default bucket count of one, the pin shard
/// persisted its whole map to a single slot that grew linearly with the number
/// of leaf-partition consumers; on the pl3c rig it reached 6.15 MB, every
/// coalesced flush and birth seed failed with "Data too large", and each retry
/// re-serialised the whole map inside the non-reentrant grain. The grain now
/// treats the configured bucket count as a floor and widens the layout by
/// powers of two whenever a slot's estimated size would exceed
/// <see cref="WalMaterialiserPinGrain.TargetSlotBytes"/>, backs off a failing
/// store, and bounds how many slots one coalesced flush writes.
/// </para>
/// <para>
/// The store here enforces the provider limit against the payload's JSON
/// encoding counted as UTF-16, which is larger than the binary encoding the
/// default grain-storage serialiser produces, so a pass is conservative.
/// </para>
/// </summary>
[TestFixture]
public sealed class WalMaterialiserPinGrainAutoSplitTests
{
    private const string Tree = "tree-3576";

    /// <summary>Azure Table's grain-storage entity payload limit.</summary>
    private const int AzureTableMaxBytes = 983_040;

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static GrainId PinGrainId() => GrainId.Create("wal-materialiser-pin", Tree);

    /// <summary>
    /// A consumer id in the production shape
    /// <c>_lattice_materialiser_{tree}_{leafGrainId}</c> with a partition suffix.
    /// </summary>
    private static string Consumer(int leaf, int partition) =>
        $"{ILeafCursorReporter.MaterialiserConsumerIdPrefix}{Tree}_leaf/{leaf:x32}~p{partition}";

    private sealed class TickHolder
    {
        public Func<CancellationToken, Task>? Value { get; set; }
    }

    private sealed record Harness(
        WalMaterialiserPinGrain Grain,
        FakePersistentState<WalMaterialiserPinState> Legacy,
        SizeLimitedPinStore Store,
        RecordingLoggerFactory Logs,
        TickHolder Tick);

    private static async Task<Harness> ActivateAsync(
        SizeLimitedPinStore store,
        int? buckets = null,
        int flushIntervalMs = 0)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(PinGrainId());

        var tick = new TickHolder();
        var timers = Substitute.For<ITimerRegistry>();
        timers.RegisterGrainTimer(
                Arg.Any<IGrainContext>(),
                Arg.Any<Func<Func<CancellationToken, Task>, CancellationToken, Task>>(),
                Arg.Any<Func<CancellationToken, Task>>(),
                Arg.Any<GrainTimerCreationOptions>())
            .Returns(ci =>
            {
                tick.Value = ci.ArgAt<Func<CancellationToken, Task>>(2);
                return Substitute.For<IGrainTimer>();
            });

        var services = new ServiceCollection();
        services.AddSingleton(timers);
        context.ActivationServices.Returns(services.BuildServiceProvider());

        // The injected IPersistentState is the legacy slot, backed by the same
        // provider in production, so it is subject to the same size limit.
        var legacy = new FakePersistentState<WalMaterialiserPinState>();
        if (store.Snapshot(WalMaterialiserPinState.StateName) is { } persistedLegacy)
        {
            legacy.State = persistedLegacy;
        }

        legacy.OnWriteState = s => store.WriteLegacy(s);

        // Default options unless a test overrides the bucket count: the
        // regression is specifically about what a host that configures nothing
        // gets.
        var latticeOptions = new LatticeOptions { WalMaterialiserPinFlushIntervalMs = flushIntervalMs };
        if (buckets is { } configured)
        {
            latticeOptions.WalMaterialiserPinBuckets = configured;
        }

        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.Get(Arg.Any<string>()).Returns(latticeOptions);

        var logs = new RecordingLoggerFactory();
        var grain = new WalMaterialiserPinGrain(
            context, legacy, options, logs.CreateLogger<WalMaterialiserPinGrain>(), store);
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        return new Harness(grain, legacy, store, logs, tick);
    }

    private static T Field<T>(WalMaterialiserPinGrain grain, string name) =>
        (T)typeof(WalMaterialiserPinGrain)
            .GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(grain)!;

    private static void SetField(WalMaterialiserPinGrain grain, string name, object value) =>
        typeof(WalMaterialiserPinGrain)
            .GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!
            .SetValue(grain, value);

    private static List<MaterialiserPinReport> LeafBirth(int leaf, int partitions, long frontier = 0)
    {
        var reports = new List<MaterialiserPinReport>(partitions);
        for (var p = 0; p < partitions; p++)
        {
            reports.Add(new MaterialiserPinReport(Consumer(leaf, p), Hlc(frontier), -1));
        }

        return reports;
    }

    [Test]
    public void Default_bucket_count_is_still_one()
    {
        // The auto-split is what makes the default safe, so the documented
        // default and its byte-for-byte legacy behaviour for small shards are
        // unchanged.
        Assert.That(new LatticeOptions().WalMaterialiserPinBuckets, Is.EqualTo(1));
        Assert.That(LatticeOptions.DefaultWalMaterialiserPinBuckets, Is.EqualTo(1));
    }

    [TestCase(1, 1, 0L, ExpectedResult = 1)]
    [TestCase(1, 1, 64L * 1024, ExpectedResult = 1)]
    [TestCase(1, 1, (64L * 1024) + 1, ExpectedResult = 2)]
    [TestCase(1, 1, 1024L * 1024, ExpectedResult = 32)]
    [TestCase(1, 8, 1_000L, ExpectedResult = 8)]
    [TestCase(1, 8, 1024L * 1024, ExpectedResult = 32)]
    [TestCase(8, 8, 512L * 1024, ExpectedResult = 8)]
    [TestCase(8, 8, (512L * 1024) + 8, ExpectedResult = 16)]
    [TestCase(8, 16, 1_000L, ExpectedResult = 16)]
    [TestCase(1, 1, long.MaxValue / 4, ExpectedResult = 1024)]
    // Growth doubles until each slot is at most half the target, so a freshly
    // split store has headroom before it has to split again.
    public int ResolveTargetWidth_grows_by_powers_of_two_from_the_floor(
        int layoutWidth, int configured, long estimate) =>
        WalMaterialiserPinGrain.ResolveTargetWidth(layoutWidth, configured, estimate, allowNarrow: false);

    [TestCase(32, 1, 1_000L, ExpectedResult = 1)]
    [TestCase(32, 2, 1_000L, ExpectedResult = 2)]
    [TestCase(32, 1, 256L * 1024, ExpectedResult = 8)]
    [TestCase(16, 16, 1_000L, ExpectedResult = 16)]
    [TestCase(12, 3, 1_000L, ExpectedResult = 3)]
    public int ResolveTargetWidth_narrows_only_to_the_floor_and_only_with_headroom(
        int layoutWidth, int configured, long estimate) =>
        WalMaterialiserPinGrain.ResolveTargetWidth(layoutWidth, configured, estimate, allowNarrow: true);

    [Test]
    public void ResolveTargetWidth_never_narrows_on_the_write_path()
    {
        Assert.That(
            WalMaterialiserPinGrain.ResolveTargetWidth(32, 1, 1_000L, allowNarrow: false),
            Is.EqualTo(32),
            "narrowing rewrites the whole shard, so it is confined to activation");
    }

    [Test]
    public void Size_estimate_is_conservative_against_the_json_encoding()
    {
        var state = new WalMaterialiserPinState();
        long estimate = 0;
        for (var leaf = 0; leaf < 200; leaf++)
        {
            var id = Consumer(leaf, leaf % 8);
            state.Pins[id] = Hlc(long.MaxValue / 3, int.MaxValue);
            state.Offsets[id] = long.MaxValue / 3;
            estimate += WalMaterialiserPinGrain.EstimatePinEntryBytes(id)
                + WalMaterialiserPinGrain.EstimateOffsetEntryBytes(id);
        }

        Assert.That(estimate, Is.GreaterThanOrEqualTo(SizeLimitedPinStore.Measure(state)),
            "the split decision is only safe if the estimate never under-counts a real payload");
    }

    /// <summary>
    /// THE regression. Ten thousand leaves across eight WAL partitions is
    /// eighty thousand consumers; spread over the default eight pin shards that
    /// is ten thousand per shard. This drives twice that into ONE shard, through
    /// the birth-seed path a bulk load uses, at default options, against a store
    /// that rejects any payload over the Azure Table limit exactly as the
    /// provider does.
    /// </summary>
    [Test]
    public async Task A_large_tree_at_default_options_persists_every_pin_within_the_provider_limit()
    {
        const int leaves = 2_500;
        const int partitions = 8;
        var store = new SizeLimitedPinStore(AzureTableMaxBytes);
        var h = await ActivateAsync(store);

        for (var leaf = 0; leaf < leaves; leaf++)
        {
            await h.Grain.SeedManyAsync(LeafBirth(leaf, partitions));
        }

        Assert.That(store.Rejections, Is.Zero,
            "no write may exceed the provider limit: before the fix the single legacy slot reached "
            + "several megabytes and every write was rejected");

        var width = Field<int>(h.Grain, "_bucketCount");
        Assert.Multiple(() =>
        {
            Assert.That(width, Is.GreaterThan(1), "the store must have split itself");
            Assert.That(width & (width - 1), Is.Zero, "and only ever by powers of two");
            Assert.That(store.LargestWriteBytes, Is.LessThanOrEqualTo(WalMaterialiserPinGrain.TargetSlotBytes),
                "every slot is written within the per-slot target, far under the provider limit");
            Assert.That(store.LegacyWrites, Is.LessThan(50),
                "the legacy slot is written only while the whole map still fits one target slot");
        });

        // Durability, not just memory: a fresh activation at default options
        // must recover every consumer from the store alone.
        var reactivated = await ActivateAsync(store);
        var pins = await reactivated.Grain.GetPinsAsync();
        Assert.That(pins, Has.Count.EqualTo(leaves * partitions),
            "every seeded block pin must be durable; a pin the WAL GC cannot see lets it trim WAL a leaf still needs");
        Assert.That(store.Rejections, Is.Zero);
    }

    [Test]
    public async Task A_split_store_is_read_at_its_recorded_width_by_a_host_configured_narrower()
    {
        var store = new SizeLimitedPinStore(AzureTableMaxBytes);
        var h = await ActivateAsync(store, buckets: 1);
        for (var leaf = 0; leaf < 200; leaf++)
        {
            await h.Grain.SeedManyAsync(LeafBirth(leaf, 8, frontier: 10 + leaf));
        }

        var width = Field<int>(h.Grain, "_bucketCount");
        Assume.That(width, Is.GreaterThan(1), "precondition: the store split");

        var reactivated = await ActivateAsync(store, buckets: 1);
        var pins = await reactivated.Grain.GetPinsAsync();
        Assert.That(pins, Has.Count.EqualTo(1_600));
        Assert.That(pins[Consumer(199, 7)], Is.EqualTo(Hlc(209)));
    }

    [Test]
    public async Task A_legacy_store_that_outgrows_one_slot_migrates_without_losing_a_pin()
    {
        // A pre-fix deployment: every pin lives in the single legacy slot.
        var store = new SizeLimitedPinStore(int.MaxValue);
        var legacyState = new WalMaterialiserPinState();
        for (var leaf = 0; leaf < 400; leaf++)
        {
            for (var p = 0; p < 8; p++)
            {
                legacyState.Pins[Consumer(leaf, p)] = Hlc(1_000 + leaf);
            }
        }

        store.Replace(WalMaterialiserPinState.StateName, legacyState);
        var h = await ActivateAsync(store);

        // Only one consumer advances; the rest of the map must be carried into
        // the new layout rather than stranded in a slot nobody rewrites.
        await h.Grain.ReportAsync(Consumer(0, 0), Hlc(5_000));
        Assert.That(Field<int>(h.Grain, "_bucketCount"), Is.GreaterThan(1),
            "precondition: the legacy map exceeds the per-slot target, so the store splits");

        var reactivated = await ActivateAsync(store);
        var pins = await reactivated.Grain.GetPinsAsync();
        Assert.Multiple(() =>
        {
            Assert.That(pins, Has.Count.EqualTo(3_200),
                "the legacy slot stays authoritative for any pin not yet rewritten into its bucket");
            Assert.That(pins[Consumer(0, 0)], Is.EqualTo(Hlc(5_000)));
            Assert.That(pins[Consumer(399, 7)], Is.EqualTo(Hlc(1_399)));
        });
    }

    [TestCase(1)]
    [TestCase(10)]
    [TestCase(24)]
    [TestCase(25)]
    [TestCase(26)]
    [TestCase(31)]
    public async Task A_growth_relayout_that_crashes_part_way_loses_no_pin(int writesBeforeCrash)
    {
        // Eight configured buckets, then enough consumers in one advance that a
        // slot of eight would exceed the target: the grain re-lays the shard out
        // at 32 - writes [8, 32), then bucket zero, then [1, 8). Crash after
        // every interesting prefix of that sequence.
        var store = new SizeLimitedPinStore(AzureTableMaxBytes);
        var h = await ActivateAsync(store, buckets: 8);
        await h.Grain.ReportAsync(Consumer(0, 0), Hlc(1));

        var reports = new List<MaterialiserPinReport>();
        for (var leaf = 1; leaf < 1_000; leaf++)
        {
            reports.Add(new MaterialiserPinReport(Consumer(leaf, 0), Hlc(100 + leaf), leaf));
        }

        // Land the map at width 8 first, over the target, by writing it with
        // the auto-split disabled for this one step.
        await SeedAtFixedWidthAsync(store, reports, width: 8);

        store.FailAfterWrites = writesBeforeCrash;
        var crashing = await ActivateAsync(store, buckets: 8);
        Assert.That(Field<int>(crashing.Grain, "_relayoutTarget"), Is.EqualTo(32).Or.EqualTo(0),
            "precondition: activation found the shard over the target and scheduled a relayout");

        store.FailAfterWrites = null;
        var recovered = await ActivateAsync(store, buckets: 8);
        var pins = await recovered.Grain.GetPinsAsync();
        var offsets = await recovered.Grain.GetPinOffsetsAsync();
        Assert.Multiple(() =>
        {
            Assert.That(pins, Has.Count.EqualTo(1_000),
                "a crash at any point of the relayout must leave bucket zero naming a layout whose slots hold every pin");
            Assert.That(pins[Consumer(999, 0)], Is.EqualTo(Hlc(1_099)));
            Assert.That(offsets[Consumer(999, 0)], Is.EqualTo(999));
        });

        // And the relayout completes on the next activation.
        Assert.That(Field<int>(recovered.Grain, "_bucketCount"), Is.EqualTo(32));
        var final = await ActivateAsync(store, buckets: 8);
        Assert.That(await final.Grain.GetPinsAsync(), Has.Count.EqualTo(1_000));
    }

    [Test]
    public async Task A_small_shard_narrows_back_to_the_legacy_slot_and_reads_back_at_any_configuration()
    {
        var store = new SizeLimitedPinStore(AzureTableMaxBytes);
        var wide = await ActivateAsync(store, buckets: 8);
        await wide.Grain.ReportAsync(Consumer(1, 0), Hlc(100));
        await wide.Grain.ReportAsync(Consumer(2, 0), Hlc(200));

        // Back to the default: the shard is tiny, so it consolidates into the
        // legacy slot and records a width of one.
        var narrowed = await ActivateAsync(store);
        Assert.That(Field<int>(narrowed.Grain, "_bucketCount"), Is.EqualTo(1));
        Assert.That(store.Snapshot(WalMaterialiserPinRouting.BucketStateName(0))!.PersistedBucketCount, Is.EqualTo(1));

        var atDefault = await ActivateAsync(store);
        var pins = await atDefault.Grain.GetPinsAsync();
        Assert.That(pins[Consumer(1, 0)], Is.EqualTo(Hlc(100)));
        Assert.That(pins[Consumer(2, 0)], Is.EqualTo(Hlc(200)));

        // Raising the floor again re-splits from the legacy slot.
        var raised = await ActivateAsync(store, buckets: 8);
        await raised.Grain.ReportAsync(Consumer(3, 0), Hlc(300));
        var again = await ActivateAsync(store, buckets: 8);
        var after = await again.Grain.GetPinsAsync();
        Assert.Multiple(() =>
        {
            Assert.That(after[Consumer(1, 0)], Is.EqualTo(Hlc(100)));
            Assert.That(after[Consumer(2, 0)], Is.EqualTo(Hlc(200)));
            Assert.That(after[Consumer(3, 0)], Is.EqualTo(Hlc(300)));
        });
    }

    [Test]
    public async Task An_oversize_rejection_splits_the_store_instead_of_retrying_the_same_write_every_tick()
    {
        // A provider limit far below the per-slot target stands in for an
        // estimate that under-counts: the legacy write is rejected as too large
        // although the estimate said it fitted.
        var store = new SizeLimitedPinStore(16 * 1024);
        var h = await ActivateAsync(store, flushIntervalMs: 50);
        var reports = new List<MaterialiserPinReport>();
        for (var leaf = 0; leaf < 60; leaf++)
        {
            reports.Add(new MaterialiserPinReport(Consumer(leaf, 0), Hlc(100 + leaf), leaf));
        }

        await h.Grain.ReportManyAsync(reports);
        Assert.That(h.Tick.Value, Is.Not.Null);
        Assert.That(Field<int>(h.Grain, "_bucketCount"), Is.EqualTo(1), "precondition: the estimate fits one slot");

        await h.Tick.Value!(CancellationToken.None);
        Assert.That(store.Rejections, Is.EqualTo(1), "the first flush is rejected as too large");
        Assert.That(h.Logs.Warnings.Any(w => w.Message.Contains("Coalesced", StringComparison.Ordinal)), Is.True);

        // Inside the backoff window no tick touches the store at all.
        var attempts = store.WriteAttempts;
        for (var i = 0; i < 20; i++)
        {
            await h.Tick.Value!(CancellationToken.None);
        }

        Assert.That(store.WriteAttempts, Is.EqualTo(attempts),
            "a failing write must not be retried on every tick: each retry re-serialises inside the non-reentrant grain");

        // After the backoff the corrected estimate splits the store, and the
        // split write fits.
        SetField(h.Grain, "_nextFlushAttemptTickMs", 0L);
        SetField(h.Grain, "_lastWriteCompletedTickMs", Environment.TickCount64 - 100_000L);
        await h.Tick.Value!(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(Field<int>(h.Grain, "_bucketCount"), Is.GreaterThan(1),
                "an oversize rejection must drive a split, not an identical retry");
            Assert.That(store.Rejections, Is.EqualTo(1), "and the split write must land");
            Assert.That(Field<int>(h.Grain, "_consecutiveFlushFailures"), Is.Zero);
        });

        var reactivated = await ActivateAsync(store);
        Assert.That(await reactivated.Grain.GetPinsAsync(), Has.Count.EqualTo(60));
    }

    [Test]
    public async Task A_coalesced_flush_writes_a_bounded_number_of_slots_per_tick()
    {
        var store = new SizeLimitedPinStore(AzureTableMaxBytes);
        var h = await ActivateAsync(store, buckets: 64, flushIntervalMs: 50);
        var reports = new List<MaterialiserPinReport>();
        for (var leaf = 0; leaf < 2_000; leaf++)
        {
            reports.Add(new MaterialiserPinReport(Consumer(leaf, 0), Hlc(100 + leaf), leaf));
        }

        await h.Grain.ReportManyAsync(reports);
        store.ResetCounters();

        var ticks = 0;
        while (Field<bool>(h.Grain, "_dirty") && ticks < 64)
        {
            SetField(h.Grain, "_lastWriteCompletedTickMs", Environment.TickCount64 - 100_000L);
            var before = store.WriteAttempts;
            await h.Tick.Value!(CancellationToken.None);
            Assert.That(store.WriteAttempts - before, Is.LessThanOrEqualTo(WalMaterialiserPinGrain.MaxSlotsPerCoalescedFlush),
                "one tick must not rewrite the whole shard: that is what monopolised the pin grain for seconds");
            ticks++;
        }

        Assert.That(ticks, Is.GreaterThan(1), "the drain spans several ticks");
        Assert.That(Field<bool>(h.Grain, "_dirty"), Is.False, "and completes");
        var reactivated = await ActivateAsync(store, buckets: 64);
        Assert.That(await reactivated.Grain.GetPinsAsync(), Has.Count.EqualTo(2_000));
    }

    [Test]
    public async Task A_persistently_failing_store_fails_seeds_fast_and_keeps_the_pins_in_memory()
    {
        var store = new SizeLimitedPinStore(AzureTableMaxBytes);
        var h = await ActivateAsync(store, buckets: 8);
        store.FailWrites = true;

        for (var i = 0; i < WalMaterialiserPinGrain.FailFastThreshold; i++)
        {
            Assert.ThrowsAsync<InvalidOperationException>(
                () => h.Grain.SeedManyAsync(LeafBirth(i, 1)),
                "a genuine store failure still surfaces to the seeding leaf");
        }

        var attempts = store.WriteAttempts;
        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.SeedManyAsync(LeafBirth(99, 1)));
        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Does.Contain("consecutive writes"),
                "past the threshold the seed fails fast rather than paying another write");
            Assert.That(store.WriteAttempts, Is.EqualTo(attempts), "no store traffic while failing fast");
        });

        var pins = await h.Grain.GetPinsAsync();
        Assert.That(pins.ContainsKey(Consumer(99, 0)), Is.True,
            "the block pin is merged in memory, which is what the WAL GC reads, so GC stays safe while the store is down");

        // Recovery: once the backoff lapses and the store heals, the next seed
        // persists everything held in memory.
        store.FailWrites = false;
        SetField(h.Grain, "_nextFlushAttemptTickMs", 0L);
        await h.Grain.SeedManyAsync(LeafBirth(100, 1));
        await h.Grain.ReportAsync(Consumer(100, 0), Hlc(1));
        Assert.That(Field<int>(h.Grain, "_consecutiveFlushFailures"), Is.Zero);

        var reactivated = await ActivateAsync(store, buckets: 8);
        var durable = await reactivated.Grain.GetPinsAsync();
        Assert.That(durable.ContainsKey(Consumer(99, 0)), Is.True,
            "the fail-fast seed was held dirty in memory and made durable by the first successful flush");
    }

    [TestCase(0, ExpectedResult = 0L)]
    [TestCase(1, ExpectedResult = 1_000L)]
    [TestCase(2, ExpectedResult = 2_000L)]
    [TestCase(6, ExpectedResult = 32_000L)]
    [TestCase(7, ExpectedResult = 60_000L)]
    [TestCase(1_000, ExpectedResult = 60_000L)]
    public long ComputeFlushBackoffMs_doubles_to_a_ceiling(int failures) =>
        WalMaterialiserPinGrain.ComputeFlushBackoffMs(failures);

    [Test]
    public void IsOversizeFailure_recognises_provider_size_rejections_through_wrappers()
    {
        var azure = new ArgumentOutOfRangeException(
            "state", "Data too large to write to Azure table. Size=1001234 MaxSize=983040");
        Assert.Multiple(() =>
        {
            Assert.That(WalMaterialiserPinGrain.IsOversizeFailure(azure), Is.True);
            Assert.That(WalMaterialiserPinGrain.IsOversizeFailure(new InvalidOperationException("outer", azure)), Is.True);
            Assert.That(WalMaterialiserPinGrain.IsOversizeFailure(new AggregateException(new TimeoutException(), azure)), Is.True);
            Assert.That(WalMaterialiserPinGrain.IsOversizeFailure(new Exception("RequestEntityTooLarge")), Is.True);
            Assert.That(WalMaterialiserPinGrain.IsOversizeFailure(new TimeoutException("timed out")), Is.False);
            Assert.That(WalMaterialiserPinGrain.IsOversizeFailure(new InconsistentStateException("etag")), Is.False);
            Assert.That(WalMaterialiserPinGrain.IsOversizeFailure(null), Is.False);
        });
    }

    /// <summary>
    /// Writes <paramref name="reports"/> straight into the store at a fixed
    /// bucketed width, stamped with that width, bypassing the grain - the
    /// state a pre-fix build configured at that width would have left behind.
    /// </summary>
    private static Task SeedAtFixedWidthAsync(SizeLimitedPinStore store, IEnumerable<MaterialiserPinReport> reports, int width)
    {
        var slots = new Dictionary<int, WalMaterialiserPinState>();
        for (var bucket = 0; bucket < width; bucket++)
        {
            slots[bucket] = store.Snapshot(WalMaterialiserPinRouting.BucketStateName(bucket)) ?? new WalMaterialiserPinState();
            slots[bucket].PersistedBucketCount = width;
        }

        foreach (var report in reports)
        {
            var slot = slots[WalMaterialiserPinRouting.BucketOf(report.ConsumerId, width)];
            slot.Pins[report.ConsumerId] = report.Frontier;
            slot.Offsets[report.ConsumerId] = report.CheckpointOffset;
        }

        foreach (var (bucket, state) in slots)
        {
            store.Replace(WalMaterialiserPinRouting.BucketStateName(bucket), state);
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// In-memory <see cref="IGrainStorage"/> that rejects a payload over a byte
    /// limit with the same exception Orleans' Azure Table grain storage throws,
    /// and can crash after a given number of writes.
    /// </summary>
    private sealed class SizeLimitedPinStore(int maxBytes) : IGrainStorage
    {
        private readonly ConcurrentDictionary<string, WalMaterialiserPinState> _slots = new(StringComparer.Ordinal);
        private int _writeAttempts;
        private int _writesLanded;
        private int _rejections;
        private long _largestWriteBytes;
        private int _legacyWrites;

        public int WriteAttempts => Volatile.Read(ref _writeAttempts);

        public int Rejections => Volatile.Read(ref _rejections);

        public long LargestWriteBytes => Interlocked.Read(ref _largestWriteBytes);

        public int LegacyWrites => Volatile.Read(ref _legacyWrites);

        public bool FailWrites { get; set; }

        /// <summary>When set, every write after this many successful ones fails.</summary>
        public int? FailAfterWrites { get; set; }

        public void ResetCounters()
        {
            _writeAttempts = 0;
            _writesLanded = 0;
            _rejections = 0;
            _largestWriteBytes = 0;
            _legacyWrites = 0;
        }

        public static long Measure(WalMaterialiserPinState state)
            => 2L * JsonSerializer.SerializeToUtf8Bytes(new { state.Pins, state.Offsets, state.PersistedBucketCount }).Length;

        public WalMaterialiserPinState? Snapshot(string stateName) =>
            _slots.TryGetValue(stateName, out var state) ? Clone(state) : null;

        public void Replace(string stateName, WalMaterialiserPinState state) => _slots[stateName] = Clone(state);

        public void WriteLegacy(WalMaterialiserPinState state)
        {
            Interlocked.Increment(ref _legacyWrites);
            Write(WalMaterialiserPinState.StateName, state);
        }

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
            try
            {
                Write(stateName, (WalMaterialiserPinState)(object)grainState.State!);
                grainState.RecordExists = true;
                return Task.CompletedTask;
            }
            catch (Exception ex)
            {
                return Task.FromException(ex);
            }
        }

        public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            _slots.TryRemove(stateName, out _);
            grainState.RecordExists = false;
            return Task.CompletedTask;
        }

        private void Write(string stateName, WalMaterialiserPinState state)
        {
            Interlocked.Increment(ref _writeAttempts);
            if (FailWrites)
            {
                throw new InvalidOperationException("durable pin store unavailable");
            }

            if (FailAfterWrites is { } limit && Volatile.Read(ref _writesLanded) >= limit)
            {
                throw new InvalidOperationException("durable pin store crashed");
            }

            var bytes = Measure(state);
            if (bytes > maxBytes)
            {
                Interlocked.Increment(ref _rejections);
                throw new ArgumentOutOfRangeException(
                    nameof(state), $"Data too large to write to Azure table. Size={bytes} MaxSize={maxBytes}");
            }

            long seen;
            while ((seen = Interlocked.Read(ref _largestWriteBytes)) < bytes
                && Interlocked.CompareExchange(ref _largestWriteBytes, bytes, seen) != seen)
            {
            }

            _slots[stateName] = Clone(state);
            Interlocked.Increment(ref _writesLanded);
        }

        private static WalMaterialiserPinState Clone(WalMaterialiserPinState source) => new()
        {
            Pins = new Dictionary<string, HybridLogicalClock>(source.Pins, StringComparer.Ordinal),
            Offsets = new Dictionary<string, long>(source.Offsets, StringComparer.Ordinal),
            PersistedBucketCount = source.PersistedBucketCount,
        };
    }
}
