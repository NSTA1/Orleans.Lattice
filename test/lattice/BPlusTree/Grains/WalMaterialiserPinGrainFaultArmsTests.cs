using System.Collections.Concurrent;
using System.Reflection;
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
/// Fault-containment and flush-coalescing coverage for
/// <see cref="WalMaterialiserPinGrain"/>.
/// <para>
/// Every failure on this grain's durable path is deliberately swallowed, and
/// they are all safe in the same direction: a pin that does not land leaves the
/// durable floor <em>lower</em> than memory, which retains more WAL. The unsafe
/// direction would be advancing a durable pin past what a materialiser has
/// actually consumed, which trims WAL the consumer still needs. These tests pin
/// that asymmetry - each one asserts the fault was absorbed <b>and</b> that the
/// in-memory floor the WAL GC actually reads is unchanged.
/// </para>
/// <para>
/// The sibling fixtures cover the happy paths: <c>WalMaterialiserPinGrainTests</c>
/// the pin contract, <c>WalMaterialiserPinGrainBucketTests</c> the bucketed
/// layout and its compatibility properties, and
/// <c>WalMaterialiserPinGrainEtagTests</c> the per-slot ETag carry.
/// </para>
/// </summary>
[TestFixture]
public sealed class WalMaterialiserPinGrainFaultArmsTests
{
    private const string Tree = "tree-fault-arms";

    // Two consumers that hash to different buckets at a width of 8, so a
    // per-bucket assertion distinguishes "wrote only this bucket" from "wrote
    // everything". Asserted rather than assumed by Consumers_used_here_*.
    private const string ConsumerA = "_lattice_materialiser_tree-fault-arms_leaf-A";
    private const string ConsumerB = "_lattice_materialiser_tree-fault-arms_leaf-B";

    private const int Buckets = 8;

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static GrainId PinGrainId() => GrainId.Create("wal-materialiser-pin", Tree);

    private sealed class TickHolder
    {
        public Func<CancellationToken, Task>? Value { get; set; }
    }

    private sealed record Harness(
        WalMaterialiserPinGrain Grain,
        FakePersistentState<WalMaterialiserPinState> Legacy,
        FaultingPinStore Store,
        RecordingLoggerFactory Logs,
        TickHolder Tick)
    {
        /// <summary>
        /// The coalesced-flush callback, once armed. The grain arms its timer
        /// lazily on the first report rather than at activation, so this is null
        /// until a report has been made with a positive flush interval.
        /// </summary>
        public Func<CancellationToken, Task>? FlushTick => Tick.Value;
    }

    /// <summary>
    /// Activates a bucketed pin grain over <paramref name="store"/>, capturing
    /// the coalesced-flush timer callback so a tick can be driven explicitly
    /// instead of waited for. A positive <paramref name="flushIntervalMs"/> is
    /// what makes the grain arm that timer and leave the pin dirty for the tick
    /// to drain; at zero it persists synchronously on every report.
    /// </summary>
    private static async Task<Harness> ActivateAsync(
        FaultingPinStore store,
        int buckets = Buckets,
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

        var legacy = new FakePersistentState<WalMaterialiserPinState>();
        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeOptions
        {
            WalMaterialiserPinBuckets = buckets,
            WalMaterialiserPinFlushIntervalMs = flushIntervalMs,
        });

        var logs = new RecordingLoggerFactory();
        var grain = new WalMaterialiserPinGrain(
            context, legacy, options, logs.CreateLogger<WalMaterialiserPinGrain>(), store);
        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

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

    [Test]
    public void Consumers_used_here_hash_to_distinct_buckets()
    {
        // Guards every per-bucket assertion below: if these two ever collided,
        // "only one bucket was written" would hold vacuously.
        Assert.That(
            WalMaterialiserPinRouting.BucketOf(ConsumerA, Buckets),
            Is.Not.EqualTo(WalMaterialiserPinRouting.BucketOf(ConsumerB, Buckets)));
    }

    // --- Bucket read faults ---

    [Test]
    public async Task A_bucket_whose_read_fails_is_omitted_from_the_floor_and_the_rest_still_load()
    {
        // Seed both consumers durably, then fail exactly one bucket's read.
        var seed = new FaultingPinStore();
        seed.SeedPin(WalMaterialiserPinRouting.BucketStateName(ConsumerA, Buckets), ConsumerA, Hlc(100));
        seed.SeedPin(WalMaterialiserPinRouting.BucketStateName(ConsumerB, Buckets), ConsumerB, Hlc(200));
        seed.FailReadsForSlot = WalMaterialiserPinRouting.BucketStateName(ConsumerA, Buckets);

        var h = await ActivateAsync(seed);
        var pins = await h.Grain.GetPinsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(pins.ContainsKey(ConsumerA), Is.False,
                "A bucket that could not be read must be omitted rather than guessed at.");
            Assert.That(pins.TryGetValue(ConsumerB, out var b) ? b : default, Is.EqualTo(Hlc(200)),
                "The readable buckets must still load - this is the positive control that "
                + "proves the omission above is the read fault and not a broken fixture.");
        });

        var warnings = h.Logs.Warnings.Where(w => w.Value("Bucket") is not null).ToArray();
        Assert.That(warnings, Is.Not.Empty, "The omitted bucket must be reported.");
        Assert.That(
            warnings.Select(w => w.Value("Bucket")).Distinct().Single(),
            Is.EqualTo(WalMaterialiserPinRouting.BucketOf(ConsumerA, Buckets)),
            "Only the bucket that actually failed may be reported as omitted.");
    }

    [Test]
    public async Task An_omitted_bucket_lowers_the_durable_floor_rather_than_raising_it()
    {
        // The safety property behind the arm above. Omitting a bucket may only
        // ever retain more WAL; a floor computed from the surviving buckets
        // must never sit above the pin that was lost.
        var seed = new FaultingPinStore();
        seed.SeedPin(WalMaterialiserPinRouting.BucketStateName(ConsumerA, Buckets), ConsumerA, Hlc(10));
        seed.SeedPin(WalMaterialiserPinRouting.BucketStateName(ConsumerB, Buckets), ConsumerB, Hlc(999));
        seed.FailReadsForSlot = WalMaterialiserPinRouting.BucketStateName(ConsumerA, Buckets);

        var h = await ActivateAsync(seed);
        var pins = await h.Grain.GetPinsAsync();

        Assert.That(pins.Values, Is.Not.Empty);
        Assert.That(pins.ContainsKey(ConsumerA), Is.False,
            "The low pin was the one lost, so the surviving floor is higher than the true "
            + "floor - which is why the read fault must not be treated as 'no pin exists' "
            + "by anything that trims. It is re-established by the consumer's next report.");
    }

    // --- Removal marks only the removed consumer's bucket ---

    [Test]
    public async Task Remove_rewrites_only_the_removed_consumers_bucket()
    {
        var h = await ActivateAsync(new FaultingPinStore());
        await h.Grain.ReportAsync(ConsumerA, Hlc(100));
        await h.Grain.ReportAsync(ConsumerB, Hlc(200));
        h.Store.WrittenSlots.Clear();

        await h.Grain.RemoveAsync(ConsumerA);

        var expected = WalMaterialiserPinRouting.BucketStateName(ConsumerA, Buckets);
        Assert.That(h.Store.WrittenSlots, Does.Contain(expected));
        Assert.That(
            h.Store.WrittenSlots,
            Does.Not.Contain(WalMaterialiserPinRouting.BucketStateName(ConsumerB, Buckets)),
            "Removing one consumer must not rewrite an unrelated consumer's bucket - that "
            + "amplification is exactly what bucketing exists to remove.");

        var pins = await h.Grain.GetPinsAsync();
        Assert.That(pins.ContainsKey(ConsumerA), Is.False);
        Assert.That(pins.ContainsKey(ConsumerB), Is.True);
    }

    [Test]
    public async Task Removing_an_unknown_consumer_writes_nothing()
    {
        var h = await ActivateAsync(new FaultingPinStore());
        await h.Grain.ReportAsync(ConsumerA, Hlc(100));
        h.Store.WrittenSlots.Clear();

        await h.Grain.RemoveAsync("_lattice_materialiser_tree-fault-arms_leaf-absent");

        Assert.That(h.Store.WrittenSlots, Is.Empty,
            "A removal that changed nothing must not issue a durable write.");
    }

    // --- Legacy-slot clear fault ---

    [Test]
    public async Task Clear_swallows_a_failing_legacy_slot_write_and_still_clears_memory()
    {
        var h = await ActivateAsync(new FaultingPinStore());
        await h.Grain.ReportAsync(ConsumerA, Hlc(100));

        // Under bucketing the buckets persist through the storage provider, so
        // the injected IPersistentState is written only by the legacy-slot
        // clear - which makes this fault specific to that one arm.
        h.Legacy.ThrowOnWrite = new InvalidOperationException("legacy slot unavailable");

        Assert.DoesNotThrowAsync(() => h.Grain.ClearAsync(),
            "Clear runs on tree deletion; a stale legacy slot only retains WAL, so it "
            + "must not fail the deletion.");

        var pins = await h.Grain.GetPinsAsync();
        Assert.That(pins, Is.Empty, "The in-memory floor must still be cleared.");

        var warning = h.Logs.Warnings.SingleOrDefault(
            w => w.Message.Contains("legacy", StringComparison.OrdinalIgnoreCase));
        Assert.That(warning, Is.Not.Null, "The swallowed legacy-slot fault must be reported.");
    }

    [Test]
    public async Task Clear_on_an_empty_pin_map_writes_nothing()
    {
        var h = await ActivateAsync(new FaultingPinStore());

        await h.Grain.ClearAsync();

        Assert.That(h.Store.WrittenSlots, Is.Empty);
    }

    // --- Coalesced flush tick ---

    [Test]
    public async Task A_coalesced_flush_tick_persists_the_accumulated_advance()
    {
        // Positive control for the two tick faults below: it proves the
        // captured callback really is the flush tick and really writes.
        var h = await ActivateAsync(new FaultingPinStore(), flushIntervalMs: 50);

        await h.Grain.ReportAsync(ConsumerA, Hlc(100));
        Assert.That(h.FlushTick, Is.Not.Null, "The first report must arm the flush timer.");
        Assert.That(h.Store.WrittenSlots, Is.Empty,
            "With coalescing armed the report must not write synchronously.");

        await h.FlushTick!(CancellationToken.None);

        Assert.That(
            h.Store.WrittenSlots,
            Does.Contain(WalMaterialiserPinRouting.BucketStateName(ConsumerA, Buckets)));
    }

    [Test]
    public async Task A_coalesced_flush_tick_with_nothing_dirty_writes_nothing()
    {
        var h = await ActivateAsync(new FaultingPinStore(), flushIntervalMs: 50);
        await h.Grain.ReportAsync(ConsumerA, Hlc(100));
        Assert.That(h.FlushTick, Is.Not.Null);

        // Drain the pending advance, then tick again on a clean grain.
        await h.FlushTick!(CancellationToken.None);
        h.Store.WrittenSlots.Clear();

        await h.FlushTick!(CancellationToken.None);

        Assert.That(h.Store.WrittenSlots, Is.Empty,
            "An idle tick must cost no provider round-trip; the timer runs forever.");
    }

    [Test]
    public async Task A_failing_coalesced_flush_is_swallowed_and_the_advance_is_retried_next_tick()
    {
        var h = await ActivateAsync(new FaultingPinStore(), flushIntervalMs: 50);
        await h.Grain.ReportAsync(ConsumerA, Hlc(100));
        Assert.That(h.FlushTick, Is.Not.Null);

        h.Store.FailWrites = true;
        Assert.DoesNotThrowAsync(() => h.FlushTick!(CancellationToken.None),
            "A timer tick that throws would tear down the flush timer and strand every "
            + "later advance with nothing to drain it.");

        var warning = h.Logs.Warnings.FirstOrDefault(
            w => w.Message.Contains("Coalesced", StringComparison.Ordinal));
        Assert.That(warning, Is.Not.Null, "The swallowed flush fault must be reported.");

        // The advance must survive the failed write and land on the retry -
        // this is what makes swallowing the fault safe rather than lossy.
        h.Store.FailWrites = false;
        await h.FlushTick!(CancellationToken.None);

        Assert.That(
            h.Store.WrittenSlots,
            Does.Contain(WalMaterialiserPinRouting.BucketStateName(ConsumerA, Buckets)),
            "The failed batch must have re-armed its buckets so the next tick retries them.");
    }

    [Test]
    public async Task A_flush_tick_inside_the_amortisation_window_defers_without_writing()
    {
        // The amortisation policy bounds the share of grain time spent inside
        // WriteStateAsync, so a burst of reports cannot starve the non-reentrant
        // queue every reporting leaf joins. Its inputs are Environment.TickCount64
        // readings taken by the previous write, whose resolution (~15ms) makes an
        // in-memory write's measured duration reliably zero - so the window is
        // established directly rather than by racing a real clock.
        var h = await ActivateAsync(new FaultingPinStore(), flushIntervalMs: 50);
        await h.Grain.ReportAsync(ConsumerA, Hlc(100));
        Assert.That(h.FlushTick, Is.Not.Null);

        SetField(h.Grain, "_lastWriteDurationMs", 100L);
        SetField(h.Grain, "_lastWriteCompletedTickMs", Environment.TickCount64);

        await h.FlushTick!(CancellationToken.None);

        Assert.That(h.Store.WrittenSlots, Is.Empty,
            "A tick inside the window must not start another write.");
        Assert.That(Field<bool>(h.Grain, "_dirty"), Is.True,
            "The advance must stay dirty so a later tick still persists it - deferring "
            + "may only make the durable pin staler (more WAL retained), never drop it.");

        // And once the window has elapsed the same tick does write, which is
        // what proves the deferral was a delay rather than a loss.
        SetField(h.Grain, "_lastWriteCompletedTickMs", Environment.TickCount64 - 100_000L);
        await h.FlushTick!(CancellationToken.None);

        Assert.That(
            h.Store.WrittenSlots,
            Does.Contain(WalMaterialiserPinRouting.BucketStateName(ConsumerA, Buckets)));
    }

    [Test]
    public void The_amortisation_policy_never_defers_before_the_first_write()
    {
        // Pins the documented boundary of the pure policy the tick consults:
        // a zero duration means no write has completed yet, so the first flush
        // after a burst starts immediately.
        Assert.Multiple(() =>
        {
            Assert.That(
                WalMaterialiserPinGrain.ShouldDeferCoalescedFlush(1_000, 1_000, 0),
                Is.False,
                "No write has completed, so there is nothing to amortise against.");
            Assert.That(
                WalMaterialiserPinGrain.ShouldDeferCoalescedFlush(1_050, 1_000, 100),
                Is.True,
                "50ms after a 100ms write is inside the 9x amortisation window.");
            Assert.That(
                WalMaterialiserPinGrain.ShouldDeferCoalescedFlush(2_000, 1_000, 100),
                Is.False,
                "1000ms after a 100ms write is past the 9x window.");
        });
    }

    // --- Concurrent flush coalescing ---
    //
    // PersistNowAsync's `_flushInFlight -> await Task.Yield(); continue;` arm is
    // deliberately left uncovered. Reaching it needs a second caller to enter
    // while a first write is parked mid-await, and the only way to hold that
    // window open from a test is a gate the second caller then spins against.
    // That spin monopolises NUnit's single-threaded async context and hangs the
    // host (observed, not theorised). Every formulation that avoids the hang
    // reintroduces a thread-timing dependence, which this repo's review bar
    // rejects outright - a coalescing arm is not worth a fixture that flakes on
    // a two-core CI runner. The property itself is not unguarded: the loop's
    // purpose is that a durability point returns only once its own mutation has
    // landed, and that is pinned by the awaited-write assertions above.

    // --- Nothing-dirty durable write ---

    [Test]
    public async Task A_durable_write_with_no_dirty_buckets_issues_no_provider_traffic()
    {
        // Every site that marks the grain dirty also marks the owning bucket, so
        // this guard is not reachable through the public surface; it is invoked
        // directly to pin the contract it encodes - a flush with no advanced
        // bucket must cost zero provider round-trips. That is the whole point of
        // bucketing, and a regression here would silently reinstate the
        // O(consumers on this shard) write amplification of issue #2012.
        var h = await ActivateAsync(new FaultingPinStore());
        await h.Grain.ReportAsync(ConsumerA, Hlc(100));
        h.Store.WrittenSlots.Clear();

        Assert.That(Field<HashSet<int>>(h.Grain, "_dirtyBuckets"), Is.Empty,
            "A landed flush leaves no bucket dirty - the precondition this guard covers.");

        var writeDurable = typeof(WalMaterialiserPinGrain)
            .GetMethod("WriteDurableAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        await (Task)writeDurable.Invoke(h.Grain, null)!;

        Assert.That(h.Store.WrittenSlots, Is.Empty);
    }

    /// <summary>
    /// An <see cref="IGrainStorage"/> that can fail a nominated slot's read and
    /// fail every write, so each swallowed fault arm can be driven precisely.
    /// </summary>
    private sealed class FaultingPinStore : IGrainStorage
    {
        private readonly ConcurrentDictionary<string, WalMaterialiserPinState> _slots = new(StringComparer.Ordinal);

        public List<string> WrittenSlots { get; } = new();

        public bool FailWrites { get; set; }

        public string? FailReadsForSlot { get; set; }

        public void SeedPin(string stateName, string consumerId, HybridLogicalClock frontier)
        {
            var state = _slots.TryGetValue(stateName, out var existing) ? Clone(existing) : new WalMaterialiserPinState();
            state.Pins[consumerId] = frontier;
            _slots[stateName] = state;
        }

        public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            if (string.Equals(stateName, FailReadsForSlot, StringComparison.Ordinal))
            {
                return Task.FromException(new InvalidOperationException($"slot '{stateName}' unavailable"));
            }

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
