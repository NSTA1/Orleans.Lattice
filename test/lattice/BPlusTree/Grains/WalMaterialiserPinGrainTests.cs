using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="WalMaterialiserPinGrain"/>: the durable per-tree
/// leaf-materialiser pin store the WAL GC consults so its trim floor survives a
/// full silo/cluster restart (issue #919). Exercises the monotonic-max merge,
/// snapshot read, and removal/clear contracts directly against a
/// <see cref="FakePersistentState{T}"/> with no storage provider.
/// </summary>
[TestFixture]
public sealed class WalMaterialiserPinGrainTests
{
    private const string Consumer = "_lattice_materialiser_tree-1_leaf-7";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static (WalMaterialiserPinGrain grain, FakePersistentState<WalMaterialiserPinState> state) CreateGrain(
        FakePersistentState<WalMaterialiserPinState>? existing = null,
        int flushIntervalMs = 0)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("wal-materialiser-pin", "tree-1"));
        var state = existing ?? new FakePersistentState<WalMaterialiserPinState>();
        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        // Drive the synchronous (no-coalescing) write path so the per-call
        // WriteCount assertions are deterministic without a grain timer runtime.
        options.Get(Arg.Any<string>()).Returns(new LatticeOptions { WalMaterialiserPinFlushIntervalMs = flushIntervalMs });
        return (new WalMaterialiserPinGrain(context, state, options), state);
    }

    [Test]
    public async Task ReportAsync_new_consumer_persists_pin()
    {
        var (grain, state) = CreateGrain();

        await grain.ReportAsync(Consumer, Hlc(100));

        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(1));
            Assert.That(state.State.Pins[Consumer], Is.EqualTo(Hlc(100)));
        });
    }

    [Test]
    public async Task ReportAsync_higher_frontier_advances_pin_and_persists()
    {
        var (grain, state) = CreateGrain();
        await grain.ReportAsync(Consumer, Hlc(100));

        await grain.ReportAsync(Consumer, Hlc(200));

        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(2));
            Assert.That(state.State.Pins[Consumer], Is.EqualTo(Hlc(200)));
        });
    }

    [Test]
    public async Task ReportAsync_lower_frontier_is_coalesced_without_write()
    {
        var (grain, state) = CreateGrain();
        await grain.ReportAsync(Consumer, Hlc(200));

        await grain.ReportAsync(Consumer, Hlc(100));

        Assert.Multiple(() =>
        {
            // The lower report is dropped: no second write, pin unchanged.
            Assert.That(state.WriteCount, Is.EqualTo(1));
            Assert.That(state.State.Pins[Consumer], Is.EqualTo(Hlc(200)));
        });
    }

    [Test]
    public async Task ReportAsync_equal_frontier_is_coalesced_without_write()
    {
        var (grain, state) = CreateGrain();
        await grain.ReportAsync(Consumer, Hlc(200));

        await grain.ReportAsync(Consumer, Hlc(200));

        Assert.That(state.WriteCount, Is.EqualTo(1));
    }

    [Test]
    public async Task ReportAsync_zero_frontier_seeds_block_pin()
    {
        var (grain, state) = CreateGrain();

        await grain.ReportAsync(Consumer, HybridLogicalClock.Zero);

        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(1));
            Assert.That(state.State.Pins.ContainsKey(Consumer), Is.True);
            Assert.That(state.State.Pins[Consumer], Is.EqualTo(HybridLogicalClock.Zero));
        });
    }

    [Test]
    public async Task ReportAsync_zero_after_real_frontier_does_not_roll_back()
    {
        var (grain, state) = CreateGrain();
        await grain.ReportAsync(Consumer, Hlc(100));

        await grain.ReportAsync(Consumer, HybridLogicalClock.Zero);

        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(1));
            Assert.That(state.State.Pins[Consumer], Is.EqualTo(Hlc(100)));
        });
    }

    [Test]
    public async Task GetPinsAsync_returns_independent_snapshot_copy()
    {
        var (grain, state) = CreateGrain();
        await grain.ReportAsync(Consumer, Hlc(100));

        var snapshot = await grain.GetPinsAsync();
        // Mutating the durable state after the snapshot must not be observed
        // through the already-returned copy.
        state.State.Pins["other"] = Hlc(999);

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.Count, Is.EqualTo(1));
            Assert.That(snapshot[Consumer], Is.EqualTo(Hlc(100)));
            Assert.That(snapshot.ContainsKey("other"), Is.False);
        });
    }

    [Test]
    public async Task RemoveAsync_removes_pin_and_persists()
    {
        var (grain, state) = CreateGrain();
        await grain.ReportAsync(Consumer, Hlc(100));

        await grain.RemoveAsync(Consumer);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Pins.ContainsKey(Consumer), Is.False);
            Assert.That(state.WriteCount, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task RemoveAsync_unknown_consumer_is_noop_without_write()
    {
        var (grain, state) = CreateGrain();
        await grain.ReportAsync(Consumer, Hlc(100));

        await grain.RemoveAsync("_lattice_materialiser_tree-1_leaf-absent");

        Assert.That(state.WriteCount, Is.EqualTo(1));
    }

    [Test]
    public async Task ClearAsync_clears_all_pins_and_persists()
    {
        var (grain, state) = CreateGrain();
        await grain.ReportAsync(Consumer, Hlc(100));
        await grain.ReportAsync("_lattice_materialiser_tree-1_leaf-8", Hlc(50));

        await grain.ClearAsync();

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Pins, Is.Empty);
            // Two reports + one clear.
            Assert.That(state.WriteCount, Is.EqualTo(3));
        });
    }

    [Test]
    public async Task ClearAsync_empty_store_is_noop_without_write()
    {
        var (grain, state) = CreateGrain();

        await grain.ClearAsync();

        Assert.That(state.WriteCount, Is.EqualTo(0));
    }

    [Test]
    public async Task ReportManyAsync_merges_all_pins_in_a_single_write()
    {
        var (grain, state) = CreateGrain();

        await grain.ReportManyAsync(new[]
        {
            new MaterialiserPinReport(Consumer, Hlc(100), 100),
            new MaterialiserPinReport("_lattice_materialiser_tree-1_leaf-8", Hlc(50), 50),
        });

        Assert.Multiple(() =>
        {
            // The whole batch coalesces into one durable write.
            Assert.That(state.WriteCount, Is.EqualTo(1));
            Assert.That(state.State.Pins[Consumer], Is.EqualTo(Hlc(100)));
            Assert.That(state.State.Pins["_lattice_materialiser_tree-1_leaf-8"], Is.EqualTo(Hlc(50)));
        });
    }

    [Test]
    public async Task ReportManyAsync_records_checkpoint_offsets_readable_via_GetPinOffsets()
    {
        var (grain, _) = CreateGrain();

        await grain.ReportManyAsync(new[]
        {
            new MaterialiserPinReport(Consumer, Hlc(100), 100),
            new MaterialiserPinReport("_lattice_materialiser_tree-1_leaf-8", Hlc(50), 50),
        });

        var offsets = await grain.GetPinOffsetsAsync();
        Assert.Multiple(() =>
        {
            Assert.That(offsets[Consumer], Is.EqualTo(100L));
            Assert.That(offsets["_lattice_materialiser_tree-1_leaf-8"], Is.EqualTo(50L));
        });
    }

    [Test]
    public async Task ReportManyAsync_offset_advances_monotonically_even_when_frontier_is_flat()
    {
        // A tombstone-compaction reap advances the applied WAL offset while the
        // HLC frontier stays flat (the reap reuses the reaped entry's old HLC).
        // The offset axis must still advance (an advance on EITHER axis is a
        // real advance) so the WAL GC offset floor tracks the reap.
        var (grain, state) = CreateGrain();
        await grain.ReportManyAsync(new[] { new MaterialiserPinReport(Consumer, Hlc(100), 5) });

        await grain.ReportManyAsync(new[] { new MaterialiserPinReport(Consumer, Hlc(100), 9) });

        var offsets = await grain.GetPinOffsetsAsync();
        Assert.Multiple(() =>
        {
            Assert.That(state.State.Pins[Consumer], Is.EqualTo(Hlc(100)), "The flat frontier is retained.");
            Assert.That(offsets[Consumer], Is.EqualTo(9L), "The offset advances even though the frontier did not.");
            Assert.That(state.WriteCount, Is.EqualTo(2), "An offset-only advance is a real advance and is persisted.");
        });
    }

    [Test]
    public async Task ReportManyAsync_offset_never_regresses_on_a_stale_report()
    {
        var (grain, _) = CreateGrain();
        await grain.ReportManyAsync(new[] { new MaterialiserPinReport(Consumer, Hlc(100), 100) });

        // A stale/duplicate report with a lower offset must not lower the pin.
        await grain.ReportManyAsync(new[] { new MaterialiserPinReport(Consumer, Hlc(100), 40) });

        var offsets = await grain.GetPinOffsetsAsync();
        Assert.That(offsets[Consumer], Is.EqualTo(100L));
    }

    [Test]
    public async Task ReportManyAsync_all_coalesced_writes_nothing()
    {
        var (grain, state) = CreateGrain();
        await grain.ReportAsync(Consumer, Hlc(200));

        await grain.ReportManyAsync(new[]
        {
            new MaterialiserPinReport(Consumer, Hlc(100), -1),
            new MaterialiserPinReport(Consumer, HybridLogicalClock.Zero, -1),
        });

        Assert.That(state.WriteCount, Is.EqualTo(1));
    }

    [Test]
    public async Task SeedManyAsync_persists_block_pins_durably()
    {
        var (grain, state) = CreateGrain();

        await grain.SeedManyAsync(new[]
        {
            new MaterialiserPinReport(Consumer, HybridLogicalClock.Zero, -1),
            new MaterialiserPinReport("_lattice_materialiser_tree-1_leaf-8", HybridLogicalClock.Zero, -1),
        });

        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(1));
            Assert.That(state.State.Pins[Consumer], Is.EqualTo(HybridLogicalClock.Zero));
            Assert.That(state.State.Pins["_lattice_materialiser_tree-1_leaf-8"], Is.EqualTo(HybridLogicalClock.Zero));
        });
    }

    [Test]
    public async Task SeedManyAsync_zero_after_real_frontier_is_noop()
    {
        var (grain, state) = CreateGrain();
        await grain.ReportAsync(Consumer, Hlc(100));

        await grain.SeedManyAsync(new[] { new MaterialiserPinReport(Consumer, HybridLogicalClock.Zero, -1) });

        Assert.Multiple(() =>
        {
            // The real frontier is retained and no redundant write is issued.
            Assert.That(state.WriteCount, Is.EqualTo(1));
            Assert.That(state.State.Pins[Consumer], Is.EqualTo(Hlc(100)));
        });
    }

    [Test]
    public void ReportAsync_null_consumer_throws()
    {
        var (grain, _) = CreateGrain();
        Assert.That(
            async () => await grain.ReportAsync(null!, Hlc(1)),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void RemoveAsync_whitespace_consumer_throws()
    {
        var (grain, _) = CreateGrain();
        Assert.That(
            async () => await grain.RemoveAsync("   "),
            Throws.InstanceOf<ArgumentException>());
    }
}
