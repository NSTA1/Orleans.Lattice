using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4641: the override hold the pin store keeps beside the monotonic-max
/// pins. A hold is raised durably before a leaf appends a write stamped below its
/// clock, and it must stand until - and only until - the consumer's durable
/// offset becomes real, because only from then on does the WAL GC's offset-floor
/// stop protect the write on every arm.
/// </summary>
[TestFixture]
public sealed class WalMaterialiserPinGrainOverrideHoldTests
{
    private const string Consumer = "_lattice_materialiser_tree-4641_leaf-7_1";
    private const string Other = "_lattice_materialiser_tree-4641_leaf-8_1";

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks };

    private static (WalMaterialiserPinGrain Grain, FakePersistentState<WalMaterialiserPinState> State) CreateGrain(
        FakePersistentState<WalMaterialiserPinState>? existing = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("wal-materialiser-pin", "tree-4641"));
        var state = existing ?? new FakePersistentState<WalMaterialiserPinState>();
        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeOptions { WalMaterialiserPinFlushIntervalMs = 0 });
        return (new WalMaterialiserPinGrain(context, state, options), state);
    }

    [Test]
    public async Task A_raised_hold_is_durable_before_the_raise_returns()
    {
        var (grain, state) = CreateGrain();
        await grain.ReportManyAsync([new MaterialiserPinReport(Consumer, Hlc(500), -1)]);
        var writes = state.WriteCount;

        await grain.RaiseOverrideHoldsAsync([Consumer]);

        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(writes + 1), "the raise is a write-through");
            Assert.That(state.State.OverrideHolds, Does.Contain(Consumer));
            Assert.That(state.State.Pins[Consumer], Is.EqualTo(Hlc(500)), "the hold leaves the max-merged frontier alone");
        });
        Assert.That(await grain.GetOverrideHoldsAsync(), Is.EquivalentTo(new[] { Consumer }));
    }

    [Test]
    public async Task A_raise_for_a_consumer_with_a_real_offset_writes_nothing()
    {
        var (grain, state) = CreateGrain();
        await grain.ReportManyAsync([new MaterialiserPinReport(Consumer, Hlc(500), 3)]);
        var writes = state.WriteCount;

        await grain.RaiseOverrideHoldsAsync([Consumer]);

        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(writes), "the offset-floor stop already protects the write");
            Assert.That(state.State.OverrideHolds, Is.Empty);
        });
    }

    [Test]
    public async Task A_raise_for_a_consumer_with_no_pin_also_seeds_a_block_pin()
    {
        var (grain, state) = CreateGrain();

        await grain.RaiseOverrideHoldsAsync([Consumer]);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.OverrideHolds, Does.Contain(Consumer));
            Assert.That(state.State.Pins[Consumer], Is.EqualTo(HybridLogicalClock.Zero),
                "a held consumer always carries a pin, so the GC census sees it");
            Assert.That(state.State.Offsets[Consumer], Is.EqualTo(-1));
        });
    }

    [TestCase(-1L, TestName = "A_hold_stands_through_a_report_with_no_real_offset")]
    [TestCase(-1L, true, TestName = "A_hold_stands_through_a_block_report_from_a_checkpoint_without_a_capture")]
    public async Task A_hold_stands_until_a_real_offset_lands(long offset, bool blockReport = false)
    {
        var (grain, state) = CreateGrain();
        await grain.ReportManyAsync([new MaterialiserPinReport(Consumer, Hlc(500), -1)]);
        await grain.RaiseOverrideHoldsAsync([Consumer]);

        // A leaf that persisted a checkpoint without a capture still publishes
        // (Zero, -1): LeafDurablePinCore gates every real offset on coverage.
        await grain.ReportManyAsync(
            [new MaterialiserPinReport(Consumer, blockReport ? HybridLogicalClock.Zero : Hlc(900), offset)]);

        Assert.That(state.State.OverrideHolds, Does.Contain(Consumer),
            "nothing protects the held write until a coverage-gated offset lands");
    }

    [Test]
    public async Task A_real_offset_drops_the_hold_in_the_same_write()
    {
        var (grain, state) = CreateGrain();
        await grain.ReportManyAsync([new MaterialiserPinReport(Consumer, Hlc(500), -1)]);
        await grain.RaiseOverrideHoldsAsync([Consumer, Other]);

        await grain.ReportManyAsync([new MaterialiserPinReport(Consumer, Hlc(900), 0)]);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.OverrideHolds, Is.EquivalentTo(new[] { Other }),
                "only the consumer whose offset became real is released");
            Assert.That(state.State.Offsets[Consumer], Is.EqualTo(0));
        });
    }

    [Test]
    public async Task A_raise_that_does_not_become_durable_is_not_left_standing()
    {
        var (grain, state) = CreateGrain();
        await grain.ReportManyAsync([new MaterialiserPinReport(Consumer, Hlc(500), -1)]);
        state.ThrowOnWrite = new InvalidOperationException("storage unavailable");

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.RaiseOverrideHoldsAsync([Consumer]));
        Assert.That(await grain.GetOverrideHoldsAsync(), Is.Empty,
            "a second raise must not find an in-memory hold and return without a durable write");

        await grain.RaiseOverrideHoldsAsync([Consumer]);
        Assert.That(state.State.OverrideHolds, Does.Contain(Consumer));
    }

    [Test]
    public async Task Holds_survive_a_reactivation_and_one_whose_offset_became_real_is_pruned()
    {
        var (grain, state) = CreateGrain();
        await grain.ReportManyAsync(
        [
            new MaterialiserPinReport(Consumer, Hlc(500), -1),
            new MaterialiserPinReport(Other, Hlc(500), -1),
        ]);
        await grain.RaiseOverrideHoldsAsync([Consumer, Other]);

        // A stale slot can carry a hold next to an offset that is already real.
        state.State.Offsets[Other] = 4;
        var (reactivated, _) = CreateGrain(state);
        await LeafActivationHarness.ActivateAsync(reactivated, CancellationToken.None);

        Assert.That(await reactivated.GetOverrideHoldsAsync(), Is.EquivalentTo(new[] { Consumer }));
    }

    [Test]
    public async Task Remove_and_clear_drop_holds()
    {
        var (grain, state) = CreateGrain();
        await grain.RaiseOverrideHoldsAsync([Consumer, Other]);

        await grain.RemoveAsync(Consumer);
        Assert.That(state.State.OverrideHolds, Is.EquivalentTo(new[] { Other }));

        await grain.ClearAsync();
        Assert.That(state.State.OverrideHolds, Is.Empty);
    }

    [Test]
    public void Raise_rejects_a_blank_consumer()
    {
        var (grain, _) = CreateGrain();

        Assert.ThrowsAsync<ArgumentException>(() => grain.RaiseOverrideHoldsAsync([" "]));
        Assert.ThrowsAsync<ArgumentNullException>(() => grain.RaiseOverrideHoldsAsync(null!));
    }
}
