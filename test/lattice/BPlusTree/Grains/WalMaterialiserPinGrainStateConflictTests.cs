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
/// Regression coverage for issue #3572 on the default single-slot layout of
/// <see cref="WalMaterialiserPinGrain"/> (the bucketed layout's ETag handling is
/// issue #2096's). A pin write that lands but is reported as an ETag conflict
/// must translate the fault, deactivate, and stop writing with the stale ETag; a
/// fresh activation over the same row must see the landed pin and write again.
/// </summary>
[TestFixture]
public sealed class WalMaterialiserPinGrainStateConflictTests
{
    private const string Tree = "tree-3572";
    private const string ConsumerA = "_lattice_materialiser_tree-3572_leaf-A";

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    private static async Task<(WalMaterialiserPinGrain Grain, LandedConflictPersistentState<WalMaterialiserPinState> State, IGrainContext Context)> ActivateAsync(
        DurableStateRow<WalMaterialiserPinState> row)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("wal-materialiser-pin", Tree));
        var state = new LandedConflictPersistentState<WalMaterialiserPinState>(row);
        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeOptions { WalMaterialiserPinFlushIntervalMs = 0 });

        var grain = new WalMaterialiserPinGrain(context, state, options);
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        return (grain, state, context);
    }

    [Test]
    public async Task ReportAsync_landed_conflict_deactivates_and_a_fresh_activation_writes_again()
    {
        var row = new DurableStateRow<WalMaterialiserPinState>();
        var (grain, state, context) = await ActivateAsync(row);
        await grain.ReportAsync(ConsumerA, Hlc(100));
        state.LandThenConflictOnNextWrite();

        var ex = Assert.CatchAsync(() => grain.ReportAsync(ConsumerA, Hlc(200)));

        Assert.That(ex, Is.TypeOf<LatticeStateWriteFailedException>());
        var translated = (LatticeStateWriteFailedException)ex!;
        Assert.Multiple(() =>
        {
            Assert.That(translated.Conflict, Is.True);
            Assert.That(translated.GrainType, Is.EqualTo("wal-materialiser-pin"));
            Assert.That(translated.GrainKey, Is.EqualTo(Tree));
            Assert.That(translated.InnerException, Is.Null);
        });
        context.ReceivedWithAnyArgs(1).Deactivate(default!);

        var attemptsBefore = state.WriteAttempts;
        Assert.That(
            async () => await grain.ReportAsync(ConsumerA, Hlc(300)),
            Throws.TypeOf<LatticeStateWriteFailedException>(),
            "the conflicted activation must fail fast rather than rewrite with its stale ETag");
        Assert.That(state.WriteAttempts, Is.EqualTo(attemptsBefore));
        Assert.That(state.StaleEtagRejections, Is.Zero);

        var (fresh, freshState, _) = await ActivateAsync(row);
        Assert.That((await fresh.GetPinsAsync())[ConsumerA], Is.EqualTo(Hlc(200)), "the conflicted write landed");

        await fresh.ReportAsync(ConsumerA, Hlc(300));

        Assert.That(row.Value!.Pins[ConsumerA], Is.EqualTo(Hlc(300)));
        Assert.That(freshState.StaleEtagRejections, Is.Zero);
    }

    [Test]
    public async Task SeedManyAsync_landed_conflict_is_a_no_op_when_the_seed_is_resent_to_a_fresh_activation()
    {
        var row = new DurableStateRow<WalMaterialiserPinState>();
        var (grain, state, _) = await ActivateAsync(row);
        state.LandThenConflictOnNextWrite();
        var seed = new[] { new MaterialiserPinReport(ConsumerA, HybridLogicalClock.Zero, -1) };

        Assert.CatchAsync<LatticeStateWriteFailedException>(() => grain.SeedManyAsync(seed));

        var (fresh, freshState, _) = await ActivateAsync(row);
        await fresh.SeedManyAsync(seed);

        Assert.Multiple(() =>
        {
            Assert.That(freshState.WriteAttempts, Is.Zero, "the landed seed is already durable, so re-seeding writes nothing");
            Assert.That(row.Value!.Offsets[ConsumerA], Is.EqualTo(-1));
            Assert.That(row.LandedWrites, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task OnDeactivateAsync_on_a_conflicted_activation_skips_the_final_flush()
    {
        var row = new DurableStateRow<WalMaterialiserPinState>();
        var (grain, state, _) = await ActivateAsync(row);
        state.LandThenConflictOnNextWrite();
        Assert.CatchAsync(() => grain.ReportAsync(ConsumerA, Hlc(100)));
        var attemptsBefore = state.WriteAttempts;

        await ((IGrainBase)grain).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ApplicationRequested, "test"), CancellationToken.None);

        Assert.That(state.WriteAttempts, Is.EqualTo(attemptsBefore),
            "a final flush with a stale ETag can only conflict again");
    }

    [Test]
    public async Task ReportAsync_bcl_write_fault_is_not_treated_as_a_conflict()
    {
        var legacy = new FakePersistentState<WalMaterialiserPinState> { ThrowOnWrite = new TimeoutException("blip") };
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("wal-materialiser-pin", Tree));
        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeOptions { WalMaterialiserPinFlushIntervalMs = 0 });
        var grain = new WalMaterialiserPinGrain(context, legacy, options);
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        Assert.ThrowsAsync<TimeoutException>(() => grain.ReportAsync(ConsumerA, Hlc(100)));
        await grain.ReportAsync(ConsumerA, Hlc(200));

        context.DidNotReceiveWithAnyArgs().Deactivate(default!);
        Assert.That(legacy.WriteCount, Is.EqualTo(1));
    }
}
