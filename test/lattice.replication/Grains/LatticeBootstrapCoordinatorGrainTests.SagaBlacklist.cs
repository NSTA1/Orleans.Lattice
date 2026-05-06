using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Coverage of the saga-blacklist plumbing through the receiver-side
/// bootstrap state machine: <see cref="LatticeBootstrapCoordinatorGrain.DrainSnapshotAsync"/>
/// captures <see cref="SnapshotStream.SagaBlacklist"/> into
/// <see cref="BootstrapCoordinatorState.SagaBlacklist"/>;
/// <see cref="LatticeBootstrapCoordinatorGrain.PinAndCompleteAsync"/>
/// registers it with the per-tree
/// <see cref="IReplicationTxBufferGrain"/> on transition to
/// <see cref="LatticeBootstrapState.LiveIncremental"/>.
/// </summary>
public partial class LatticeBootstrapCoordinatorGrainTests
{
    private static (
        LatticeBootstrapCoordinatorGrain Grain,
        FakePersistentState<BootstrapCoordinatorState> State,
        IGrainFactory Factory,
        ISnapshotProvider Provider,
        IReplicationTxBufferGrain TxBuffer) CreateWithBuffer(
            FakePersistentState<BootstrapCoordinatorState>? existingState = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("bootstrap-coordinator", Tree));
        var factory = Substitute.For<IGrainFactory>();
        var provider = Substitute.For<ISnapshotProvider>();
        var reminders = Substitute.For<IReminderRegistry>();
        var apply = Substitute.For<IReplicationApplyGrain>();
        var hwm = Substitute.For<IReplicationHighWaterMarkGrain>();
        var txBuffer = Substitute.For<IReplicationTxBufferGrain>();
        factory.GetGrain<IReplicationApplyGrain>(Arg.Any<string>()).Returns(apply);
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Arg.Any<string>()).Returns(hwm);
        factory.GetGrain<IReplicationTxBufferGrain>(Arg.Any<string>()).Returns(txBuffer);
        var fakeState = existingState ?? new FakePersistentState<BootstrapCoordinatorState>();
        var grain = new LatticeBootstrapCoordinatorGrain(
            context, factory, provider, reminders,
            NullLogger<LatticeBootstrapCoordinatorGrain>.Instance, fakeState);
        return (grain, fakeState, factory, provider, txBuffer);
    }

    // --- DrainSnapshotAsync captures SagaBlacklist ---

    [Test]
    public async Task DrainSnapshot_captures_empty_blacklist_into_persisted_state()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.RequestingSnapshot);
        var (grain, _, _, provider, _) = CreateWithBuffer(fake);

        provider.ExportAsync(Tree, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(MakeStream(Hlc(10), new VersionVector())));

        await grain.ProcessNextPhaseAsync();

        Assert.That(fake.State.SagaBlacklist, Is.Not.Null);
        Assert.That(fake.State.SagaBlacklist, Is.Empty);
    }

    [Test]
    public async Task DrainSnapshot_captures_non_empty_blacklist_into_persisted_state()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.RequestingSnapshot);
        var (grain, _, _, provider, _) = CreateWithBuffer(fake);

        var tx1 = Guid.NewGuid();
        var tx2 = Guid.NewGuid();
        provider.ExportAsync(Tree, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new SnapshotStream(
                Tree, Hlc(10), new VersionVector(), Stream(),
                sagaBlacklist: new[] { tx1, tx2 })));

        await grain.ProcessNextPhaseAsync();

        Assert.That(fake.State.SagaBlacklist, Is.EqualTo(new[] { tx1, tx2 }));
    }

    [Test]
    public async Task DrainSnapshot_replaces_existing_blacklist_on_re_export()
    {
        // A re-export from a fresh ExportAsync call yields a fresh
        // blacklist computed against the fresh quiesce window.
        // Replace semantics — the prior export's blacklist is not
        // authoritative.
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.RequestingSnapshot);
        var stale = Guid.NewGuid();
        fake.State.SagaBlacklist = new List<Guid> { stale };
        var (grain, _, _, provider, _) = CreateWithBuffer(fake);

        var fresh = Guid.NewGuid();
        provider.ExportAsync(Tree, HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new SnapshotStream(
                Tree, Hlc(10), new VersionVector(), Stream(),
                sagaBlacklist: new[] { fresh })));

        await grain.ProcessNextPhaseAsync();

        Assert.That(fake.State.SagaBlacklist, Is.EqualTo(new[] { fresh }));
        Assert.That(fake.State.SagaBlacklist, Does.Not.Contain(stale));
    }

    // --- PinAndCompleteAsync registers with IReplicationTxBufferGrain ---

    [Test]
    public async Task PinAndComplete_skips_register_call_when_blacklist_is_empty()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.IncrementalHandoff);
        fake.State.SnapshotAsOfHlc = Hlc(50);
        fake.State.CausalStableFrontier = new VersionVector();
        fake.State.SagaBlacklist = new List<Guid>();
        var (grain, _, _, _, txBuffer) = CreateWithBuffer(fake);

        await grain.ProcessNextPhaseAsync();

        await txBuffer.DidNotReceive().RegisterBlacklistedTransactionsAsync(
            Arg.Any<IReadOnlyList<Guid>>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PinAndComplete_registers_non_empty_blacklist_with_tx_buffer()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.IncrementalHandoff);
        fake.State.SnapshotAsOfHlc = Hlc(50);
        fake.State.CausalStableFrontier = new VersionVector();
        var tx1 = Guid.NewGuid();
        var tx2 = Guid.NewGuid();
        fake.State.SagaBlacklist = new List<Guid> { tx1, tx2 };
        var (grain, _, _, _, txBuffer) = CreateWithBuffer(fake);

        await grain.ProcessNextPhaseAsync();

        await txBuffer.Received(1).RegisterBlacklistedTransactionsAsync(
            Arg.Is<IReadOnlyList<Guid>>(list =>
                list.Count == 2 && list.Contains(tx1) && list.Contains(tx2)),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PinAndComplete_advances_phase_to_LiveIncremental_after_register()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.IncrementalHandoff);
        fake.State.SnapshotAsOfHlc = Hlc(50);
        fake.State.CausalStableFrontier = new VersionVector();
        fake.State.SagaBlacklist = new List<Guid> { Guid.NewGuid() };
        var (grain, _, _, _, _) = CreateWithBuffer(fake);

        await grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(fake.State.Phase, Is.EqualTo(LatticeBootstrapState.LiveIncremental));
            Assert.That(fake.State.InProgress, Is.False);
        });
    }

    [Test]
    public async Task SagaBlacklist_survives_crash_via_persisted_state_default()
    {
        // Default-constructed BootstrapCoordinatorState (legacy
        // pre-R-102 persisted state with no SagaBlacklist slot in
        // its persisted bytes) must decode the field as a non-null
        // empty list rather than null, so PinAndCompleteAsync's
        // null-tolerant {Count: > 0} check is safe on cold replay.
        var freshState = new BootstrapCoordinatorState();
        Assert.That(freshState.SagaBlacklist, Is.Not.Null);
        Assert.That(freshState.SagaBlacklist, Is.Empty);
    }
}
