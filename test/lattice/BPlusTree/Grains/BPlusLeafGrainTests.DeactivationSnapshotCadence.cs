using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #1537 (snapshot-cadence liveness gap). A
/// data-bearing leaf holds its <see cref="HybridLogicalClock.Zero"/> block pin
/// until a durable snapshot covers its checkpointed prefix. Before this fix the
/// only two snapshot-capture triggers - the activation-time advisory and the
/// per-checkpoint periodic recheck (every
/// <see cref="LatticeOptions.LeafSnapshotReClassifyEveryNCheckpoints"/>, default
/// 64) - both require the leaf to STAY activated. A short-lived bursty
/// activation (activate, take a few writes and checkpoints, deactivate before
/// the cadence threshold) fires neither, so the block pin is held forever and
/// the shared-shard WAL is retained without bound.
/// <para>
/// The fix captures a snapshot on graceful deactivation, gated on a genuine
/// this-activation checkpoint advance (<c>_checkpointAdvancedThisActivation</c>)
/// so a cold reactivation never captures an empty/partial cache and falsely
/// claims coverage (the #1535 no-loss invariant, guarded by the
/// <c>ColdRestartResidualPin</c> / <c>EmptyPartitionCoverageGate</c> fixtures).
/// These tests drive the real leaf through the real
/// <see cref="LeafCursorReporter"/> + <see cref="WalMaterialiserPinGrain"/>
/// durable pin store AND a real snapshot-storage stub (over a Guid leaf key so
/// the capture path actually runs), then assert the deactivation capture lifts
/// the block pin to a real frontier and lets the <see cref="LatticeWalGc"/> trim
/// the now-covered prefix.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static ILeafSnapshotStorageGrain CreateSucceedingSnapshotStub()
    {
        var stub = Substitute.For<ILeafSnapshotStorageGrain>();
        stub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LeafSnapshotBlob?>(null));
        return stub;
    }

    /// <summary>
    /// Like <c>CreateLeafWithDurablePinStore</c> but over a <b>Guid</b> leaf key
    /// (so <c>context.GrainId.GetGuidKey()</c> - used by the snapshot capture
    /// path - resolves instead of throwing, which the string-keyed rig relies on
    /// to make capture a no-op) and with an explicit
    /// <see cref="ILeafSnapshotStorageGrain"/> stub wired on the same factory so
    /// a capture actually persists and advances durable coverage.
    /// </summary>
    private static (BPlusLeafGrain Leaf, WalMaterialiserPinGrain PinGrain,
        InMemoryWalCursorRegistry Registry, IGrainFactory Factory, ILeafSnapshotStorageGrain SnapshotStub)
        CreateLeafWithDurablePinAndSnapshotStore(string? treeId, ILeafSnapshotStorageGrain? snapshotStub = null)
    {
        var registry = new InMemoryWalCursorRegistry();

        var pinContext = Substitute.For<IGrainContext>();
        pinContext.GrainId.Returns(GrainId.Create("wal-materialiser-pin", PinSeamTreeId));
        var pinGrain = new WalMaterialiserPinGrain(pinContext, new FakePersistentState<WalMaterialiserPinState>(), PinOptionsMonitor());

        snapshotStub ??= CreateSucceedingSnapshotStub();

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(pinGrain);
        factory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);

        var reporter = new LeafCursorReporter(registry, factory);

        var services = new ServiceCollection();
        services.AddSingleton<ILeafCursorReporter>(reporter);
        var provider = services.BuildServiceProvider();

        var leafKey = Guid.NewGuid().ToString("N");
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", leafKey));
        context.ActivationServices.Returns(provider);

        var state = new FakePersistentState<LeafNodeState>();
        if (treeId is not null)
            state.State.TreeId = treeId;

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions { WalPartitions = 1 }, maxLeafKeys: 128, shardCount: 1, factory: factory);
        var leaf = new BPlusLeafGrain(
            context, state, factory, optionsResolver, TestMutationObservers.NoObservers(), TestOriginClusterIdResolver.Default());

        return (leaf, pinGrain, registry, factory, snapshotStub);
    }

    [Test]
    public async Task Bursty_leaf_captures_snapshot_on_graceful_deactivation_and_lifts_block_pin()
    {
        var (leaf, pinGrain, _, _, snapshotStub) =
            CreateLeafWithDurablePinAndSnapshotStore(treeId: PinSeamTreeId);

        // Bursty short activation: apply data + checkpoint (routes through the
        // pending-advance branch -> latches _checkpointAdvancedThisActivation),
        // well under the periodic recheck cadence (default 64) so NO cadence
        // capture fires. This is the exact #1537 gap: on baseline the block pin
        // is now held forever.
        await CheckpointLeafAsync(leaf, "k1", hlcPhysical: 100, offset: 1);

        // Preconditions: the cadence has NOT captured yet, and the
        // data-bearing-but-uncovered leaf holds a Zero block pin.
        await snapshotStub.DidNotReceive().SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>());
        var beforePins = await pinGrain.GetPinsAsync();
        Assert.That(beforePins.Values, Has.All.EqualTo(HybridLogicalClock.Zero),
            "precondition: before deactivation the checkpointed-but-uncovered leaf holds a Zero block pin.");

        await ((IGrainBase)leaf).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            CancellationToken.None);

        // #1537: graceful deactivation must capture exactly one snapshot...
        await snapshotStub.Received(1).SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>());
        // ...which advances durable coverage so the durable-pin flush that
        // follows lifts the block pin from Zero to the leaf's real frontier.
        var afterPins = await pinGrain.GetPinsAsync();
        Assert.That(afterPins, Is.Not.Empty,
            "the deactivation pin flush must leave a durable retention pin behind.");
        Assert.That(afterPins.Values, Has.All.GreaterThan(HybridLogicalClock.Zero),
            "the snapshot captured on deactivation covers the checkpointed prefix, lifting the block pin to a real frontier.");
    }

    [Test]
    public async Task Snapshot_captured_on_deactivation_lets_wal_gc_trim_covered_prefix_after_restart()
    {
        var (leaf, _, _, factory, _) =
            CreateLeafWithDurablePinAndSnapshotStore(treeId: PinSeamTreeId);

        // Bursty activation: checkpoint at offset 1 (HLC 20), then a graceful
        // deactivation captures a snapshot covering the prefix [0, 1].
        await CheckpointLeafAsync(leaf, "k1", hlcPhysical: 20, offset: 1);
        await ((IGrainBase)leaf).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            CancellationToken.None);

        // Simulate a restart: wipe the process-local registry so only the
        // durable pin (now a REAL frontier at offset 1, not a Zero block pin)
        // survives.
        var freshRegistry = new InMemoryWalCursorRegistry();

        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            PinSeamTreeId,
            0,
            new[]
            {
                PinWalEntry(0, PinHlc(10)),
                PinWalEntry(1, PinHlc(20)),
                PinWalEntry(2, PinHlc(30)),
                PinWalEntry(3, PinHlc(40)),
            },
            CancellationToken.None);

        // A forward consumer sits at the WAL head.
        await freshRegistry.ReportCursorAsync(PinSeamTreeId, "shipper", PinHlc(40));

        var gcServices = new ServiceCollection();
        gcServices.AddSingleton<IWalStorageProvider>(provider);
        gcServices.AddSingleton(factory);
        var gc = new LatticeWalGc(gcServices.BuildServiceProvider(), freshRegistry, PinOptionsMonitor());

        var report = await gc.RunOnceAsync(PinSeamTreeId);

        // Contrast with the uncovered leaf (Zero block pin -> MinCursor null,
        // whole WAL retained; see Dormant_leaf_pin_survives_registry_wipe...):
        // here the deactivation snapshot lifted the block, so the durable pin is
        // a real frontier that FLOORS the trim at the checkpoint rather than
        // disabling it.
        Assert.That(report.MinCursor, Is.Not.Null,
            "#1537: a snapshot captured on deactivation lifts the block pin, so the cursor trim is enabled (not disabled).");

        var survivors = await SurvivingPinOffsetsAsync(provider);
        Assert.That(survivors, Does.Not.Contain(0L).And.Not.Contain(1L),
            "the snapshot-covered prefix [0, 1] must now be trimmable, closing the retention-growth gap.");
        Assert.That(survivors, Does.Contain(2L).And.Contain(3L),
            "the still-uncovered WAL tail above the checkpoint offset must be retained.");
    }

    [Test]
    public async Task Deactivation_snapshot_store_failure_retains_block_pin()
    {
        var failingStub = Substitute.For<ILeafSnapshotStorageGrain>();
        failingStub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LeafSnapshotBlob?>(null));
        failingStub.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
            .Throws(new InvalidOperationException("snapshot store down"));

        var (leaf, pinGrain, _, _, _) =
            CreateLeafWithDurablePinAndSnapshotStore(treeId: PinSeamTreeId, snapshotStub: failingStub);

        await CheckpointLeafAsync(leaf, "k1", hlcPhysical: 100, offset: 1);
        await ((IGrainBase)leaf).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            CancellationToken.None);

        // The capture attempt threw and was swallowed (best-effort); coverage
        // did NOT advance, so the durable pin stays a Zero block pin - the WAL
        // is retained, never trimmed ahead of durable coverage (#1535 no-loss
        // invariant). A failed deactivation snapshot must degrade to the safe,
        // pre-#1537 retention behaviour, not to data loss.
        var pins = await pinGrain.GetPinsAsync();
        Assert.That(pins, Is.Not.Empty, "the durable block pin must remain after a failed capture.");
        Assert.That(pins.Values, Has.All.EqualTo(HybridLogicalClock.Zero),
            "a failed deactivation snapshot must not advance coverage or lift the block pin.");
    }
}
