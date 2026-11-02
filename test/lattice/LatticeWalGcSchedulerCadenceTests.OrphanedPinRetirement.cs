using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Regression fixture for the WAL GC half of issue #3101: the sweep must retire
/// a materialiser pin whose leaf reports no bound tree id, rather than counting
/// the touch as a success and re-arming on it forever.
/// <para>
/// The leaf-side fix (retiring pins during reclaim) stops new orphans being
/// created; it does nothing for the ones already registered in a deployment
/// that ran the defective build. This arm is what drains them, which is why the
/// scheduler needs a retirement seam of its own rather than relying on the leaf
/// to have done the right thing on its way out.
/// </para>
/// <para>
/// Since issue #5358 the drive's NotDriven verdict only starts the retirement:
/// the removal runs the orphan sweep's fenced sequence - a retirement token and
/// a pin read from every read key, a fresh durable read of the leaf, then
/// removals the pin grain refuses if the consumer was merged since. Every
/// fixture here therefore serves the pin store and the leaf's durable row, and
/// stubs the orphan sweep's coordinator so that the sweep itself never runs and
/// every removal observed is the drive's.
/// </para>
/// </summary>
public partial class LatticeWalGcSchedulerCadenceTests
{
    [Test]
    public async Task ExecuteAsync_retires_the_pin_of_a_leaf_that_reports_no_tree_id()
    {
        const string TreeId = "orphan-sweep";
        var time = new VirtualTimeProvider();

        // The fixture's options carry the default WAL partition count, so the
        // consumer id is the partition-suffixed one a leaf of such a tree
        // publishes: retirement acts only on a leaf's own id (issue #4238).
        var consumerId = BlockedConsumerId(TreeId) + "_0";
        var pins = DrivePinStore(TreeId, consumerId);
        var storage = new LeafStateBook();
        storage.PutHusk(BlockedLeafGrainId());

        // A drive verdict of NotDriven is exactly what a reclaimed leaf returns:
        // its state was cleared, so there is no tree to drive a checkpoint for.
        var (scheduler, recorder) = BlockedTreeProbing(
            time,
            () => Task.FromResult<string?>(null),
            consumerId: consumerId,
            treeId: TreeId,
            cursorReporter: Substitute.For<ILeafCursorReporter>(),
            pins: pins,
            leafStateStorage: storage,
            orphanSweepCoordinator: InertSweepCoordinator());

        using var decisions = new InstrumentRecorder(LatticeMetrics.WalGcDriveOrphanPinRetirements, TreeId);
        using (recorder)
        {
            await StartAndRunFirstPassAsync(scheduler, time);
            await AttemptsBeforeFirstAbandonmentAsync(time, recorder);
            await scheduler.StopAsync(CancellationToken.None);
        }

        // Issue #4246: the drive's removal is counted, with its cause.
        Assert.Multiple(() =>
        {
            Assert.That(pins.Removals.Select(r => r.ConsumerId), Does.Contain(consumerId),
                "the orphan's pin must be removed from the keys it was found under.");
            Assert.That(pins.RemainingPins, Is.Zero, "the pin must be gone from every read key.");
            Assert.That(DriveDecisions(decisions, "retired", "not_driven"), Is.GreaterThan(0));
            Assert.That(DriveDecisions(decisions, "retire_failed"), Is.Zero);
            Assert.That(DriveDecisions(decisions, "refused_malformed_id"), Is.Zero);
            Assert.That(DriveDecisions(decisions, "refused_ambiguous_partition"), Is.Zero);
        });
    }

    [Test]
    public async Task ExecuteAsync_retires_no_pin_for_a_leaf_that_drives_normally()
    {
        // The negative control. Without it the assertion above cannot tell
        // "retires orphans" from "retires everything it touches", and the second
        // would discard WAL that live leaves still depend on.
        const string TreeId = "orphan-sweep-control";
        var time = new VirtualTimeProvider();

        // Suffixed exactly as in the positive test above. Issue #4258: with the
        // unsuffixed id the #4238 retirement gate refuses the id before any
        // removal, so this control held even under "retires everything it
        // touches" - the very defect it exists to rule out.
        var consumerId = BlockedConsumerId(TreeId) + "_0";
        var pins = DrivePinStore(TreeId, consumerId);
        var storage = new LeafStateBook();
        storage.PutHusk(BlockedLeafGrainId());
        var (scheduler, recorder) = BlockedTreeProbing(
            time,
            () => Task.FromResult<string?>(TreeId),
            consumerId: consumerId,
            treeId: TreeId,
            cursorReporter: Substitute.For<ILeafCursorReporter>(),
            pins: pins,
            leafStateStorage: storage,
            orphanSweepCoordinator: InertSweepCoordinator());

        using var decisions = new InstrumentRecorder(LatticeMetrics.WalGcDriveOrphanPinRetirements, TreeId);
        int attempted;
        using (recorder)
        {
            await StartAndRunFirstPassAsync(scheduler, time);
            attempted = await AttemptsBeforeFirstAbandonmentAsync(time, recorder);
            await scheduler.StopAsync(CancellationToken.None);
        }

        Assert.Multiple(() =>
        {
            Assert.That(attempted, Is.GreaterThan(0),
                "anti-vacuity: the leaf must actually have been driven.");
            Assert.That(DriveDecisions(decisions, "refused_malformed_id"), Is.Zero,
                "anti-vacuity: the gate must accept this id, or a retirement could never be observed.");
            Assert.That(DriveDecisions(decisions, "refused_ambiguous_partition"), Is.Zero,
                "anti-vacuity: the gate must accept this id, or a retirement could never be observed.");
            Assert.That(DriveDecisions(decisions, "retired"), Is.Zero,
                "a leaf that drives normally must keep its pin.");
            Assert.That(pins.Removals, Is.Empty, "a leaf that drives normally must keep its pin.");
        });
    }

    [Test]
    public async Task ExecuteAsync_survives_a_pin_store_that_refuses_the_retirement()
    {
        // Retirement is opportunistic: a store that refuses it leaves the WAL
        // retained, which is the pre-fix behaviour, but must not take the sweep
        // down with it - the sweep is what every other blocked tree depends on.
        const string TreeId = "orphan-sweep-faulting";
        var time = new VirtualTimeProvider();

        // Suffixed as a leaf of the default (partitioned) tree publishes it, so
        // the gate admits it and the store's refusal is actually reached.
        // Before issue #4246 this fixture used the unsuffixed id, which the
        // #4238 gate refuses, so the throwing store was never called and
        // nothing said so.
        var consumerId = BlockedConsumerId(TreeId) + "_0";
        var pins = DrivePinStore(TreeId, consumerId);
        pins.RemoveThrows = new InvalidOperationException("pin store unavailable");
        var storage = new LeafStateBook();
        storage.PutHusk(BlockedLeafGrainId());
        var (scheduler, recorder) = BlockedTreeProbing(
            time,
            () => Task.FromResult<string?>(null),
            consumerId: consumerId,
            treeId: TreeId,
            cursorReporter: Substitute.For<ILeafCursorReporter>(),
            pins: pins,
            leafStateStorage: storage,
            orphanSweepCoordinator: InertSweepCoordinator());

        using var decisions = new InstrumentRecorder(LatticeMetrics.WalGcDriveOrphanPinRetirements, TreeId);
        using (recorder)
        {
            await StartAndRunFirstPassAsync(scheduler, time);
            var attempted = await AttemptsBeforeFirstAbandonmentAsync(time, recorder);
            await scheduler.StopAsync(CancellationToken.None);

            Assert.Multiple(() =>
            {
                Assert.That(attempted, Is.GreaterThan(0),
                    "the sweep must keep running, or the assertion below is vacuous.");
                Assert.That(Outcomes(recorder, "orphaned"), Is.GreaterThan(0),
                    "a failed retirement must still be classified as orphaned, or the touch reverts to counting as a success.");
                Assert.That(DriveDecisions(decisions, "retire_failed", "not_driven"), Is.GreaterThan(0),
                    "issue #4246: a removal that threw is its own counted branch.");
                Assert.That(DriveDecisions(decisions, "retired"), Is.Zero,
                    "a removal that threw must not be counted as a retirement.");
            });
        }
    }

    /// <summary>
    /// Sums the non-priming measurements of one arm of
    /// <see cref="LatticeMetrics.WalGcDriveOrphanPinRetirements"/>, optionally
    /// restricted to one cause.
    /// </summary>
    private static long DriveDecisions(InstrumentRecorder recorder, string status, string? cause = null) =>
        (long)recorder.Counted
            .Where(m => (m.Tag(LatticeMetrics.TagStatus) as string) == status)
            .Where(m => cause is null || (m.Tag(LatticeMetrics.TagPinRetirementCause) as string) == cause)
            .Sum(m => m.Value);

    /// <summary>
    /// A pin store holding <paramref name="consumerId"/>'s pin under every read
    /// key of <paramref name="treeId"/> under the fixture's options: one logical
    /// pin duplicated across keys, as an earlier routing leaves it.
    /// </summary>
    private static FakePinStore DrivePinStore(string treeId, string consumerId)
    {
        var pins = new FakePinStore();
        var keys = WalMaterialiserPinRouting.EnumerateReadKeys(
            treeId,
            WalMaterialiserPinRouting.ResolveShardCount(Monitor(Adaptive(floor: SweepPass))));
        foreach (var key in keys)
        {
            pins.Seed(key, consumerId);
        }

        return pins;
    }

    /// <summary>
    /// An orphan sweep coordinator that accepts every request and sweeps
    /// nothing, so the only retirement a fixture observes is the drive's.
    /// </summary>
    private static IWalOrphanPinSweepCoordinator InertSweepCoordinator()
    {
        var coordinator = Substitute.For<IWalOrphanPinSweepCoordinator>();
        coordinator.RequestSweepAsync(Arg.Any<string>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<WalOrphanPinSweepOutcome?>(null));
        return coordinator;
    }
}
