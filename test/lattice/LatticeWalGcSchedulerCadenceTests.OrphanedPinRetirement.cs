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
/// </summary>
public partial class LatticeWalGcSchedulerCadenceTests
{
    [Test]
    public async Task ExecuteAsync_retires_the_pin_of_a_leaf_that_reports_no_tree_id()
    {
        const string TreeId = "orphan-sweep";
        var reporter = Substitute.For<ILeafCursorReporter>();
        var time = new VirtualTimeProvider();

        // The fixture's options carry the default WAL partition count, so the
        // consumer id is the partition-suffixed one a leaf of such a tree
        // publishes: retirement acts only on a leaf's own id (issue #4238).
        var consumerId = BlockedConsumerId(TreeId) + "_0";

        // A drive verdict of NotDriven is exactly what a reclaimed leaf returns:
        // its state was cleared, so there is no tree to drive a checkpoint for.
        var (scheduler, recorder) = BlockedTreeProbing(
            time,
            () => Task.FromResult<string?>(null),
            consumerId: consumerId,
            treeId: TreeId,
            cursorReporter: reporter);

        using var decisions = new InstrumentRecorder(LatticeMetrics.WalGcDriveOrphanPinRetirements, TreeId);
        using (recorder)
        {
            await StartAndRunFirstPassAsync(scheduler, time);
            await AttemptsBeforeFirstAbandonmentAsync(time, recorder);
            await scheduler.StopAsync(CancellationToken.None);
        }

        await reporter.Received().UnregisterAsync(
            TreeId,
            consumerId,
            Arg.Any<CancellationToken>());

        // Issue #4246: the drive's removal is counted, with its cause.
        Assert.Multiple(() =>
        {
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
        var reporter = Substitute.For<ILeafCursorReporter>();
        var time = new VirtualTimeProvider();

        var (scheduler, recorder) = BlockedTreeProbing(
            time,
            () => Task.FromResult<string?>(TreeId),
            treeId: TreeId,
            cursorReporter: reporter);

        using (recorder)
        {
            await StartAndRunFirstPassAsync(scheduler, time);
            await AttemptsBeforeFirstAbandonmentAsync(time, recorder);
            await scheduler.StopAsync(CancellationToken.None);
        }

        await reporter.DidNotReceive().UnregisterAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ExecuteAsync_survives_a_reporter_that_refuses_the_retirement()
    {
        // Retirement is opportunistic: a registry that refuses it leaves the WAL
        // retained, which is the pre-fix behaviour, but must not take the sweep
        // down with it - the sweep is what every other blocked tree depends on.
        const string TreeId = "orphan-sweep-faulting";
        var reporter = Substitute.For<ILeafCursorReporter>();
        reporter.UnregisterAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromException(new InvalidOperationException("registry unavailable")));

        var time = new VirtualTimeProvider();

        // Suffixed as a leaf of the default (partitioned) tree publishes it, so
        // the gate admits it and the reporter's refusal is actually reached.
        // Before issue #4246 this fixture used the unsuffixed id, which the
        // #4238 gate refuses, so the throwing reporter was never called and
        // nothing said so.
        var consumerId = BlockedConsumerId(TreeId) + "_0";
        var (scheduler, recorder) = BlockedTreeProbing(
            time,
            () => Task.FromResult<string?>(null),
            consumerId: consumerId,
            treeId: TreeId,
            cursorReporter: reporter);

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
}
