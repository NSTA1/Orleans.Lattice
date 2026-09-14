using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Regression tests for issue #2692 Half B: the blocked-leaf sweep must
/// <b>drive</b> a starved leaf's replay rather than merely touch it, and must
/// report per leaf what the drive achieved.
/// </summary>
/// <remarks>
/// <para>
/// <b>The defect.</b> The per-partition checkpoint advance is reached from
/// exactly one call site in the whole solution, inside
/// <c>OnActivateAsync</c>. The sweep touched the blocking leaf with a read-only
/// <c>GetTreeIdAsync</c>, on the reasoning that "the work is done by activation,
/// not by the call". That is correct for a <i>dormant</i> leaf and is precisely
/// the defect for a live one: an already-active leaf answers the call
/// immediately, never re-enters activation, and so never advances its
/// checkpoint. Its durable materialiser pin stays at the sentinel forever and
/// its tree can never trim.
/// </para>
/// <para>
/// <b>Why the old fixture could not catch it.</b> The pre-existing tests
/// asserted <c>leaf.Received().GetTreeIdAsync()</c> - that the touch was
/// <i>issued</i>. That is a proxy for repair, not the property itself, and the
/// correlation breaks in exactly the case that matters: the call is delivered,
/// returns cleanly, is recorded as <c>Completed</c>, and achieves nothing. The
/// sweep was not failing loudly, it was succeeding vacuously, and every
/// assertion in the suite stayed green while it did. These tests assert the
/// verdict instead.
/// </para>
/// </remarks>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    /// <summary>
    /// Builds a blocked-leaf factory whose drive returns a chosen verdict, so a
    /// test can pin one arm without restating the harness.
    /// </summary>
    private static (IGrainFactory Factory, IBPlusLeafGrain Leaf) FactoryWithDriveVerdict(
        LeafStarvationDriveOutcome verdict)
    {
        var (factory, leaf) = FactoryWithBlockedLeaf(StrandedTree);
        leaf.DriveStarvedCheckpointAsync().Returns(_ => Task.FromResult(verdict));
        return (factory, leaf);
    }

    /// <summary>
    /// Runs the sweep against a permanently blocked tree until it has touched
    /// the leaf at least once, and returns the recorder holding the verdicts.
    /// </summary>
    private static async Task<InstrumentRecorder> SweepOnceAsync(
        IGrainFactory factory,
        VirtualTimeProvider time)
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNaming(BlockedConsumerId())));

        var recorder = new InstrumentRecorder(
            LatticeMetrics.WalGcBlockedLeafReactivations, StrandedTree);
        var scheduler = CreateScheduler(factory, gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(6));
        await scheduler.StopAsync(CancellationToken.None);
        return recorder;
    }

    [Test]
    public async Task ExecuteAsync_drives_replay_rather_than_only_touching_the_blocking_leaf()
    {
        // The reach argument, asserted directly, and the whole of issue #2692
        // Half B in one test. A read-only touch cannot advance a checkpoint on
        // a leaf that is already active, because the only call site that
        // advances one is inside the activation hook. So the sweep must issue
        // the call that does the work rather than the call that merely arrives.
        //
        // Both halves are asserted. Received(DriveStarvedCheckpointAsync) alone
        // would stay green if the sweep also kept the old touch, and that is
        // not a harmless redundancy: it would mean the sweep still reactivates
        // through a path whose verdict nothing inspects.
        var time = new VirtualTimeProvider();
        var (factory, leaf) = FactoryWithDriveVerdict(LeafStarvationDriveOutcome.Lifted);

        using var recorder = await SweepOnceAsync(factory, time);

        await leaf.Received(1).DriveStarvedCheckpointAsync();
        await leaf.DidNotReceive().GetTreeIdAsync();
    }

    [Test]
    public async Task ExecuteAsync_records_drove_lifted_when_the_drive_lifted_the_pin()
    {
        var time = new VirtualTimeProvider();
        var (factory, _) = FactoryWithDriveVerdict(LeafStarvationDriveOutcome.Lifted);

        using var recorder = await SweepOnceAsync(factory, time);

        Assert.Multiple(() =>
        {
            Assert.That(Outcomes(recorder, "drove_lifted"), Is.EqualTo(1),
                "a drive that lifted the pin is the one affirmative reading the sweep can produce.");
            Assert.That(Outcomes(recorder, "attempted"), Is.EqualTo(1),
                "the cost series stays complete: a verdict is emitted alongside 'attempted', never instead of it.");
        });
    }

    /// <summary>
    /// The five arms the drive verdicts land on, in enum order.
    /// </summary>
    private static readonly string[] StarvationDriveArms =
    [
        "drove_lifted", "drove_no_advance", "drove_memory_refused",
        "drove_not_driven", "drove_already_driving",
    ];

    /// <summary>
    /// The arm each verdict is required to land on, written out by hand.
    /// </summary>
    /// <remarks>
    /// Deliberately <b>not</b> derived from <c>DriveOutcomeTag</c>, which is the
    /// mapping under test. A matrix whose diagonal is read from the production
    /// mapping moves its own expectation whenever that mapping moves, so a build
    /// that collapsed every verdict onto one arm would relabel the diagonal to
    /// match and pass - proving only that the mapping is self-consistent. An
    /// independent expectation is what makes the collapse redden. The
    /// exhaustiveness of this table against the enum is asserted rather than
    /// assumed, so a verdict added later cannot quietly go unasserted.
    /// </remarks>
    private static readonly Dictionary<LeafStarvationDriveOutcome, string> ExpectedDriveArm = new()
    {
        [LeafStarvationDriveOutcome.Lifted] = "drove_lifted",
        [LeafStarvationDriveOutcome.NoAdvance] = "drove_no_advance",
        [LeafStarvationDriveOutcome.MemoryRefused] = "drove_memory_refused",
        [LeafStarvationDriveOutcome.NotDriven] = "drove_not_driven",
        [LeafStarvationDriveOutcome.AlreadyDriving] = "drove_already_driving",
    };

    /// <summary>
    /// Sweeps a blocked tree whose leaf returns <paramref name="verdict"/> and
    /// returns how many times each drive arm fired.
    /// </summary>
    private static async Task<Dictionary<string, int>> DriveArmCountsAsync(
        LeafStarvationDriveOutcome verdict)
    {
        var time = new VirtualTimeProvider();
        var (factory, _) = FactoryWithDriveVerdict(verdict);

        using var recorder = await SweepOnceAsync(factory, time);

        return StarvationDriveArms.ToDictionary(arm => arm, arm => Outcomes(recorder, arm));
    }

    [Test]
    public async Task ExecuteAsync_records_each_drive_verdict_on_its_own_arm_and_no_other()
    {
        // A 5x5 identity matrix rather than five independent tests, for the
        // reason issue #2942 makes explicit: asserting an absence is not a
        // detector, because a broken arm and a correct-and-quiet one are the
        // same observation. Here each off-diagonal zero is licensed by the
        // diagonal entry in the SAME matrix - the run that shows the arm can
        // fire is the run that shows the others did not - so no zero is an
        // unearned one and no separate positive control has to be maintained.
        //
        // This replaced four per-verdict tests. Two of their zero clauses were
        // not detectors: nothing in the perturbation matrix could make
        // 'drove_no_advance' non-zero while a sibling arm was asserted zero, so
        // those clauses would have stayed green against a build that never
        // emitted the arm at all, and equally against a typo in the test's own
        // tag literal. Reading counts through the dictionary INDEXER rather
        // than GetValueOrDefault closes the second of those: a mistyped arm
        // throws KeyNotFoundException instead of counting a comfortable zero.
        //
        // The verdicts these arms separate call for opposite operator
        // responses, which is why folding any pair together is a defect and not
        // a simplification. A leaf that drove and lifted nothing is blocked on
        // something structural and more attempts will not help. One refused for
        // heap pressure was never given its chance and is worth retrying once
        // pressure lifts. One already driving is sweep contention rather than
        // leaf starvation. One not driven at all means the blocking report and
        // the grain disagree about what is there. Folded together, a transient
        // resource stall reads as a permanent structural block - the reading
        // that would stop anyone looking further.
        var verdicts = Enum.GetValues<LeafStarvationDriveOutcome>();

        // Method Rule 2: assert the input count before reading any result, so a
        // matrix that scanned nothing cannot report clean.
        Assert.Multiple(() =>
        {
            Assert.That(verdicts, Is.Not.Empty,
                "with no verdicts the matrix below is vacuously satisfied.");
            Assert.That(ExpectedDriveArm.Keys, Is.EquivalentTo(verdicts),
                "every declared verdict needs an expected arm, or a verdict added later goes unasserted while this fixture stays green.");
            Assert.That(ExpectedDriveArm.Values, Is.EquivalentTo(StarvationDriveArms),
                "and every arm needs a verdict that reaches it, or an arm is charted and never proven reachable.");
        });

        var matrix = new Dictionary<LeafStarvationDriveOutcome, Dictionary<string, int>>();
        foreach (var verdict in verdicts)
        {
            matrix[verdict] = await DriveArmCountsAsync(verdict);
        }

        Assert.Multiple(() =>
        {
            foreach (var (verdict, counts) in matrix)
            {
                var own = ExpectedDriveArm[verdict];
                foreach (var arm in StarvationDriveArms)
                {
                    if (string.Equals(arm, own, StringComparison.Ordinal))
                    {
                        Assert.That(counts[arm], Is.GreaterThan(0),
                            $"a leaf returning '{verdict}' must advance the '{arm}' arm, or that arm's zeros elsewhere are structural rather than measured.");
                    }
                    else
                    {
                        Assert.That(counts[arm], Is.Zero,
                            $"a leaf returning '{verdict}' must not advance the '{arm}' arm, or the verdicts are not separable.");
                    }
                }
            }
        });
    }

    [Test]
    public async Task ExecuteAsync_records_exactly_one_drive_arm_per_driven_touch()
    {
        // The conservation half. The matrix above proves each verdict reaches
        // its own arm and no sibling; this proves it reaches exactly one arm
        // rather than none or several, which is the property that makes the
        // five arms readable as a partition instead of five loose counters.
        // Under-counting is the failure it exists to catch, and under-counting
        // is invisible to a per-arm assertion.
        foreach (var verdict in Enum.GetValues<LeafStarvationDriveOutcome>())
        {
            var time = new VirtualTimeProvider();
            var (factory, leaf) = FactoryWithDriveVerdict(verdict);

            using var recorder = await SweepOnceAsync(factory, time);

            await leaf.Received(1).DriveStarvedCheckpointAsync();
            var total = StarvationDriveArms.Sum(arm => Outcomes(recorder, arm));
            Assert.That(total, Is.EqualTo(1),
                $"one drive returning '{verdict}' must be counted on exactly one arm.");
        }
    }
}
