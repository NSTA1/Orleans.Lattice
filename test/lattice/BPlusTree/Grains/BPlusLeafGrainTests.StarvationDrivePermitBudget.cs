using System.Collections.Concurrent;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for the WAL replay permit gate deadlock (issue #3065),
/// the root cause behind epic #2368.
/// <para>
/// <c>DriveStarvedCheckpointAsync</c> - the WAL GC scheduler's own starvation
/// drive - took a permit from the per-silo replay concurrency gate and then
/// passed <see cref="CancellationToken.None"/> to every await inside the
/// permit-guarded region. <c>None</c> propagates all the way into the slice-read
/// loop, whose only escape is a <c>ThrowIfCancellationRequested</c> that is
/// inert under it, so a drive parked in host-supplied storage held its permit
/// for the lifetime of the process. The gate is a <see cref="SemaphoreSlim"/>
/// sized once on first use and never re-created or topped up, so the loss is
/// permanent: a production silo was measured at ceiling 2, available 0, with 345
/// activations queued and not one acquisition in 102 seconds.
/// </para>
/// <para>
/// <b>What these tests assert, and why the obvious assertion is worthless.</b>
/// A test that merely proved the drive returns would prove nothing - the method
/// returning is not the property that broke. The property that broke is that the
/// permit never came back, so the load-bearing assertion here reads the gate's
/// available count <i>while the fake provider is still parked</i>. Sampling it
/// after releasing the park would measure a provider that recovered, which is
/// the one scenario the defect never had trouble with.
/// </para>
/// <para>
/// Every fixture below drives the abandonment through <c>GrowingWal.OnRead</c>,
/// parked on a <see cref="TaskCompletionSource"/> that is never completed until
/// the assertions have run. That hook sits inside <c>ReadSliceAsync</c>, which
/// is the await reached on every partition of every replay, so it stands in for
/// any storage seam that stops answering.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// Budget used by every abandonment fixture here. Long enough that the drive
    /// genuinely reaches the parked read on a loaded CI agent, short enough that
    /// the abandonment is observable in a unit test rather than in five minutes.
    /// </summary>
    /// <remarks>
    /// The production default is deliberately not used. Its derivation is about
    /// how slow a legitimate replay may be against real storage; this constant is
    /// about how long a test may take. Conflating them is how a fixture ends up
    /// either flaky or five minutes long.
    /// </remarks>
    private static readonly TimeSpan TestStarvationDriveBudget = TimeSpan.FromMilliseconds(400);

    private static string UniqueStarvationDriveTree() => $"starvation-drive-{Guid.NewGuid():N}";

    /// <summary>
    /// Warms the process-wide replay gate and returns it with a quiescent
    /// baseline count.
    /// </summary>
    /// <remarks>
    /// The gate is created lazily by the first activation that resolves options,
    /// so a baseline read before any activation would find no gate at all. This
    /// mirrors the warm-up the issue #2256 permit-leak fixture performs, for the
    /// same reason.
    /// </remarks>
    private static async Task<(SemaphoreSlim Gate, int Baseline)> WarmReplayGateAsync()
    {
        var warmWal = new GrowingWal();
        var (warmGrain, warmState, _, _) = CreateGrainWithMaterialiser(
            warmWal.Coordinator,
            treeId: UniqueStarvationDriveTree(),
            persistedCheckpoint: -1L);
        _ = warmState;
        await ActivateAsync(warmGrain);

        var gate = BPlusLeafGrain.ReplayConcurrencyGateForTest;
        Assert.That(gate, Is.Not.Null,
            "a completed activation with a tree id must have sized the process-wide replay gate; "
            + "without it every assertion in this fixture would be reading a null");
        return (gate!, gate!.CurrentCount);
    }

    /// <summary>
    /// Builds an activated leaf whose next slice read parks forever, and returns
    /// the park handle plus the gate sample taken at the instant the read is
    /// entered.
    /// </summary>
    private static async Task<(BPlusLeafGrain Grain, TaskCompletionSource Park, Func<int> ObservedWhileParked)>
        CreateLeafParkedInsideTheDriveAsync(SemaphoreSlim gate)
    {
        var wal = new GrowingWal();
        var (grain, _, _, _) = CreateGrainWithMaterialiser(
            wal.Coordinator,
            treeId: UniqueStarvationDriveTree(),
            persistedCheckpoint: -1L,
            starvationDriveBudget: TestStarvationDriveBudget);

        // Activate against an empty WAL first. The hook must be installed AFTER
        // activation, or it would park the activation replay this fixture
        // depends on rather than the drive under test.
        await ActivateAsync(grain);
        wal.GrowTo(3);

        var park = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var observed = -1;
        wal.OnRead = () =>
        {
            // First entry only. The abandoned drive continues detached and may
            // re-enter, and a last-writer-wins probe would then sample a window
            // in which the permit had already been returned - reporting that no
            // permit was ever held and quietly voiding the assertion below.
            if (observed < 0)
            {
                observed = gate.CurrentCount;
            }

            return park.Task;
        };

        return (grain, park, () => observed);
    }

    [Test]
    public async Task DriveStarvedCheckpointAsync_releases_its_replay_permit_while_the_slice_read_is_still_parked()
    {
        var (gate, baseline) = await WarmReplayGateAsync();
        var (grain, park, observedWhileParked) = await CreateLeafParkedInsideTheDriveAsync(gate);

        var verdict = await grain.DriveStarvedCheckpointAsync();

        try
        {
            Assert.Multiple(() =>
            {
                // Instrument validation, exactly as the issue #2256 fixture does
                // it. Without this clause the gate assertion below would pass
                // against broken source whenever the drive happened to abandon
                // BEFORE taking a permit - no permit taken is no permit leaked,
                // and the test would be proving nothing.
                Assert.That(observedWhileParked(), Is.EqualTo(baseline - 1),
                    "the parked read must be entered while the drive holds a replay permit - otherwise "
                    + "this fixture proves nothing about the leak, because no permit was ever taken");

                Assert.That(park.Task.IsCompleted, Is.False,
                    "THE PRECONDITION OF THIS WHOLE FIXTURE: the fake storage provider must still be "
                    + "hung at the moment the gate is sampled. A provider that recovered is the one "
                    + "case the defect never had trouble with, so sampling after the park is released "
                    + "would measure recovery rather than abandonment");

                Assert.That(gate.CurrentCount, Is.EqualTo(baseline),
                    "THE LOAD-BEARING ASSERTION. The permit must be back in the gate while the storage "
                    + "read that consumed it is STILL parked. The gate is sized once per process and "
                    + "never topped up, so a permit lost here is a permanent reduction in leaf "
                    + "activation concurrency, and exhaustion presents as a silent hang on WaitAsync "
                    + "rather than as an error - which is exactly what issue #3065 measured at "
                    + "available 0 with 345 activations queued");

                Assert.That(verdict, Is.EqualTo(LeafStarvationDriveOutcome.TimedOut),
                    "an abandoned drive must report its own verdict. Folding it into NoAdvance would "
                    + "tell the sweep the leaf was reached and had nothing to give, when in fact the "
                    + "leaf was never reached at all - the misreading that kept epic #2368 open");
            });
        }
        finally
        {
            park.TrySetResult();
        }
    }

    [Test]
    public async Task DriveStarvedCheckpointAsync_lets_a_leaf_activate_while_an_abandoned_drive_is_still_parked()
    {
        var (gate, baseline) = await WarmReplayGateAsync();
        var (grain, park, _) = await CreateLeafParkedInsideTheDriveAsync(gate);

        _ = await grain.DriveStarvedCheckpointAsync();

        try
        {
            Assert.That(park.Task.IsCompleted, Is.False,
                "the provider must still be hung, or this test measures recovery rather than release");

            // The user-visible property, not the proxy. A returned permit is
            // necessary but not sufficient: the barrier or another latch could
            // still hold activation, and what the operator actually lost during
            // issue #3065 was the ability to bring leaves online. Prove that.
            var freshWal = new GrowingWal();
            var (freshGrain, _, _, _) = CreateGrainWithMaterialiser(
                freshWal.Coordinator,
                treeId: UniqueStarvationDriveTree(),
                persistedCheckpoint: -1L);

            Assert.DoesNotThrowAsync(
                async () => await ActivateAsync(freshGrain),
                "a leaf must be able to activate while a previous drive is still parked in storage. "
                + "This is the property the operator lost, and a gate count returning to baseline is "
                + "only a proxy for it");

            Assert.That(gate.CurrentCount, Is.EqualTo(baseline),
                "the fresh activation must also have returned its own permit, leaving the gate exactly "
                + "as it found it");
        }
        finally
        {
            park.TrySetResult();
        }
    }

    [Test]
    public async Task DriveStarvedCheckpointAsync_clears_the_in_flight_latch_when_a_drive_is_abandoned()
    {
        var (gate, _) = await WarmReplayGateAsync();
        var (grain, park, _) = await CreateLeafParkedInsideTheDriveAsync(gate);

        var first = await grain.DriveStarvedCheckpointAsync();
        var second = await grain.DriveStarvedCheckpointAsync();

        try
        {
            Assert.Multiple(() =>
            {
                Assert.That(first, Is.EqualTo(LeafStarvationDriveOutcome.TimedOut),
                    "precondition: the first drive must actually have been abandoned");

                // The single easiest way to ship a fix that changes nothing.
                // Releasing the permit but leaving _starvationDriveInFlight set
                // converts a permit latch into a flag latch, and the symptom is
                // identical: every later drive bounces off AlreadyDriving and no
                // work is ever done. The frozen production container showed
                // exactly this - all three completed touches returned
                // AlreadyDriving, having bounced off a flag left set by a
                // predecessor that never reached its finally.
                Assert.That(second, Is.Not.EqualTo(LeafStarvationDriveOutcome.AlreadyDriving),
                    "the in-flight latch must be cleared on the abandonment path. Left set, it latches "
                    + "the leaf exactly as the leaked permit latched the gate, and every subsequent "
                    + "drive bounces off it forever");

                Assert.That(second, Is.EqualTo(LeafStarvationDriveOutcome.TimedOut),
                    "and the second drive must genuinely re-enter the work and be abandoned on its own "
                    + "budget, rather than returning some other verdict that merely happens not to be "
                    + "AlreadyDriving");
            });
        }
        finally
        {
            park.TrySetResult();
        }
    }

    [Test]
    public async Task DriveStarvedCheckpointAsync_counts_an_abandoned_drive_on_the_grain_side_arm()
    {
        var (gate, _) = await WarmReplayGateAsync();
        var (grain, park, _) = await CreateLeafParkedInsideTheDriveAsync(gate);

        var records = new ConcurrentBag<(long Value, KeyValuePair<string, object?>[] Tags)>();
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.WalReplayStarvationDriveAbandonments,
            l => l.SetMeasurementEventCallback<long>(
                (_, value, tags, _) => records.Add((value, tags.ToArray()))));

        var verdict = await grain.DriveStarvedCheckpointAsync();

        try
        {
            Assert.That(verdict, Is.EqualTo(LeafStarvationDriveOutcome.TimedOut),
                "precondition: the drive must actually have been abandoned");

            var abandonments = records.Where(r => r.Value > 0).ToArray();
            Assert.That(abandonments, Has.Length.EqualTo(1),
                "an abandoned drive must be counted exactly once on the grain-side arm. This counter is "
                + "the SOLE discriminator for this fault: the scheduler-side drove_timed_out arm is "
                + "recorded from the value this call returns, and the scheduler's touch abandons at the "
                + "Orleans response deadline far below any sane budget, so in the wedged case nobody is "
                + "still listening for it");

            Assert.That(
                abandonments.Single().Tags.Select(t => t.Key),
                Is.EquivalentTo(new[] { LatticeMetrics.TagTree, LatticeTenantLabel.TagTenant }),
                "tagged by tree plus the universal derived tenant dimension, which is exactly the tag set "
                + "the scheduler's priming site emits (it resolves the same LatticeTenantLabel.ForTree from "
                + "the same tree id). A tag set the priming site cannot reproduce would leave the primed "
                + "series and the emitted series unjoinable, which is the whole point of co-priming them - "
                + "the primed zero would sit beside the real count forever instead of being it");
        }
        finally
        {
            park.TrySetResult();
        }
    }

    [Test]
    public async Task DriveStarvedCheckpointAsync_that_completes_normally_releases_its_permit_exactly_once()
    {
        // The control. Without it, a "fix" that simply stopped taking a permit
        // at all would satisfy every assertion above, and a fix that released
        // twice would inflate the gate past its ceiling and quietly raise
        // replay concurrency beyond what the host was sized for.
        var (gate, baseline) = await WarmReplayGateAsync();

        var wal = new GrowingWal();
        var (grain, state, _, _) = CreateGrainWithMaterialiser(
            wal.Coordinator,
            treeId: UniqueStarvationDriveTree(),
            persistedCheckpoint: -1L,
            starvationDriveBudget: TestStarvationDriveBudget);

        await ActivateAsync(grain);
        wal.GrowTo(3);

        var verdict = await grain.DriveStarvedCheckpointAsync();

        Assert.Multiple(() =>
        {
            Assert.That(verdict, Is.Not.EqualTo(LeafStarvationDriveOutcome.TimedOut),
                "a drive against a responsive provider must not be abandoned. If this goes red the "
                + "budget is too tight and the fix has manufactured a worse failure than the one it "
                + "repaired");

            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(3L),
                "and it must still do the work: the bounded region must not have truncated the replay");

            Assert.That(gate.CurrentCount, Is.EqualTo(baseline),
                "exactly one release. Below baseline is the leak this issue is about; ABOVE baseline is "
                + "a double release, which raises the gate past the ceiling the host was sized for and "
                + "reintroduces the unbounded-replay pathology of issue #2862 from the other direction");
        });
    }
}
