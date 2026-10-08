using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Opt-in Coyote tests for the end-to-end WAL durability lifecycle model
/// (<see cref="WalDurabilityLifecycleModel"/>), the implementation-level
/// companion of <c>spec/wal/WalDurability.tla</c>. Tagged
/// <c>[Category("Coyote")]</c> so the dev loop and the deterministic CI tier skip
/// them; the <c>coyote</c> tier runs them.
/// <para>
/// Each guard test removes exactly one fix and requires Coyote to find a
/// violation REPORTED BY THE ASSERTION THAT FIX PROTECTS, identified by its tag
/// in the bug report. Accepting any violation would let a guard pass because
/// some other assertion fired, which proves nothing about the fix it names.
/// </para>
/// <para>
/// Every guard explores under <see cref="CoyoteModelHarness.GuardSeed"/>, so it
/// explores the same runs, and reports the same violation, on every execution.
/// Unseeded, a removed fix that breaks two properties was reported by whichever
/// assertion its random sample reached first: removing
/// <see cref="WalDurabilityLifecycleGuard.ReplayStopsAtPersistedCheckpoint"/>
/// was reported by <c>[ReclamationEventuallyAdvances]</c> instead of
/// <c>[EveryAckedWriteMaterialised]</c> in 7 of 55 runs, a required test that
/// flaked (issue #4727). The fixed-design tests stay unseeded, so each run
/// samples fresh paths.
/// </para>
/// </summary>
[TestFixture]
[Category("Coyote")]
public sealed class WalDurabilityLifecycleCoyoteTests
{
    /// <summary>
    /// The fixed design: under every explored interleaving of appends,
    /// out-of-order flushes, reads, persists and captures that can fail, pin
    /// publication, trims and leaf stops at any step, no acknowledged write is
    /// lost or skipped, no leaf falls off the log, and the lifecycle converges
    /// once faults are spent.
    /// </summary>
    [Test]
    public void The_lifecycle_loses_no_acknowledged_write_under_crash_anywhere_recovery()
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new WalDurabilityLifecycleModel(WalDurabilityLifecycleGuard.None));
    }

    /// <summary>
    /// The fixed design with one leaf that owns nothing, so the core's
    /// never-written release (issues #3453, #4456 and #4523) is exercised: that
    /// leaf scans, persists, captures and publishes over entries it never
    /// applies, stops and reactivates, and is never latched stale.
    /// </summary>
    [Test]
    public void A_never_written_leaf_is_never_latched_stale_under_crash_anywhere_recovery()
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new WalDurabilityLifecycleModel(WalDurabilityLifecycleGuard.None, faultBudget: 1, neverWrittenLeaf: true));
    }

    /// <summary>
    /// The guard for the never-written arm, at its root cause: with the release
    /// published at the persisted checkpoint whatever coverage the leaf holds
    /// (before issue #4456; with no coverage, before issue #4523), the leaf's pin
    /// rises above its snapshot at the publication itself, and
    /// <c>[ReleaseBackedBySnapshot]</c> must be the assertion that reports it. It
    /// needs no fault and is reached in the default ownership, where leaf 1 is
    /// never-written until it applies its first entry.
    /// </summary>
    [Test]
    public void Removing_the_never_written_release_bound_is_caught_by_the_release_backing_assertion()
    {
        var result = CoyoteModelHarness.Explore(new WalDurabilityLifecycleModel(
            WalDurabilityLifecycleGuard.NeverWrittenReleaseIgnoresCoverage, faultBudget: 0),
            seed: CoyoteModelHarness.GuardSeed);

        Assert.That(
            result.BugsFound,
            Is.GreaterThan(0),
            $"removing the never-written release's coverage bound must produce a violation in {result.Iterations} explored runs.");
        Assert.That(
            string.Join("\n", result.BugReports),
            Does.Contain("[ReleaseBackedBySnapshot]"),
            "the unbounded never-written release was caught, but not by [ReleaseBackedBySnapshot].");
    }

    /// <summary>
    /// The guard for the never-written arm, at its outcome: with the root-cause
    /// assertion off, the unbounded release lets the GC trim past the snapshot of
    /// the leaf that owns nothing, and its restart must be reported by
    /// <c>[RecoveryNeverFallsOffLog]</c>. This is the TLA+ module's trace (the
    /// leaf reads, captures below its read position, persists, publishes and
    /// stops) on production's cores.
    /// </summary>
    /// <remarks>
    /// The latch needs the leaf's snapshot coverage to fall at least two offsets
    /// below the trimmed tail, with no recapture or stop in between
    /// (<see cref="Orleans.Lattice.BPlusTree.WalFallOffCore.IsPrefixLost(long, long)"/>; a checkpoint of 0 counts since
    /// issue #4433). The measured per-run detection rate is p ~ 8.5e-3 (40 Coyote
    /// explorations, 4723 paths, every one reported by [RecoveryNeverFallsOffLog];
    /// it was ~ 2.0e-3 while the core exempted a checkpoint of 0), so the default
    /// 1000 runs would still miss it ~ 2e-4 of the time; 10000 runs miss it with
    /// probability ~ e^-85. Exploration stops at the first violation, so the
    /// expected cost is ~ 1/p ~ 120 runs.
    /// </remarks>
    [Test]
    public void Removing_the_never_written_release_bound_is_caught_by_the_fall_off_assertion()
    {
        var result = CoyoteModelHarness.Explore(
            new WalDurabilityLifecycleModel(
                WalDurabilityLifecycleGuard.NeverWrittenReleaseIgnoresCoverage,
                faultBudget: 1,
                neverWrittenLeaf: true,
                checkReleaseBacking: false),
            iterations: 10000,
            seed: CoyoteModelHarness.GuardSeed);

        Assert.That(
            result.BugsFound,
            Is.GreaterThan(0),
            $"removing the never-written release's coverage bound must produce a violation in {result.Iterations} explored runs.");
        Assert.That(
            string.Join("\n", result.BugReports),
            Does.Contain("[RecoveryNeverFallsOffLog]"),
            "the unbounded never-written release was caught, but not by [RecoveryNeverFallsOffLog].");
    }
    /// <summary>
    /// The guards: each removed fix is caught by its own assertion.
    /// </summary>
    /// <remarks>
    /// The fault budget is per case, and only as large as the violation needs:
    /// a failed persist and a cold start each need one, the others need none, and every
    /// unneeded fault action widens the choice at each step and dilutes the
    /// search for the state the guard is about.
    /// <para>
    /// The iteration budget is per case too, sized from the measured per-run
    /// detection rate p (explored paths to first violation, 40 explorations
    /// each, on the fixed core): PinFromPendingCheckpoint p ~ 0.34,
    /// NoRollbackOnFailedPersist p ~ 0.018, ReaderIgnoresWatermark p ~ 0.75,
    /// TrimFloorFromHighestPin p ~ 0.0026. The last needs both leaves to
    /// publish divergent offset pins and a trim inside the random phase, and
    /// since #4535 a never-written leaf may only release behind a kept
    /// capture, which roughly halved its reach (p ~ 0.0046 before). At the
    /// default 1000 runs it is missed with probability (1 - p)^1000 ~ 7%, which
    /// is the CI flake this budget removes: at 10000 runs the miss probability
    /// is ~ e^-26, and ~ 2e-8 even at the lower 95% bound of p. Exploration
    /// stops at the first violation, so the expected cost is ~ 1/p ~ 400 runs.
    /// </para>
    /// <para>
    /// Those rates size the budgets, but the seed fixes the outcome: under
    /// <see cref="CoyoteModelHarness.GuardSeed"/> each case finds its violation,
    /// reported by its own tag, identically on every run. Removing
    /// <see cref="WalDurabilityLifecycleGuard.ReplayStopsAtPersistedCheckpoint"/>
    /// also stalls reclamation, so unseeded it was reported by
    /// <c>[ReclamationEventuallyAdvances]</c> in 7 of 55 runs, and under 2 of
    /// the first 20 seeds (7 and 9); the seed is what makes the case specific on
    /// every run (issue #4727).
    /// </para>
    /// </remarks>
    [TestCase(WalDurabilityLifecycleGuard.PinFromPendingCheckpoint, 0, CoyoteModelHarness.DefaultIterations, "[PublishedPinWithinPersistedBelief]")]
    [TestCase(WalDurabilityLifecycleGuard.NoRollbackOnFailedPersist, 1, CoyoteModelHarness.DefaultIterations, "[PersistedBeliefHonest]")]
    [TestCase(WalDurabilityLifecycleGuard.ReaderIgnoresWatermark, 0, CoyoteModelHarness.DefaultIterations, "[ShippingNeverSkips]")]
    [TestCase(WalDurabilityLifecycleGuard.TrimFloorFromHighestPin, 0, 10000, "[TrimCoveredBySnapshot]")]
    [TestCase(WalDurabilityLifecycleGuard.AckBeforeFlush, 0, CoyoteModelHarness.DefaultIterations, "[AckedWriteDurable]")]
    [TestCase(WalDurabilityLifecycleGuard.ColdStartResumesFromCheckpoint, 1, 10000, "[ReadPositionHonest]")]
    [TestCase(WalDurabilityLifecycleGuard.ReplayStopsAtPersistedCheckpoint, 0, CoyoteModelHarness.DefaultIterations, "[EveryAckedWriteMaterialised]")]
    public void Removing_one_fix_is_caught_by_the_assertion_it_protects(
        WalDurabilityLifecycleGuard guard, int faultBudget, int iterations, string tag)
    {
        var result = CoyoteModelHarness.Explore(new WalDurabilityLifecycleModel(guard, faultBudget), iterations, seed: CoyoteModelHarness.GuardSeed);

        Assert.That(
            result.BugsFound,
            Is.GreaterThan(0),
            $"removing {guard} must produce a violation in {result.Iterations} explored runs; none was found, "
            + "so the model no longer exercises the state that fix protects.");
        Assert.That(
            string.Join("\n", result.BugReports),
            Does.Contain(tag),
            $"removing {guard} was caught, but not by {tag}: the guard is not specific to the fix it names.");
    }

    /// <summary>
    /// The guard for issue #2270 at the liveness assertion: with the read position
    /// advancing only over a leaf's own entries, the leaf that owns nothing never
    /// advances, keeps its seeded block pin and holds the GC, so the stream is
    /// never reclaimed. Every write is still materialised by its owner, so
    /// <c>[ReclamationEventuallyAdvances]</c> is what must report it.
    /// </summary>
    [Test]
    public void Removing_the_read_position_over_foreign_entries_is_caught_by_the_reclamation_assertion()
    {
        var result = CoyoteModelHarness.Explore(
            new WalDurabilityLifecycleModel(
                WalDurabilityLifecycleGuard.ReadPositionTracksOwnEntries, faultBudget: 0, neverWrittenLeaf: true),
            seed: CoyoteModelHarness.GuardSeed);

        Assert.That(
            result.BugsFound,
            Is.GreaterThan(0),
            $"a read position that tracks only the leaf's own entries must produce a violation in {result.Iterations} explored runs.");
        Assert.That(
            string.Join("\n", result.BugReports),
            Does.Contain("[ReclamationEventuallyAdvances]"),
            "a read position that tracks only the leaf's own entries was caught, but not by [ReclamationEventuallyAdvances].");
    }
}