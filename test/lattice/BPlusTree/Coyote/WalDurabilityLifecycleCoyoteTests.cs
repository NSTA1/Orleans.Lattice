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
    /// The guards: each removed fix is caught by its own assertion.
    /// </summary>
    /// <remarks>
    /// The fault budget is per case, and only as large as the violation needs:
    /// a failed persist needs one, the other three need none, and every
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
    /// </remarks>
    [TestCase(WalDurabilityLifecycleGuard.PinFromPendingCheckpoint, 0, CoyoteModelHarness.DefaultIterations, "[PublishedPinWithinPersistedBelief]")]
    [TestCase(WalDurabilityLifecycleGuard.NoRollbackOnFailedPersist, 1, CoyoteModelHarness.DefaultIterations, "[PersistedBeliefHonest]")]
    [TestCase(WalDurabilityLifecycleGuard.ReaderIgnoresWatermark, 0, CoyoteModelHarness.DefaultIterations, "[ShippingNeverSkips]")]
    [TestCase(WalDurabilityLifecycleGuard.TrimFloorFromHighestPin, 0, 10000, "[TrimCoveredBySnapshot]")]
    public void Removing_one_fix_is_caught_by_the_assertion_it_protects(
        WalDurabilityLifecycleGuard guard, int faultBudget, int iterations, string tag)
    {
        var result = CoyoteModelHarness.Explore(new WalDurabilityLifecycleModel(guard, faultBudget), iterations);

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
}
