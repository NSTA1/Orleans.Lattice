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
    /// </remarks>
    [TestCase(WalDurabilityLifecycleGuard.PinFromPendingCheckpoint, 0, "[PublishedPinWithinPersistedBelief]")]
    [TestCase(WalDurabilityLifecycleGuard.NoRollbackOnFailedPersist, 1, "[PersistedBeliefHonest]")]
    [TestCase(WalDurabilityLifecycleGuard.ReaderIgnoresWatermark, 0, "[ShippingNeverSkips]")]
    [TestCase(WalDurabilityLifecycleGuard.TrimFloorFromHighestPin, 0, "[TrimCoveredBySnapshot]")]
    public void Removing_one_fix_is_caught_by_the_assertion_it_protects(
        WalDurabilityLifecycleGuard guard, int faultBudget, string tag)
    {
        var result = CoyoteModelHarness.Explore(new WalDurabilityLifecycleModel(guard, faultBudget));

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
