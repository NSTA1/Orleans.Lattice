using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Backup.Tests.Coyote;

/// <summary>
/// Coyote tests for the backup retention fence (<see cref="BackupRetentionPlanner"/>,
/// issue #5357): the fixed-design arm, an anti-vacuity witness that some
/// schedule makes the increment fall back, and one guard per side of the fence
/// that must find a catalog row which is not restorable once that side is
/// removed. Opt-in: <c>[Category("Coyote")]</c>.
/// </summary>
[TestFixture]
[Category("Coyote")]
public sealed class BackupRetentionCoyoteTests
{
    /// <summary>
    /// Iteration budget of the violation-seeking cases.
    /// </summary>
    /// <remarks>
    /// Sized from the measured per-run detection rate p: over 400 single-run
    /// explorations each, the witness found its schedule 3 times, the re-check
    /// guard 3 times and the scan guard 2 times, so p ~ 0.005-0.0075. Each needs
    /// the pass, the newer full capture and the increment to line up in one of a
    /// few orders out of many, and a pass that starts early finds nothing to do.
    /// At the default 1000 runs a miss would have probability up to ~ e^-5, a CI
    /// flake; at 10000 it is ~ e^-50, and ~ 3e-7 even at the lower 95% bound of
    /// p (~ 0.0015). Exploration stops at the first violation, so the expected
    /// cost is ~ 1/p ~ 200 runs.
    /// </remarks>
    private const int ViolationSeekingIterations = 10_000;

    /// <summary>
    /// Under the production planner every catalog row stays restorable on every
    /// explored schedule of a pass racing an increment and a newer full capture.
    /// </summary>
    [Test]
    public void Production_retention_never_leaves_a_listed_backup_unrestorable() =>
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new BackupRetentionModel(BackupRetentionRulesRemoved.None));

    /// <summary>
    /// Anti-vacuity witness: some explored schedule has the pass take the
    /// increment's base first, so the increment falls back rather than publish.
    /// </summary>
    [Test]
    public void Exploration_reaches_an_increment_falling_back_because_a_pass_had_its_base() =>
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(
            new BackupRetentionModel(BackupRetentionRulesRemoved.None, witnessFallback: true),
            ViolationSeekingIterations);

    /// <summary>
    /// Guard: without the increment's base re-check, a pass that scanned before
    /// the increment wrote its manifest deletes the base it then publishes over.
    /// </summary>
    [Test]
    public void Without_the_base_recheck_an_increment_is_published_over_a_pruned_base() =>
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(
            new BackupRetentionModel(BackupRetentionRulesRemoved.BaseRecheck),
            ViolationSeekingIterations);

    /// <summary>
    /// Guard: without the dependent scan, a pass deletes the base of an increment
    /// that passed its re-check before the pass removed the base's row.
    /// </summary>
    [Test]
    public void Without_the_dependent_scan_a_pass_deletes_a_base_an_increment_published_over() =>
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(
            new BackupRetentionModel(BackupRetentionRulesRemoved.DependentScan),
            ViolationSeekingIterations);
}