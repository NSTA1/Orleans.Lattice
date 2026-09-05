namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Bootstrap;

/// <summary>
/// Tests for <see cref="RepoWalkPruning.ShouldForceFullSweep"/>: the pure decision the
/// background reconcile uses to choose between a full sweep (stat every file) and a
/// pruned walk (carry unchanged directories forward). This is the regression guard for
/// #2042, where the shipped <see cref="RepoContextIndexingOptions.FullWalkInterval"/>
/// default was shorter than the maximum reconcile spacing, so every reconcile was forced
/// to a full sweep and pruning never engaged.
/// </summary>
[TestFixture]
public sealed class RepoWalkPruningDecisionTests
{
    private const long LastFullSweepTicks = 1_000_000L;

    [Test]
    public void A_run_that_may_not_prune_always_forces_a_full_sweep()
    {
        // An explicit onboarding or re-bootstrap leaves AllowPrune false and must be exact.
        var force = RepoWalkPruning.ShouldForceFullSweep(
            allowPrune: false,
            hasPriorSnapshot: true,
            nowTicks: LastFullSweepTicks + 1,
            lastFullSweepTicks: LastFullSweepTicks,
            fullWalkInterval: TimeSpan.FromMinutes(60));

        Assert.That(force, Is.True);
    }

    [Test]
    public void A_cold_run_with_no_prior_snapshot_forces_a_full_sweep()
    {
        // The first walk after a process restart has no baseline to prune against.
        var force = RepoWalkPruning.ShouldForceFullSweep(
            allowPrune: true,
            hasPriorSnapshot: false,
            nowTicks: LastFullSweepTicks + 1,
            lastFullSweepTicks: LastFullSweepTicks,
            fullWalkInterval: TimeSpan.FromMinutes(60));

        Assert.That(force, Is.True);
    }

    [Test]
    public void A_run_at_or_past_the_full_walk_interval_forces_a_full_sweep()
    {
        var interval = TimeSpan.FromMinutes(60);

        Assert.Multiple(() =>
        {
            Assert.That(
                RepoWalkPruning.ShouldForceFullSweep(
                    allowPrune: true,
                    hasPriorSnapshot: true,
                    nowTicks: LastFullSweepTicks + interval.Ticks,
                    lastFullSweepTicks: LastFullSweepTicks,
                    fullWalkInterval: interval),
                Is.True,
                "Exactly at the interval a full sweep is due.");
            Assert.That(
                RepoWalkPruning.ShouldForceFullSweep(
                    allowPrune: true,
                    hasPriorSnapshot: true,
                    nowTicks: LastFullSweepTicks + interval.Ticks + 1,
                    lastFullSweepTicks: LastFullSweepTicks,
                    fullWalkInterval: interval),
                Is.True,
                "Past the interval a full sweep is due.");
        });
    }

    [Test]
    public void A_second_reconcile_inside_the_full_walk_window_prunes()
    {
        var interval = TimeSpan.FromMinutes(60);

        var force = RepoWalkPruning.ShouldForceFullSweep(
            allowPrune: true,
            hasPriorSnapshot: true,
            nowTicks: LastFullSweepTicks + TimeSpan.FromMinutes(20).Ticks,
            lastFullSweepTicks: LastFullSweepTicks,
            fullWalkInterval: interval);

        Assert.That(force, Is.False, "A reconcile well inside the full-walk window must prune, not force a full sweep.");
    }

    [Test]
    public void At_the_shipped_defaults_a_reconcile_at_maximum_spacing_prunes()
    {
        // The behavioural proof for #2042: even at the widest possible gap between two
        // reconciles (ReconcileInterval + full jitter), a second reconcile after the last
        // full sweep must still be able to prune. Before the fix, FullWalkInterval was
        // 5 minutes against a 20-minute maximum spacing, so this always forced a full
        // sweep and pruning was dead.
        var options = new RepoContextIndexingOptions();
        var nowTicks = LastFullSweepTicks + options.MaxReconcileSpacing.Ticks;

        var force = RepoWalkPruning.ShouldForceFullSweep(
            allowPrune: true,
            hasPriorSnapshot: true,
            nowTicks: nowTicks,
            lastFullSweepTicks: LastFullSweepTicks,
            fullWalkInterval: options.FullWalkInterval);

        Assert.That(force, Is.False, "At the shipped defaults a reconcile at the maximum spacing after the last full sweep must prune.");
    }
}
