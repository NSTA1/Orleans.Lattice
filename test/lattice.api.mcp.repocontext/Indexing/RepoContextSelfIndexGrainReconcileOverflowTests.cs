namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Indexing;

/// <summary>
/// Regression tests for the reconcile-scheduling overflow on
/// <see cref="RepoContextSelfIndexGrain"/>. <c>ReconcileInterval</c> is settable
/// per second through <c>LATTICE_RECONCILE_INTERVAL_SECONDS</c> with no upper
/// bound, and the scheduler once added it to the current tick unguarded, so an
/// extreme interval overflowed <c>NextReconcileAfterTicks</c> to a negative, past
/// instant - and the tick gate (<c>nowTicks &gt;= NextReconcileAfterTicks</c>)
/// then re-drove a reconcile on every tick, the busiest cadence in place of the
/// rarest one the operator configured. The sum now saturates (siblings of issues
/// 2221 and 2342).
/// </summary>
[TestFixture]
public sealed class RepoContextSelfIndexGrainReconcileOverflowTests
{
    [Test]
    public async Task EnsureRunningAsync_saturates_the_reconcile_deadline_for_an_extreme_interval()
    {
        var harness = new SelfIndexGrainHarness(options: new RepoContextIndexingOptions
        {
            Role = RepoContextIndexingRole.Hub,
            ReconcileInterval = TimeSpan.MaxValue,
            ReconcileIntervalJitter = TimeSpan.Zero,
        });
        var grain = harness.CreateGrain();

        await grain.EnsureRunningAsync(SelfIndexGrainHarness.Request());

        Assert.That(
            harness.State.State.NextReconcileAfterTicks,
            Is.EqualTo(long.MaxValue),
            "an extreme reconcile interval must saturate the schedule, not overflow it to a " +
            "past instant that re-drives a reconcile on every tick");
    }
}
