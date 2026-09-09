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

    [Test]
    public void SaturatingAddTicks_clamps_a_positive_overflow_to_long_MaxValue()
    {
        Assert.That(
            RepoContextSelfIndexGrain.SaturatingAddTicks(long.MaxValue - 10, 1_000L),
            Is.EqualTo(long.MaxValue));
    }

    [Test]
    public void SaturatingAddTicks_is_exact_when_the_sum_does_not_overflow()
    {
        Assert.That(RepoContextSelfIndexGrain.SaturatingAddTicks(1_000L, 2_000L), Is.EqualTo(3_000L));
    }
}
