using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Tests that <see cref="RepoContextAnnOpenSliceDeadline"/> holds an open-slice
/// budget longer than a timer can wait to the longest wait a timer accepts,
/// instead of faulting the open.
/// <para>
/// The deadline arms its period with <see cref="TimeProvider.CreateTimer"/>, and
/// the system timer refuses a due time or period above <c>0xFFFFFFFE</c>
/// milliseconds (about 49.7 days) with <see cref="ArgumentOutOfRangeException"/>.
/// <see cref="RepoContextAnnOptions.OpenSliceBudget"/> is read from
/// <c>LATTICE_REPOCONTEXT_ANN_OPEN_SLICE_BUDGET_SECONDS</c>, which accepts any
/// duration <see cref="TimeSpan"/> can hold, so a budget meant as "effectively
/// unbounded" used to throw on every open attempt and the approximate index never
/// opened.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextAnnOpenSliceDeadlineTests
{
    private static readonly TimeSpan TimerCeiling = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    private static IEnumerable<TimeSpan> BudgetsAboveTheTimerCeiling()
    {
        yield return TimerCeiling + TimeSpan.FromMilliseconds(1);
        yield return TimeSpan.FromDays(60);
        yield return TimeSpan.MaxValue;
    }

    [TestCaseSource(nameof(BudgetsAboveTheTimerCeiling))]
    public void A_budget_above_the_timer_ceiling_arms_without_faulting(TimeSpan budget)
    {
        Assert.That(
            () => TimeProvider.System.CreateTimer(static _ => { }, null, budget, budget).Dispose(),
            Throws.InstanceOf<ArgumentOutOfRangeException>(),
            "anti-vacuity: the budget is one the system timer itself refuses");

        RepoContextAnnOpenSliceDeadline? deadline = null;
        Assert.That(
            () => deadline = new RepoContextAnnOpenSliceDeadline(budget, 6, static () => 0, TimeProvider.System),
            Throws.Nothing);

        using (deadline)
        {
            Assert.That(deadline!.IsCancellationRequested, Is.False);
        }
    }

    [Test]
    public void A_clamped_budget_still_fires_at_the_timer_ceiling()
    {
        var clock = new ManualTimeProvider();
        using var deadline = new RepoContextAnnOpenSliceDeadline(
            TimeSpan.FromDays(60), maxExtensions: 0, static () => 0, clock);

        clock.Advance(TimerCeiling - TimeSpan.FromMilliseconds(1));
        Assert.That(deadline.IsCancellationRequested, Is.False, "the clamped period has not elapsed yet");

        clock.Advance(TimeSpan.FromMilliseconds(1));
        Assert.That(
            deadline.IsCancellationRequested,
            Is.True,
            "the budget is clamped to the longest period a timer can hold, so the open is still bounded");
    }

    [Test]
    public void A_budget_within_the_timer_ceiling_is_armed_unchanged()
    {
        var clock = new ManualTimeProvider();
        var budget = TimeSpan.FromSeconds(5);
        using var deadline = new RepoContextAnnOpenSliceDeadline(budget, maxExtensions: 0, static () => 0, clock);

        clock.Advance(budget - TimeSpan.FromMilliseconds(1));
        Assert.That(deadline.IsCancellationRequested, Is.False);

        clock.Advance(TimeSpan.FromMilliseconds(1));
        Assert.That(deadline.IsCancellationRequested, Is.True);
    }
}
