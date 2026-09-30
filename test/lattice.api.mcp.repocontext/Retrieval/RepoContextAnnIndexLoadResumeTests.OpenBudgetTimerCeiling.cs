namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// An open-slice budget longer than a timer can wait must still open the index.
/// <para>
/// <see cref="RepoContextAnnOptions.OpenSliceBudget"/> is read from an environment
/// variable that accepts any duration <see cref="TimeSpan"/> can hold, and the open
/// hands it to a timer-backed deadline. Above <c>0xFFFFFFFE</c> milliseconds the
/// system timer throws <see cref="ArgumentOutOfRangeException"/> before the key walk
/// starts, so a budget meant as "effectively unbounded" left the approximate index
/// unable to open at all.
/// </para>
/// </summary>
public sealed partial class RepoContextAnnIndexLoadResumeTests
{
    private static IEnumerable<TimeSpan> OpenBudgetsAboveTheTimerCeiling()
    {
        yield return TimeSpan.FromMilliseconds(uint.MaxValue - 1) + TimeSpan.FromMilliseconds(1);
        yield return TimeSpan.FromDays(60);
        yield return TimeSpan.MaxValue;
    }

    [TestCaseSource(nameof(OpenBudgetsAboveTheTimerCeiling))]
    public async Task An_open_budget_above_the_timer_ceiling_still_opens_the_index(TimeSpan budget)
    {
        var store = await SeededStoreAsync();
        using var reporter = new RepoContextAnnIndexLoadReporter();
        var prefix = RepoContextAnnIndexKeys.IndexPrefix(RepoId, Space);

        using var handle = NewHandle(
            SeededSource(), store, prefix, reporter, BudgetedOptions(TimeProvider.System, budget));

        Assert.That(async () => await handle.AdvanceAsync(Ct), Throws.Nothing);

        var snapshot = reporter.Snapshot();
        Assert.Multiple(() =>
        {
            Assert.That(handle.IsServing, Is.True,
                "an unobstructed open completes long before a budget of weeks, so the handle must serve");
            Assert.That(snapshot.Faulted, Is.Zero,
                "a budget the timer cannot hold must be clamped, not surfaced as a failed open");
        });
    }
}
