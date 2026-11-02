namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Pending;

/// <summary>
/// A step wrapped because the UI lacks a fundamental concept an epic gap sub-issue is building:
/// the wrapped body asserts the affordance is <em>still absent</em> (decision D8 of epic #5401).
/// Once the gap feature lands the body's assertion fails, and the wrapper fails loudly: remove
/// it and drive the step for real.
/// </summary>
/// <example>
/// <code>
/// await new KnownGap("#5368", "No New tree... on the trees list").StillAbsentAsync(async () =>
///     await Expect(page.Locator("[data-lt-command='cluster.create-tree']")).ToHaveCountAsync(0));
/// </code>
/// </example>
public sealed class KnownGap
{
    /// <summary>Creates the wrapper.</summary>
    /// <param name="issue">The gap issue, as <c>#N</c>.</param>
    /// <param name="summary">What the UI lacks.</param>
    public KnownGap(string issue, string summary)
    {
        Issue = PendingIssue.Validate(issue);
        ArgumentException.ThrowIfNullOrWhiteSpace(summary);
        Summary = summary;
    }

    /// <summary>The gap issue.</summary>
    public string Issue { get; }

    /// <summary>What the UI lacks.</summary>
    public string Summary { get; }

    /// <summary>Runs <paramref name="absence"/>, which asserts the affordance is absent.</summary>
    /// <param name="absence">Asserts the affordance is still absent.</param>
    /// <exception cref="PendingWrapperResolvedException">The affordance now exists.</exception>
    public async Task StillAbsentAsync(Func<Task> absence)
    {
        ArgumentNullException.ThrowIfNull(absence);
        try
        {
            await absence();
        }
        catch (Exception ex) when (ex is not PendingWrapperResolvedException and not OperationCanceledException)
        {
            throw new PendingWrapperResolvedException(
                $"KnownGap({Issue}) is no longer a gap: {Summary}. The wrapped step asserts the affordance is absent, and that assertion failed, so {Issue} looks landed. Remove the wrapper and drive the step for real.",
                ex);
        }

        PendingWrappers.Record(new PendingWrapper(nameof(KnownGap), Issue, Summary, null, 0));
    }
}
