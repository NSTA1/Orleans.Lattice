namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Pending;

/// <summary>
/// A step wrapped because an epic sub-issue tracks its defect: the wrapped body asserts the
/// <em>defective</em> behaviour, so the wrapper passes only while the defect still reproduces
/// (decision D7 of epic #5401). Once the defect is fixed the body's assertion fails, and the
/// wrapper fails loudly: remove it and drive the step for real.
/// </summary>
/// <example>
/// <code>
/// await new KnownDefect("#5412", "Saving retention drops the window").ReproducesAsync(async () =>
///     await Expect(page.GetByText("Retention: none")).ToBeVisibleAsync());
/// </code>
/// </example>
public sealed class KnownDefect
{
    /// <summary>Creates the wrapper.</summary>
    /// <param name="issue">The tracking issue, as <c>#N</c>.</param>
    /// <param name="summary">What the defect is.</param>
    public KnownDefect(string issue, string summary)
    {
        Issue = PendingIssue.Validate(issue);
        ArgumentException.ThrowIfNullOrWhiteSpace(summary);
        Summary = summary;
    }

    /// <summary>The tracking issue.</summary>
    public string Issue { get; }

    /// <summary>What the defect is.</summary>
    public string Summary { get; }

    /// <summary>Runs <paramref name="defectiveBehaviour"/>, which asserts the defect.</summary>
    /// <param name="defectiveBehaviour">Drives the step and asserts its defective outcome.</param>
    /// <exception cref="PendingWrapperResolvedException">The defect no longer reproduces.</exception>
    public async Task ReproducesAsync(Func<Task> defectiveBehaviour)
    {
        ArgumentNullException.ThrowIfNull(defectiveBehaviour);
        try
        {
            await defectiveBehaviour();
        }
        catch (Exception ex) when (ex is not PendingWrapperResolvedException and not OperationCanceledException)
        {
            throw new PendingWrapperResolvedException(
                $"KnownDefect({Issue}) no longer reproduces: {Summary}. The wrapped step asserts the defective behaviour, and that assertion failed, so {Issue} looks fixed. Remove the wrapper and drive the step for real.",
                ex);
        }

        PendingWrappers.Record(new PendingWrapper(nameof(KnownDefect), Issue, Summary, null, 0));
    }
}
