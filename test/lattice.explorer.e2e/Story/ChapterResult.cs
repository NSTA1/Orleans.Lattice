namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

/// <summary>How one chapter of a story run ended.</summary>
/// <param name="Id">The chapter id.</param>
/// <param name="Status">How it ended.</param>
/// <param name="BlockedBy">The consumed chapters that failed or were blocked, by name; empty unless <see cref="ChapterStatus.Blocked"/>.</param>
/// <param name="Failure">What failed, when <see cref="ChapterStatus.Failed"/>.</param>
/// <param name="Elapsed">How long it ran.</param>
public sealed record ChapterResult(string Id, ChapterStatus Status, IReadOnlyList<string> BlockedBy, Exception? Failure, TimeSpan Elapsed)
{
    /// <summary>Whether the chapters that consume this one may run: it passed, or was stood in.</summary>
    public bool Holds => Status is ChapterStatus.Passed or ChapterStatus.StoodIn;

    /// <summary>A one-line account of the outcome, for the report.</summary>
    public string Detail => Status switch
    {
        ChapterStatus.Blocked => "blocked by " + string.Join(", ", BlockedBy),
        ChapterStatus.Failed => FirstLine(Failure?.Message),
        ChapterStatus.StoodIn => "not implemented yet: stood in by its F6 contract",
        _ => string.Empty,
    };

    private static string FirstLine(string? text) =>
        string.IsNullOrEmpty(text) ? "failed" : text.Split('\n')[0].Trim();
}
