namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

/// <summary>
/// What a chapter does: its UI steps, run against a <see cref="ChapterContext"/> whose estate
/// already holds everything the chapter consumes. A journey implements it by deriving from
/// <see cref="JourneyChapter"/>.
/// </summary>
public interface IStoryChapter
{
    /// <summary>Runs the chapter's steps.</summary>
    /// <param name="context">The chapter's estate, ledger and run.</param>
    Task RunAsync(ChapterContext context);
}
