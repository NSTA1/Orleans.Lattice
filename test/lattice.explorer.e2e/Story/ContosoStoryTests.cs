using Orleans.Lattice.Explorer.EndToEnd.Tests.Pending;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

/// <summary>The Contoso story (A1 to C4) on one estate, in story mode.</summary>
[TestFixture]
[Category("UI")]
[Category("AzureStorageEmulator")]
[Category("EndToEnd")]
[Category("Story")]
public sealed class ContosoStoryTests() : StoryRunFixture(StoryNames.Storylines.Contoso)
{
    /// <inheritdoc />
    /// <remarks>C1's stand-in contract: its east-ledger atomic batch reaches strict west as plain last-writer-wins rows re-shipped from east's re-seeded copy, which strict ingest does not judge (#5684).</remarks>
    protected override IReadOnlyDictionary<string, string> KnownDefectIssues { get; } =
        new Dictionary<string, string>(StringComparer.Ordinal) { ["C1"] = "#5684" };

    /// <inheritdoc />
    protected override Task RecordKnownDefectAsync(string chapter) =>
        new KnownDefect("#5684", "C1's east-ledger atomic batch reaches strict west as plain last-writer-wins rows re-shipped from east's re-seeded copy, which strict ingest does not judge (#5684)").ReproducesAsync(() => Task.CompletedTask);

    /// <summary>Runs the story through <paramref name="chapter"/>.</summary>
    /// <param name="chapter">The chapter id.</param>
    [TestCaseSource(nameof(Chapters))]
    public Task Chapter(string chapter) => RunChapterAsync(chapter);

    private static IEnumerable<TestCaseData> Chapters() => CasesFor(StoryNames.Storylines.Contoso);
}
