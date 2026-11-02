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
    /// <remarks>C1's stand-in contract: after a coordinated restore of factory-orders the regions re-seed each other repeatedly, and its east-ledger batch never reaches west's dead letters (#5534).</remarks>
    protected override IReadOnlyDictionary<string, string> KnownDefectIssues { get; } =
        new Dictionary<string, string>(StringComparer.Ordinal) { ["C1"] = "#5534" };

    /// <inheritdoc />
    protected override Task RecordKnownDefectAsync(string chapter) =>
        new KnownDefect("#5534", "after a coordinated restore of factory-orders the regions re-seed each other repeatedly, and C1's east-ledger batch never reaches west's dead letters (#5534)").ReproducesAsync(() => Task.CompletedTask);

    /// <summary>Runs the story through <paramref name="chapter"/>.</summary>
    /// <param name="chapter">The chapter id.</param>
    [TestCaseSource(nameof(Chapters))]
    public Task Chapter(string chapter) => RunChapterAsync(chapter);

    private static IEnumerable<TestCaseData> Chapters() => CasesFor(StoryNames.Storylines.Contoso);
}
