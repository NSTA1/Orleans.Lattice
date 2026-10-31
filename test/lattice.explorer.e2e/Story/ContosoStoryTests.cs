namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

/// <summary>The Contoso story (A1 to C4) on one estate, in story mode.</summary>
[TestFixture]
[Category("UI")]
[Category("AzureStorageEmulator")]
[Category("EndToEnd")]
[Category("Story")]
public sealed class ContosoStoryTests() : StoryRunFixture(StoryNames.Storylines.Contoso)
{
    /// <summary>Runs the story through <paramref name="chapter"/>.</summary>
    /// <param name="chapter">The chapter id.</param>
    [TestCaseSource(nameof(Chapters))]
    public Task Chapter(string chapter) => RunChapterAsync(chapter);

    private static IEnumerable<TestCaseData> Chapters() => CasesFor(StoryNames.Storylines.Contoso);
}
