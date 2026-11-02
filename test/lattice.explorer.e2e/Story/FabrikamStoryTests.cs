namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

/// <summary>The Fabrikam story (T1 to T6) on one tenanted estate, in story mode.</summary>
[TestFixture]
[Category("UI")]
[Category("AzureStorageEmulator")]
[Category("EndToEnd")]
[Category("Story")]
public sealed class FabrikamStoryTests() : StoryRunFixture(StoryNames.Storylines.Fabrikam)
{
    /// <summary>Runs the story through <paramref name="chapter"/>.</summary>
    /// <param name="chapter">The chapter id.</param>
    [TestCaseSource(nameof(Chapters))]
    public Task Chapter(string chapter) => RunChapterAsync(chapter);

    private static IEnumerable<TestCaseData> Chapters() => CasesFor(StoryNames.Storylines.Fabrikam);
}
