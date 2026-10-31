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
    /// <remarks>
    /// C1's stand-in contract: its coordinated restore of the replicated <c>factory-orders</c>
    /// can fail under the CI estate's load (#5506), as in the conformance self-test.
    /// </remarks>
    protected override IReadOnlyDictionary<string, string> KnownDefectIssues { get; } =
        new Dictionary<string, string>(StringComparer.Ordinal) { ["C1"] = "#5506" };

    /// <inheritdoc />
    protected override Task RecordKnownDefectAsync(string chapter) =>
        new KnownDefect("#5506", "A coordinated restore's saga outlasts the grain call timeout, and can leave the tree write-fenced").ReproducesAsync(() => Task.CompletedTask);

    /// <summary>Runs the story through <paramref name="chapter"/>.</summary>
    /// <param name="chapter">The chapter id.</param>
    [TestCaseSource(nameof(Chapters))]
    public Task Chapter(string chapter) => RunChapterAsync(chapter);

    private static IEnumerable<TestCaseData> Chapters() => CasesFor(StoryNames.Storylines.Contoso);
}
