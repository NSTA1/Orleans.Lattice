using System.Reflection;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

/// <summary>
/// The base of every journey fixture. A journey derives from it, declares its chapter with
/// <see cref="JourneyChapterAttribute"/>, lives in <c>Journeys/&lt;id&gt;/</c> (namespace
/// <c>Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.&lt;id&gt;</c>), and implements
/// <see cref="RunAsync"/> with its UI steps.
/// </summary>
/// <remarks>
/// <para>
/// In isolated mode (<c>LATTICE_E2E_MODE</c> unset or <c>isolated</c>) the fixture's one test,
/// <see cref="Isolated"/>, starts a fresh estate for the chapter's storyline, establishes and
/// verifies the chapter's consumes through their F6 contracts, runs <see cref="RunAsync"/>
/// inside a coverage scope named after the chapter, and verifies the chapter's own produces.
/// </para>
/// <para>
/// In story mode the test is skipped: the storyline's story fixture runs the chapter in order
/// on the storyline's one estate.
/// </para>
/// </remarks>
[TestFixture]
[Category("UI")]
[Category("AzureStorageEmulator")]
[Category("EndToEnd")]
[Category("Journey")]
public abstract class JourneyChapter : IStoryChapter
{
    /// <summary>The chapter this journey declares.</summary>
    protected JourneyChapterAttribute Declaration =>
        GetType().GetCustomAttribute<JourneyChapterAttribute>(inherit: false)
            ?? throw new InvalidOperationException($"{GetType().FullName} derives from JourneyChapter but has no [JourneyChapter].");

    /// <inheritdoc />
    public abstract Task RunAsync(ChapterContext context);

    /// <summary>Runs the chapter in isolated mode; skipped in story mode.</summary>
    [Test]
    public async Task Isolated()
    {
        if (JourneyRunModes.Current != JourneyRunMode.Isolated)
        {
            Assert.Ignore($"{JourneyRunModes.Variable}=story: chapter {Declaration.Id} runs inside its storyline's story fixture.");
        }

        var id = Declaration.Id;
        var storyline = Declaration.Storyline ?? StoryOutline.Find(id)?.Storyline
            ?? throw new InvalidOperationException($"Chapter {id} is not in the story outline.");
        await using var estate = await StoryEstates.StartAsync(storyline);
        var session = new StorySession(storyline, JourneyRunMode.Isolated, estate, ChapterContracts.Discovered, StoryPlan.GraphFor(storyline), estate.ArtifactsDirectory);
        try
        {
            await StoryRunner.RunIsolatedAsync(session, id, this);
        }
        finally
        {
            await File.WriteAllTextAsync(Path.Combine(estate.ArtifactsDirectory, $"ledger-{id}.json"), session.Ledger.ToJson());
        }
    }
}
