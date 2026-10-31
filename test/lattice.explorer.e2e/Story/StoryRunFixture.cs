using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

/// <summary>
/// The base of a storyline's story fixture: in story mode it starts the storyline's one estate,
/// and each test case runs the story through one chapter, in dependency order, with nothing
/// seeded. Skipped in isolated mode.
/// </summary>
/// <param name="storyline">The storyline.</param>
public abstract class StoryRunFixture(string storyline)
{
    private JourneyEstate? _estate;
    private StoryRunner? _runner;

    /// <summary>The storyline.</summary>
    protected string Storyline { get; } = storyline;

    /// <summary>Starts the storyline's estate, when the run is in story mode.</summary>
    [OneTimeSetUp]
    public async Task StartStoryAsync()
    {
        if (JourneyRunModes.Current != JourneyRunMode.Story)
        {
            Assert.Ignore($"The {Storyline} story runs only when {JourneyRunModes.Variable}=story.");
        }

        _estate = await StoryEstates.StartAsync(Storyline);
        var session = new StorySession(Storyline, JourneyRunMode.Story, _estate, ChapterContracts.Discovered, StoryPlan.GraphFor(Storyline), _estate.ArtifactsDirectory);
        _runner = new StoryRunner(session, StoryPlan.For(Storyline));
    }

    /// <summary>Publishes the story report and stops the estate.</summary>
    [OneTimeTearDown]
    public async Task StopStoryAsync()
    {
        if (_runner is not null)
        {
            StoryReport.Publish(_runner, Storyline, $"Story run: {Storyline}");
        }

        if (_estate is not null)
        {
            await _estate.DisposeAsync();
        }
    }

    /// <summary>One test case per outline chapter of <paramref name="storyline"/>, numbered in story order.</summary>
    /// <param name="storyline">The storyline.</param>
    protected static IEnumerable<TestCaseData> CasesFor(string storyline) =>
        StoryOutline.For(storyline).Select((chapter, index) =>
            new TestCaseData(chapter.Id).SetName($"{index + 1:D2}_{chapter.Id}"));

    /// <summary>
    /// Runs the story through <paramref name="chapter"/> and reports its outcome: passed; failed
    /// with its failure; or skipped, naming why - blocked by the chapters that failed, or stood in
    /// by its contract because its journey does not exist yet. Never a pass unless it passed.
    /// </summary>
    /// <param name="chapter">The chapter id.</param>
    protected async Task RunChapterAsync(string chapter)
    {
        var result = await _runner!.RunThroughAsync(chapter);
        switch (result.Status)
        {
            case ChapterStatus.Passed:
                return;
            case ChapterStatus.Blocked:
                Assert.Ignore($"BLOCKED, never passed: {chapter} is {result.Detail}.");
                return;
            case ChapterStatus.StoodIn:
                Assert.Ignore($"{chapter} is {result.Detail}.");
                return;
            default:
                Assert.Fail($"{chapter} failed: {result.Failure}");
                return;
        }
    }
}
