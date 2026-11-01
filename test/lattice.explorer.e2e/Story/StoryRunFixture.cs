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
    private readonly Dictionary<string, string> _knownDefectBlocks = new(StringComparer.Ordinal);

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
                var issues = result.BlockedBy.Select(blocker => _knownDefectBlocks.GetValueOrDefault(blocker)).Where(issue => issue is not null).Distinct().ToList();
                var known = issues.Count > 0 && result.BlockedBy.All(_knownDefectBlocks.ContainsKey);
                if (known)
                {
                    _knownDefectBlocks[chapter] = issues[0]!;
                }

                Assert.Ignore($"BLOCKED, never passed: {chapter} is {result.Detail}{(known ? $" (known defect {string.Join(", ", issues)})" : string.Empty)}.");
                return;
            case ChapterStatus.StoodIn:
                Assert.Ignore($"{chapter} is {result.Detail}.");
                return;
            default:
                var isImplemented = _runner.Order.First(entry => entry.Id == chapter).IsImplemented;
                if (KnownDefectFor(result, isImplemented, KnownDefectIssues) is { } issue)
                {
                    // An open epic sub-issue tracks this stand-in contract's failure: report it and
                    // block its dependents by the issue, without failing the leg. A real journey's
                    // failure always stays red. I1 (#5400) requires every such entry gone.
                    _knownDefectBlocks[chapter] = issue;
                    await RecordKnownDefectAsync(chapter);
                    Assert.Ignore($"KNOWN DEFECT {issue}: {chapter}'s stand-in contract failed: {result.Detail}");
                }

                Assert.Fail($"{chapter} failed: {result.Failure}");
                return;
        }
    }

    /// <summary>
    /// The issue that tracks <paramref name="result"/>'s failure, or <see langword="null"/> when
    /// it must fail the run: only a failed contract stand-in (a chapter whose journey does not
    /// exist yet) listed in <paramref name="knownDefects"/> is tolerated.
    /// </summary>
    /// <param name="result">The chapter's result.</param>
    /// <param name="isImplemented">Whether the chapter's journey exists (then it is never tolerated).</param>
    /// <param name="knownDefects">The tolerated chapters, as chapter to issue.</param>
    public static string? KnownDefectFor(ChapterResult result, bool isImplemented, IReadOnlyDictionary<string, string> knownDefects)
    {
        ArgumentNullException.ThrowIfNull(result);
        ArgumentNullException.ThrowIfNull(knownDefects);
        return result.Status == ChapterStatus.Failed && !isImplemented && knownDefects.TryGetValue(result.Id, out var issue) ? issue : null;
    }

    /// <summary>
    /// The chapters whose stand-in contract's failure an open epic sub-issue tracks, as chapter
    /// to issue: such a chapter is reported, its dependents are reported blocked by the issue,
    /// and neither fails the leg.
    /// </summary>
    protected virtual IReadOnlyDictionary<string, string> KnownDefectIssues { get; } =
        new Dictionary<string, string>(StringComparer.Ordinal);

    /// <summary>
    /// Records the pending wrapper for a known-defect chapter that failed (a <c>KnownDefect</c>
    /// written with literals, as the pending-wrapper gate requires).
    /// </summary>
    /// <param name="chapter">The chapter.</param>
    protected virtual Task RecordKnownDefectAsync(string chapter) => Task.CompletedTask;
}
