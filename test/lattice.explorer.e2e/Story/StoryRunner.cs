using System.Diagnostics;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

/// <summary>
/// Runs the chapters of a plan on one <see cref="StorySession"/> in dependency order (story
/// mode), and runs one chapter alone (isolated mode).
/// </summary>
/// <remarks>
/// A chapter whose consumed chapters did not all hold is <see cref="ChapterStatus.Blocked"/>:
/// it never runs, and it is reported blocked by the names of those chapters, never as passed.
/// A chapter whose journey does not exist yet is <see cref="ChapterStatus.StoodIn"/> by its F6
/// contract, so the story keeps running through it while journeys land in parallel; the run
/// reports every stand-in.
/// </remarks>
public sealed class StoryRunner
{
    private readonly StorySession _session;
    private readonly bool _verifyContracts;
    private readonly Dictionary<string, ChapterResult> _results = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim _gate = new(1, 1);

    /// <summary>Creates a runner.</summary>
    /// <param name="session">The session the chapters share.</param>
    /// <param name="plan">The plan, in its preferred order; it is ordered by dependency.</param>
    /// <param name="verifyContracts">
    /// Whether each chapter's contract verifies what its UI steps produced. On for every real
    /// storyline; off only for a self-test whose stub chapters have no contracts.
    /// </param>
    public StoryRunner(StorySession session, IEnumerable<StoryPlanEntry> plan, bool verifyContracts = true)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(plan);
        _session = session;
        _verifyContracts = verifyContracts;
        Order = StoryPlan.Order(plan);
    }

    /// <summary>The plan in the order the chapters run.</summary>
    public IReadOnlyList<StoryPlanEntry> Order { get; }

    /// <summary>The session.</summary>
    public StorySession Session => _session;

    /// <summary>The chapters run so far, in run order.</summary>
    public IReadOnlyList<ChapterResult> Results => [.. Order.Where(entry => _results.ContainsKey(entry.Id)).Select(entry => _results[entry.Id])];

    /// <summary>
    /// Runs every chapter up to and including <paramref name="chapter"/> that has not run yet,
    /// in order, and returns <paramref name="chapter"/>'s result. A test per chapter can call it
    /// in any order and still sees the story run in dependency order.
    /// </summary>
    /// <param name="chapter">The chapter id.</param>
    public async Task<ChapterResult> RunThroughAsync(string chapter)
    {
        if (Order.All(entry => entry.Id != chapter))
        {
            throw new ArgumentException($"Chapter {chapter} is not in the plan.", nameof(chapter));
        }

        await _gate.WaitAsync(_session.CancellationToken);
        try
        {
            foreach (var entry in Order)
            {
                if (!_results.ContainsKey(entry.Id))
                {
                    _results[entry.Id] = await RunOneAsync(entry);
                }

                if (entry.Id == chapter)
                {
                    return _results[entry.Id];
                }
            }

            throw new UnreachableException();
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>Runs every chapter of the plan.</summary>
    public async Task<IReadOnlyList<ChapterResult>> RunAllAsync()
    {
        if (Order.Count > 0)
        {
            await RunThroughAsync(Order[^1].Id);
        }

        return Results;
    }

    /// <summary>
    /// Isolated mode: establishes and verifies <paramref name="chapter"/>'s consumes through
    /// their contracts, runs its UI steps, and verifies its own produces through its contract.
    /// </summary>
    /// <param name="session">A session on a fresh estate.</param>
    /// <param name="id">The chapter id.</param>
    /// <param name="chapter">The chapter's journey.</param>
    public static async Task RunIsolatedAsync(StorySession session, string id, IStoryChapter chapter)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(chapter);
        var consumes = session.ConsumesOf(id);
        foreach (var consume in consumes)
        {
            await session.EstablishAsync(consume);
        }

        foreach (var consume in consumes)
        {
            await session.VerifyAsync(consume);
        }

        await RunChapterStepsAsync(session, id, chapter);
        await session.VerifyAsync(id);
    }

    private static async Task RunChapterStepsAsync(StorySession session, string id, IStoryChapter chapter)
    {
        using var scope = Coverage.BeginScope(id);
        await chapter.RunAsync(session.ContextFor(id));
        scope.Complete();
        session.MarkProduced(id);
    }

    private async Task<ChapterResult> RunOneAsync(StoryPlanEntry entry)
    {
        var blockers = entry.After.Where(dependency => !_results[dependency].Holds).ToList();
        if (blockers.Count > 0)
        {
            return new ChapterResult(entry.Id, ChapterStatus.Blocked, blockers, null, TimeSpan.Zero);
        }

        var clock = Stopwatch.StartNew();
        try
        {
            if (!entry.IsImplemented)
            {
                await _session.EstablishAsync(entry.Id);
                await _session.VerifyAsync(entry.Id);
                return new ChapterResult(entry.Id, ChapterStatus.StoodIn, [], null, clock.Elapsed);
            }

            await RunChapterStepsAsync(_session, entry.Id, entry.Create());
            if (_verifyContracts)
            {
                await _session.VerifyAsync(entry.Id);
            }

            return new ChapterResult(entry.Id, ChapterStatus.Passed, [], null, clock.Elapsed);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !_session.CancellationToken.IsCancellationRequested)
        {
            return new ChapterResult(entry.Id, ChapterStatus.Failed, [], ex, clock.Elapsed);
        }
    }
}
