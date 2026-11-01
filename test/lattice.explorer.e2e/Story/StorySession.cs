using System.Collections.Concurrent;
using System.Collections.Immutable;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

/// <summary>
/// One run of a storyline - a story run, or one isolated chapter's run - on one estate: its
/// ledger, its contracts, its chapter graph, and which chapters' produces already hold. It
/// establishes a chapter at most once, after everything that chapter consumes.
/// </summary>
public sealed class StorySession
{
    private static readonly AsyncLocal<ImmutableStack<string>?> Establishing = new();
    private readonly IReadOnlyDictionary<string, IReadOnlyList<string>> _graph;
    private readonly Dictionary<string, int> _storyPosition;
    private readonly ConcurrentDictionary<string, Lazy<Task>> _established = new(StringComparer.Ordinal);
    private readonly JourneyEstate? _estate;

    /// <summary>Creates a session.</summary>
    /// <param name="storyline">The storyline.</param>
    /// <param name="mode">The run mode.</param>
    /// <param name="estate">The estate, or <see langword="null"/> for a self-test that needs none.</param>
    /// <param name="contracts">The contracts the session may call.</param>
    /// <param name="graph">Each chapter of the storyline, with the chapters it consumes.</param>
    /// <param name="artifactsDirectory">Where the session's reports are written.</param>
    /// <param name="cancellationToken">Cancels the run.</param>
    public StorySession(
        string storyline,
        JourneyRunMode mode,
        JourneyEstate? estate,
        ChapterContracts contracts,
        IReadOnlyDictionary<string, IReadOnlyList<string>> graph,
        string artifactsDirectory,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(storyline);
        ArgumentNullException.ThrowIfNull(contracts);
        ArgumentNullException.ThrowIfNull(graph);
        ArgumentException.ThrowIfNullOrEmpty(artifactsDirectory);
        Storyline = storyline;
        Mode = mode;
        _estate = estate;
        Contracts = contracts;
        _graph = graph;
        _storyPosition = StoryPlan.Order(graph.Select(entry => new StoryPlanEntry(entry.Key, entry.Value, null)))
            .Select((entry, index) => (entry.Id, index))
            .ToDictionary(pair => pair.Id, pair => pair.index, StringComparer.Ordinal);
        ArtifactsDirectory = artifactsDirectory;
        CancellationToken = cancellationToken;
    }

    /// <summary>The storyline.</summary>
    public string Storyline { get; }

    /// <summary>The run mode.</summary>
    public JourneyRunMode Mode { get; }

    /// <summary>The estate.</summary>
    /// <exception cref="InvalidOperationException">The session has no estate (a self-test).</exception>
    public JourneyEstate Estate => _estate ?? throw new InvalidOperationException($"The {Storyline} session runs without an estate.");

    /// <summary>Whether the session has an estate.</summary>
    public bool HasEstate => _estate is not null;

    /// <summary>The run's ledger.</summary>
    public StoryLedger Ledger { get; } = new();

    /// <summary>The contracts the session may call.</summary>
    public ChapterContracts Contracts { get; }

    /// <summary>Where the session's reports are written.</summary>
    public string ArtifactsDirectory { get; }

    /// <summary>Cancels the run.</summary>
    public CancellationToken CancellationToken { get; }

    /// <summary>The chapters <paramref name="chapter"/> consumes.</summary>
    /// <param name="chapter">The chapter id.</param>
    public IReadOnlyList<string> ConsumesOf(string chapter) =>
        _graph.TryGetValue(chapter, out var after)
            ? after
            : throw new ArgumentException($"Chapter {chapter} is not in the {Storyline} storyline.", nameof(chapter));

    /// <summary>
    /// Every chapter <paramref name="chapter"/> consumes, directly or through another, in story
    /// order: the order the story itself produces them, so an isolated run establishes them in
    /// the same order (a later chapter's seeder can depend on an earlier one having run first,
    /// beyond what the consumes graph states).
    /// </summary>
    /// <param name="chapter">The chapter id.</param>
    public IReadOnlyList<string> EstablishmentOrderOf(string chapter)
    {
        var closure = new HashSet<string>(StringComparer.Ordinal);
        var pending = new Stack<string>(ConsumesOf(chapter));
        while (pending.TryPop(out var next))
        {
            if (closure.Add(next))
            {
                foreach (var consume in ConsumesOf(next))
                {
                    pending.Push(consume);
                }
            }
        }

        return [.. closure.OrderBy(id => _storyPosition[id])];
    }

    /// <summary>The context a chapter, or its contract, runs with.</summary>
    /// <param name="chapter">The chapter id.</param>
    public ChapterContext ContextFor(string chapter)
    {
        _ = ConsumesOf(chapter);
        return new ChapterContext(this, chapter);
    }

    /// <summary>
    /// Establishes <paramref name="chapter"/>'s produces through its contract, after establishing
    /// everything it consumes, at most once per session. A chapter already produced through the
    /// UI (<see cref="MarkProduced"/>) is not established again.
    /// </summary>
    /// <param name="chapter">The chapter id.</param>
    public Task EstablishAsync(string chapter)
    {
        _ = ConsumesOf(chapter);
        if (Establishing.Value is { } stack && stack.Contains(chapter))
        {
            throw new InvalidOperationException(
                $"Establishing {chapter} requires establishing {chapter}: {string.Join(" <- ", stack.Prepend(chapter))}.");
        }

        return _established.GetOrAdd(chapter, id => new Lazy<Task>(() => EstablishCoreAsync(id))).Value;
    }

    /// <summary>Verifies <paramref name="chapter"/>'s produces through its contract.</summary>
    /// <param name="chapter">The chapter id.</param>
    /// <exception cref="MissingChapterContractException">The chapter has no contract.</exception>
    public Task VerifyAsync(string chapter)
    {
        var contract = Contracts.Find(chapter) ?? throw new MissingChapterContractException(chapter);
        return contract.VerifyAsync(ContextFor(chapter));
    }

    /// <summary>Records that <paramref name="chapter"/>'s produces already hold, because its UI steps produced them.</summary>
    /// <param name="chapter">The chapter id.</param>
    public void MarkProduced(string chapter)
    {
        _ = ConsumesOf(chapter);
        _established[chapter] = new Lazy<Task>(Task.CompletedTask);
    }

    /// <summary>Whether <paramref name="chapter"/>'s produces are established or produced.</summary>
    /// <param name="chapter">The chapter id.</param>
    public bool IsEstablished(string chapter) =>
        _established.TryGetValue(chapter, out var established) && established.IsValueCreated && established.Value.IsCompletedSuccessfully;

    private async Task EstablishCoreAsync(string chapter)
    {
        Establishing.Value = (Establishing.Value ?? ImmutableStack<string>.Empty).Push(chapter);
        foreach (var consume in EstablishmentOrderOf(chapter))
        {
            await EstablishAsync(consume);
        }

        var contract = Contracts.Find(chapter) ?? throw new MissingChapterContractException(chapter);
        await contract.EstablishAsync(ContextFor(chapter));
    }
}
