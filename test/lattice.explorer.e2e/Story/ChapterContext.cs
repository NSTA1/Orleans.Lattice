using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

/// <summary>
/// What a chapter, or its contract, runs with: the estate, the run's ledger, and the run's
/// mode. One session's contexts share the estate and the ledger.
/// </summary>
public sealed class ChapterContext
{
    private readonly StorySession _session;

    internal ChapterContext(StorySession session, string chapter)
    {
        _session = session;
        Chapter = chapter;
    }

    /// <summary>The chapter this context is for.</summary>
    public string Chapter { get; }

    /// <summary>The storyline.</summary>
    public string Storyline => _session.Storyline;

    /// <summary>The run mode.</summary>
    public JourneyRunMode Mode => _session.Mode;

    /// <summary>The estate.</summary>
    public JourneyEstate Estate => _session.Estate;

    /// <summary>The run's ledger.</summary>
    public StoryLedger Ledger => _session.Ledger;

    /// <summary>The chapters this chapter consumes.</summary>
    public IReadOnlyList<string> Consumes => _session.ConsumesOf(Chapter);

    /// <summary>Where the run's reports are written.</summary>
    public string ArtifactsDirectory => _session.ArtifactsDirectory;

    /// <summary>Cancels the run.</summary>
    public CancellationToken CancellationToken => _session.CancellationToken;

    /// <summary>Records a run-time fact this chapter produced, for the chapters after it.</summary>
    /// <typeparam name="T">The fact's type.</typeparam>
    /// <param name="key">The fixed key.</param>
    /// <param name="value">The fact.</param>
    public void Record<T>(StoryKey<T> key, T value)
        where T : notnull => _session.Ledger.Set(key, value, Chapter);

    /// <summary>A run-time fact a consumed chapter produced.</summary>
    /// <typeparam name="T">The fact's type.</typeparam>
    /// <param name="key">The fixed key.</param>
    public T Recall<T>(StoryKey<T> key)
        where T : notnull => _session.Ledger.Get(key);

    /// <summary>
    /// Establishes another chapter's produces through its contract, at most once per run. A
    /// contract need not call this for its consumes: the session establishes them first.
    /// </summary>
    /// <param name="chapter">The chapter id.</param>
    public Task EstablishAsync(string chapter) => _session.EstablishAsync(chapter);

    /// <summary>The context of another chapter of the same run.</summary>
    /// <param name="chapter">The chapter id.</param>
    public ChapterContext For(string chapter) => _session.ContextFor(chapter);
}
