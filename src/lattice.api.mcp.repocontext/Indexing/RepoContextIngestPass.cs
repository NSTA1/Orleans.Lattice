namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// One reconcile pass as <see cref="RepoContextIngestReporter"/> accounts for it.
/// The runner feeds it every progress report the pass emits and then settles it
/// exactly once, as completed, failed or cancelled; every call after the first
/// settle is a no-op, so a double settle can never count a pass twice.
/// </summary>
/// <remarks>
/// Progress reports carry run-cumulative figures, not deltas. The pass keeps a
/// high-water mark per figure and records only the growth beyond it, so a figure
/// re-reported unchanged records nothing and a smaller figure reported after a
/// larger one is never subtracted from a counter.
/// </remarks>
internal sealed class RepoContextIngestPass
{
    private readonly RepoContextIngestReporter _reporter;

    // High-water marks, one per run-cumulative figure. Fields rather than
    // properties so Advance can update them by reference; guarded by Sync.
    internal long FilesScanned;
    internal long FilesAdded;
    internal long FilesUpdated;
    internal long FilesRemoved;
    internal long FilesUnchanged;
    internal long FilesEmbedded;
    internal long SymbolsEmbedded;
    internal long FilesContentProjected;

    /// <summary>Creates a pass. Called only by <see cref="RepoContextIngestReporter.BeginPass"/>.</summary>
    internal RepoContextIngestPass(
        RepoContextIngestReporter reporter,
        RepoContextIngestRepository repository,
        long startedTimestamp)
    {
        _reporter = reporter;
        Repository = repository;
        StartedTimestamp = startedTimestamp;
    }

    /// <summary>The repository the pass reconciles.</summary>
    internal RepoContextIngestRepository Repository { get; }

    /// <summary>The monotonic timestamp the pass began at.</summary>
    internal long StartedTimestamp { get; }

    /// <summary>Guards the high-water marks and <see cref="Settled"/>.</summary>
    internal Lock Sync { get; } = new();

    /// <summary>Whether the pass has been settled. Read and written under <see cref="Sync"/>.</summary>
    internal bool Settled { get; private set; }

    /// <summary>Records the growth in each figure <paramref name="update"/> carries.</summary>
    /// <param name="update">A progress report from the pass.</param>
    public void Observe(in RepoIndexProgressUpdate update) => _reporter.Observe(this, update);

    /// <summary>
    /// Records the growth in each figure the pass's final <paramref name="result"/>
    /// carries. A no-change pass never reports its plan outcomes as progress, so this
    /// is what lands them.
    /// </summary>
    /// <param name="result">The pass's result.</param>
    /// <exception cref="ArgumentNullException"><paramref name="result"/> is null.</exception>
    public void Observe(RepoContextBootstrapResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        _reporter.Observe(this, new RepoIndexProgressUpdate
        {
            FilesScanned = result.FilesScanned,
            FilesAdded = result.FilesAdded,
            FilesUpdated = result.FilesUpdated,
            FilesRemoved = result.FilesRemoved,
            FilesUnchanged = result.FilesUnchanged,
        });
    }

    /// <summary>Settles the pass as completed and restarts its repository's age gauge.</summary>
    public void Complete() => _reporter.Complete(this);

    /// <summary>Settles the pass as failed.</summary>
    public void Fail() => _reporter.Fail(this);

    /// <summary>Settles the pass as cancelled.</summary>
    public void Cancel() => _reporter.Cancel(this);

    /// <summary>Marks the pass settled, returning <see langword="false"/> when it already was.</summary>
    internal bool TrySettle()
    {
        lock (Sync)
        {
            if (Settled)
            {
                return false;
            }

            Settled = true;
            return true;
        }
    }

    /// <summary>
    /// Raises <paramref name="highWater"/> to <paramref name="reported"/> and returns
    /// the growth, or returns zero when <paramref name="reported"/> is absent or does
    /// not exceed it.
    /// </summary>
    internal static long Advance(ref long highWater, int? reported)
    {
        if (reported is not { } value || value <= highWater)
        {
            return 0;
        }

        var delta = value - highWater;
        highWater = value;
        return delta;
    }
}
