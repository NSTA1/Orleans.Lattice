using Microsoft.Extensions.Logging;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The shipped <see cref="IRepoContextAnnIndex"/>: one persisted approximate
/// index per <c>(repository, embedding space)</c>, created on first use, built
/// behind live traffic, and maintained in place from the write seam.
/// <para>
/// <b>The adoption path is the default path.</b> An existing deployment starts
/// with no index at all. The first query for a repository creates the handle,
/// starts the build in the background, and is answered by the exact scan
/// immediately - so retrieval never stops working and never silently loses
/// recall. Once the build completes, later queries are answered approximately and
/// the answer says so. No operator action, no migration step, no configuration.
/// </para>
/// <para>
/// <b>The build is a task, not a timer.</b> It is started once per pair and
/// re-armed if it faults, so a transient store fault costs a retry on the next
/// query rather than a hot loop or a permanently wedged pair. A host that wants
/// deterministic control - a test, or a silo that drives its own maintenance
/// turns - sets <see cref="RepoContextAnnOptions.AutoBuild"/> to
/// <see langword="false"/> and drives <see cref="BuildStepAsync"/> itself.
/// </para>
/// </summary>
internal sealed class RepoContextAnnIndexRegistry : IRepoContextAnnIndex, IDisposable
{
    private readonly System.Collections.Concurrent.ConcurrentDictionary<PlaneKey, Entry> _entries = new();
    private readonly IRepoContextAnnBackingFactory _backing;
    private readonly RepoContextAnnOptions _options;
    private readonly ILogger<RepoContextAnnIndexRegistry> _logger;
    private readonly CancellationTokenSource _stopping = new();
    private bool _disposed;

    /// <summary>Creates the registry.</summary>
    /// <param name="backing">The factory binding each index to its store of record and its durable store. Must not be <see langword="null"/>.</param>
    /// <param name="options">The plane's shaping and maintenance options. Must not be <see langword="null"/>.</param>
    /// <param name="logger">The logger the build-state report is written to. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">An argument is null.</exception>
    public RepoContextAnnIndexRegistry(
        IRepoContextAnnBackingFactory backing,
        RepoContextAnnOptions options,
        ILogger<RepoContextAnnIndexRegistry> logger)
    {
        ArgumentNullException.ThrowIfNull(backing);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);
        _backing = backing;
        _options = options;
        _logger = logger;
    }

    /// <inheritdoc />
    public ValueTask<RepoContextAnnSearchOutcome> SearchAsync(
        string repoId,
        ReadOnlyMemory<float> query,
        EmbeddingSpaceTag space,
        int k,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(k);

        if (_disposed)
        {
            return new ValueTask<RepoContextAnnSearchOutcome>(RepoContextAnnSearchOutcome.Bootstrapping);
        }

        var entry = GetOrCreate(repoId, space);
        if (!entry.Handle.IsServing)
        {
            // Answering from a half-built index would be quietly incomplete, so the
            // plane declines and the caller serves the exact scan. Arming the build
            // here rather than on a timer means a repository that is never queried
            // never pays to index itself.
            ArmBuild(entry);
            return new ValueTask<RepoContextAnnSearchOutcome>(RepoContextAnnSearchOutcome.Bootstrapping);
        }

        return entry.Handle.SearchAsync(query, k, cancellationToken);
    }

    /// <inheritdoc />
    public bool TryGetProgress(string repoId, EmbeddingSpaceTag space, out VectorIndexBuildProgress progress)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        if (_entries.TryGetValue(new PlaneKey(repoId, space), out var entry))
        {
            progress = entry.Handle.Progress;
            return true;
        }

        progress = default;
        return false;
    }

    /// <inheritdoc />
    public Task ApplyWriteAsync(
        string repoId,
        EmbeddingSpaceTag space,
        IReadOnlyList<RepoContextAnnVectorUpdate> upserts,
        IReadOnlyList<string> retired,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(upserts);
        ArgumentNullException.ThrowIfNull(retired);

        // Deliberately does not create a handle: a write must never be the thing
        // that starts an expensive build, and a pair with no open index picks the
        // write up from the store of record when it is eventually built.
        return _disposed || !_entries.TryGetValue(new PlaneKey(repoId, space), out var entry)
            ? Task.CompletedTask
            : entry.Handle.ApplyWriteAsync(upserts, retired, cancellationToken);
    }

    /// <inheritdoc />
    public async Task ApplyRetirementAsync(
        string repoId, IReadOnlyList<string> retired, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(retired);

        if (_disposed || retired.Count == 0)
        {
            return;
        }

        foreach (var (key, entry) in _entries)
        {
            if (string.Equals(key.RepoId, repoId, StringComparison.Ordinal))
            {
                await entry.Handle
                    .ApplyWriteAsync(Array.Empty<RepoContextAnnVectorUpdate>(), retired, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Advances the index for one repository and embedding space by exactly one
    /// bounded build step, creating it if needed, and reports where it got to.
    /// This is the deterministic entry point a test or a host-driven maintenance
    /// turn uses instead of the background build.
    /// </summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <param name="space">The embedding space.</param>
    /// <param name="cancellationToken">Cancels the step.</param>
    /// <returns>Progress after the step.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    internal Task<VectorIndexBuildProgress> BuildStepAsync(
        string repoId, EmbeddingSpaceTag space, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        return GetOrCreate(repoId, space).Handle.AdvanceAsync(cancellationToken);
    }

    /// <summary>
    /// Drives the index for one repository and embedding space until it is
    /// serving, creating it if needed.
    /// </summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <param name="space">The embedding space.</param>
    /// <param name="cancellationToken">Cancels the build between steps.</param>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    internal Task EnsureBuiltAsync(string repoId, EmbeddingSpaceTag space, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        return GetOrCreate(repoId, space).Handle.EnsureBuiltAsync(cancellationToken);
    }

    /// <summary>
    /// Persists any maintenance the index for one repository and embedding space
    /// is holding. A no-op when no index is open for the pair.
    /// </summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <param name="space">The embedding space.</param>
    /// <param name="cancellationToken">Cancels the flush.</param>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    internal Task FlushAsync(string repoId, EmbeddingSpaceTag space, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        return _entries.TryGetValue(new PlaneKey(repoId, space), out var entry)
            ? entry.Handle.FlushAsync(cancellationToken)
            : Task.CompletedTask;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _stopping.Cancel();
        foreach (var entry in _entries.Values)
        {
            entry.Handle.Dispose();
        }

        _entries.Clear();
        _stopping.Dispose();
    }

    private Entry GetOrCreate(string repoId, EmbeddingSpaceTag space)
    {
        var key = new PlaneKey(repoId, space);
        if (_entries.TryGetValue(key, out var existing))
        {
            return existing;
        }

        var created = new Entry(new RepoContextAnnIndexHandle(
            repoId,
            space,
            _backing.CreateSource(repoId, space),
            _backing.CreateStore(repoId, space),
            _options,
            LatticeRepoContextAnnBackingFactory.KeyPrefix(repoId, space),
            _logger));

        var winner = _entries.GetOrAdd(key, created);
        if (!ReferenceEquals(winner, created))
        {
            // Another query created the pair first; only one handle may own the key
            // prefix, so the loser is disposed rather than left holding a store it
            // will never open.
            created.Handle.Dispose();
        }

        return winner;
    }

    private void ArmBuild(Entry entry)
    {
        if (!_options.AutoBuild || Interlocked.CompareExchange(ref entry.BuildArmed, 1, 0) != 0)
        {
            return;
        }

        StartBuild(entry);
    }

    /// <summary>
    /// Starts the background build. It is a separate, deliberately un-inlined method
    /// because the closure the task captures would otherwise be allocated at the top
    /// of <see cref="ArmBuild"/> - a closure over a parameter is created on entry,
    /// not at the lambda - and so on <b>every</b> declining query, which is exactly
    /// the path an unbuilt deployment takes for every request until its build
    /// finishes. Isolating it moves that allocation to the once-per-pair path it
    /// belongs on.
    /// </summary>
    [System.Runtime.CompilerServices.MethodImpl(
        System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private void StartBuild(Entry entry)
        => _ = Task.Run(() => RunBuildAsync(entry), CancellationToken.None);

    private async Task RunBuildAsync(Entry entry)
    {
        // RETRY IN PLACE, WITH A BOUNDED BACKOFF, RATHER THAN RE-ARMING.
        //
        // This loop used to be a single attempt: any fault tore the build task
        // down, logged, and set BuildArmed back to 0 so the NEXT QUERY started a
        // fresh attempt. The stated reason was that re-arming is "naturally
        // rate-limited by query traffic instead of by a timer nobody can see",
        // and that reasoning is sound FOR A CHEAP RETRY. It is wrong for an
        // expensive one, and this retry is expensive: a build resumes from its
        // last flushed checkpoint, so a fault costs every vector ingested since
        // that flush, and the resumed attempt re-opens the index before it can
        // make any forward progress.
        //
        // Measured on a restored copy of the live deployment (#1844): a corpus of
        // roughly 35,800 vectors faulted about once per flush boundary with a
        // 30-second Orleans response timeout against a saturated shard root, so
        // query-gated re-arming turned a TRANSIENT fault into PERMANENT
        // non-convergence - the index sat at one flush (4,096 vectors) and never
        // reached Ready, so query cost stayed proportional to corpus size and
        // cold start could not improve. Retrying in place lets the build absorb
        // the transient and carry on from its checkpoint.
        //
        // The backoff is linear and capped so a genuinely broken store still
        // yields rather than spinning, and the loop still gives up eventually and
        // re-arms, so a query can start a fresh attempt later.
        for (var attempt = 1; attempt <= BuildAttempts; attempt++)
        {
            try
            {
                await entry.Handle.EnsureBuiltAsync(_stopping.Token).ConfigureAwait(false);
                return;
            }
            catch (OperationCanceledException) when (_stopping.IsCancellationRequested)
            {
                // The host is shutting down. The index is derived, so an interrupted
                // build costs the next start a resume from its last checkpoint.
                return;
            }
            catch (ObjectDisposedException)
            {
                // The registry was disposed while the build was in flight.
                return;
            }
            catch (Exception ex)
            {
                // The build is best-effort by construction: retrieval keeps working
                // through the exact scan the whole time it is not serving.
                var lastAttempt = attempt == BuildAttempts;
                _logger.LogWarning(
                    ex,
                    "Repository-context approximate index build step {Attempt} of {Attempts} did not complete; "
                    + "semantic retrieval continues through the exact path and the build {Continuation}.",
                    attempt,
                    BuildAttempts,
                    lastAttempt ? "will be retried on the next query" : "resumes from its last checkpoint");

                if (lastAttempt)
                {
                    Volatile.Write(ref entry.BuildArmed, 0);
                    return;
                }

                try
                {
                    await Task.Delay(BackoffFor(attempt), _stopping.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }
    }

    /// <summary>
    /// How many times a background build absorbs a transient fault and resumes
    /// from its checkpoint before handing back to the query-gated re-arm.
    /// </summary>
    private const int BuildAttempts = 32;

    /// <summary>
    /// Backoff before a build resumes from its checkpoint. Short at first, so a
    /// TRANSIENT fault - a saturated shard root timing out, which is the case
    /// measured on a real deployment - is absorbed promptly rather than costing
    /// seconds of stalled progress per flush boundary. It grows linearly and is
    /// capped, so a persistently broken store yields the thread pool instead of
    /// spinning.
    /// </summary>
    private static TimeSpan BackoffFor(int attempt)
        => TimeSpan.FromMilliseconds(Math.Min(200 * attempt, 10_000));

    private sealed class Entry(RepoContextAnnIndexHandle handle)
    {
        public int BuildArmed;

        public RepoContextAnnIndexHandle Handle { get; } = handle;
    }

    private readonly record struct PlaneKey(string RepoId, EmbeddingSpaceTag Space);
}
