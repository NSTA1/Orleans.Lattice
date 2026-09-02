using Microsoft.Extensions.Logging;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The shipped <see cref="IRepoContextAnnIndex"/>: one persisted approximate
/// index per <c>(repository, embedding space)</c>, created on first use, built
/// behind live traffic, and maintained in place from the write seam.
/// <para>
/// <b>The adoption path is the default path.</b> An existing deployment starts
/// with no index at all. Until one is built, a query is answered by the exact
/// scan immediately - so retrieval never stops working and never silently loses
/// recall. Once the build completes, later queries are answered approximately and
/// the answer says so. No operator action, no migration step, no configuration.
/// </para>
/// <para>
/// <b>The registry does not schedule anything.</b> It opens, advances, searches,
/// and maintains an index; deciding <i>when</i> to build belongs to
/// <see cref="IRepoContextAnnIndexBuildGrain"/>, a reminder-anchored coordinator
/// per pair. The registry previously armed a fire-and-forget build from a
/// declining query, which made the work that accelerates queries reachable only
/// from a query: it died with the process, it left a repository nobody queried
/// unindexed forever, and its in-place retry loop existed only because a process
/// death forgot everything. Orleans' single-threaded activation and a durable
/// reminder replace all of it, so what is left here is the deterministic
/// <see cref="BuildStepAsync"/> the coordinator pumps.
/// </para>
/// </summary>
internal sealed class RepoContextAnnIndexRegistry : IRepoContextAnnIndex, IDisposable
{
    private readonly System.Collections.Concurrent.ConcurrentDictionary<PlaneKey, RepoContextAnnIndexHandle> _entries = new();
    private readonly IRepoContextAnnBackingFactory _backing;
    private readonly RepoContextAnnOptions _options;
    private readonly ILogger<RepoContextAnnIndexRegistry> _logger;
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

        var handle = GetOrCreate(repoId, space);
        if (!handle.IsServing)
        {
            // Answering from a half-built index would be quietly incomplete, so the
            // plane declines and the caller serves the exact scan. Nothing is armed
            // here: the build is scheduled by its own durable coordinator, so this
            // path allocates nothing and a repository converges whether or not it
            // is ever queried.
            return new ValueTask<RepoContextAnnSearchOutcome>(RepoContextAnnSearchOutcome.Bootstrapping);
        }

        return handle.SearchAsync(query, k, cancellationToken);
    }

    /// <inheritdoc />
    public bool TryGetProgress(string repoId, EmbeddingSpaceTag space, out VectorIndexBuildProgress progress)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        if (_entries.TryGetValue(new PlaneKey(repoId, space), out var handle))
        {
            progress = handle.Progress;
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
        return _disposed || !_entries.TryGetValue(new PlaneKey(repoId, space), out var handle)
            ? Task.CompletedTask
            : handle.ApplyWriteAsync(upserts, retired, cancellationToken);
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

        foreach (var (key, handle) in _entries)
        {
            if (string.Equals(key.RepoId, repoId, StringComparison.Ordinal))
            {
                await handle
                    .ApplyWriteAsync(Array.Empty<RepoContextAnnVectorUpdate>(), retired, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Advances the index for one repository and embedding space by exactly one
    /// bounded build step, creating it if needed, and reports where it got to.
    /// This is the deterministic entry point the build coordinator's phase pump
    /// drives, and the one a test drives directly so no assertion depends on a
    /// clock or on a race with a background task.
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
        return GetOrCreate(repoId, space).AdvanceAsync(cancellationToken);
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
        return GetOrCreate(repoId, space).EnsureBuiltAsync(cancellationToken);
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
        return _entries.TryGetValue(new PlaneKey(repoId, space), out var handle)
            ? handle.FlushAsync(cancellationToken)
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
        foreach (var handle in _entries.Values)
        {
            handle.Dispose();
        }

        _entries.Clear();
    }

    private RepoContextAnnIndexHandle GetOrCreate(string repoId, EmbeddingSpaceTag space)
    {
        var key = new PlaneKey(repoId, space);
        if (_entries.TryGetValue(key, out var existing))
        {
            return existing;
        }

        var created = new RepoContextAnnIndexHandle(
            repoId,
            space,
            _backing.CreateSource(repoId, space),
            _backing.CreateStore(repoId, space),
            _options,
            LatticeRepoContextAnnBackingFactory.KeyPrefix(repoId, space),
            _logger);

        var winner = _entries.GetOrAdd(key, created);
        if (!ReferenceEquals(winner, created))
        {
            // Another caller created the pair first; only one handle may own the key
            // prefix, so the loser is disposed rather than left holding a store it
            // will never open.
            created.Dispose();
        }

        return winner;
    }

    private readonly record struct PlaneKey(string RepoId, EmbeddingSpaceTag Space);
}
