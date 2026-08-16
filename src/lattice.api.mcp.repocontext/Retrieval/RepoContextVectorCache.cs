namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// A warm, in-memory cache of the decoded vector candidate sets behind
/// <see cref="ExactKnnSemanticIndex"/>, so a repeated query does not re-scan all
/// vector metadata and re-decode every payload from the store on every search.
/// Candidates are cached per <c>(repoId, embedding space)</c> - the exact grain the
/// gather already filters to - so a cache hit reproduces byte-identical ranking and
/// recall to the uncached path, including the fail-closed embedding-space guard.
/// <para>
/// <b>Precise invalidation, TTL backstop.</b> A local write to a repository's
/// vectors calls <see cref="Invalidate(string)"/>, which drops that repository's
/// cached sets immediately and race-safely (a search that gathered against the
/// pre-write generation cannot store its now-stale result - see
/// <see cref="CaptureGeneration(string)"/> and
/// <see cref="Store(string, EmbeddingSpaceTag, IReadOnlyList{RepoContextVectorCandidate}, long)"/>).
/// A bounded time-to-live then backstops any change that bypasses the local writer -
/// a vector landing via cross-cluster replication, which the invalidation cannot
/// observe - so a stale set self-heals within
/// <see cref="RepoContextIndexingOptions.VectorCacheTtl"/>. A non-positive TTL
/// disables the cache: every lookup misses, exactly reproducing the uncached path.
/// </para>
/// <para>
/// <b>Thread-safe.</b> Concurrent searches and writes are safe: the per-repository
/// containers and their space lines are held in
/// <see cref="System.Collections.Concurrent.ConcurrentDictionary{TKey, TValue}"/>
/// instances, and a cached candidate list is immutable once stored (the ranker only
/// reads it), so a hit hands back the stored reference without copying.
/// </para>
/// </summary>
internal sealed class RepoContextVectorCache
{
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, RepoEntry> _repos =
        new(StringComparer.Ordinal);
    private readonly TimeProvider _timeProvider;
    private readonly RepoContextIndexingOptions _options;

    /// <summary>Creates the vector cache.</summary>
    /// <param name="timeProvider">The clock used to age cached sets against the TTL. Must not be <see langword="null"/>.</param>
    /// <param name="options">The indexing options carrying <see cref="RepoContextIndexingOptions.VectorCacheTtl"/>. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">Any argument is null.</exception>
    public RepoContextVectorCache(TimeProvider timeProvider, RepoContextIndexingOptions options)
    {
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(options);
        _timeProvider = timeProvider;
        _options = options;
    }

    /// <summary>
    /// Tries to return the cached, still-fresh candidate set for a repository and
    /// embedding space. A set older than <see cref="RepoContextIndexingOptions.VectorCacheTtl"/>,
    /// or absent, is a miss. When the TTL is non-positive every lookup is a miss.
    /// </summary>
    /// <param name="repoId">The repository identifier. Must not be <see langword="null"/>.</param>
    /// <param name="space">The query's embedding space.</param>
    /// <param name="candidates">The cached candidate set on a hit; <see langword="null"/> on a miss.</param>
    /// <returns><see langword="true"/> on a fresh hit; otherwise <see langword="false"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    public bool TryGet(
        string repoId,
        EmbeddingSpaceTag space,
        out IReadOnlyList<RepoContextVectorCandidate> candidates)
    {
        ArgumentNullException.ThrowIfNull(repoId);

        var ttl = _options.VectorCacheTtl;
        if (ttl <= TimeSpan.Zero)
        {
            candidates = Array.Empty<RepoContextVectorCandidate>();
            return false;
        }

        if (_repos.TryGetValue(repoId, out var entry) && entry.Lines.TryGetValue(space, out var line))
        {
            var age = _timeProvider.GetUtcNow() - line.StoredAt;
            if (age < ttl)
            {
                candidates = line.Candidates;
                return true;
            }
        }

        candidates = Array.Empty<RepoContextVectorCandidate>();
        return false;
    }

    /// <summary>
    /// Captures the repository's current cache generation before a search gathers, so
    /// a concurrent <see cref="Invalidate(string)"/> that lands during the gather can
    /// be detected and the now-stale result discarded rather than cached. The
    /// repository's container is created if it does not yet exist.
    /// </summary>
    /// <param name="repoId">The repository identifier. Must not be <see langword="null"/>.</param>
    /// <returns>An opaque generation token to pass to
    /// <see cref="Store(string, EmbeddingSpaceTag, IReadOnlyList{RepoContextVectorCandidate}, long)"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    public long CaptureGeneration(string repoId)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        var entry = _repos.GetOrAdd(repoId, static _ => new RepoEntry());
        return Volatile.Read(ref entry.Generation);
    }

    /// <summary>
    /// Stores a freshly gathered candidate set for a repository and embedding space,
    /// but only when the repository's generation still matches
    /// <paramref name="generation"/> - a store whose generation was superseded by an
    /// <see cref="Invalidate(string)"/> during the gather is dropped, so a stale set
    /// is never cached.
    /// </summary>
    /// <param name="repoId">The repository identifier. Must not be <see langword="null"/>.</param>
    /// <param name="space">The embedding space the candidates were gathered for.</param>
    /// <param name="candidates">The candidate set to cache. Must not be <see langword="null"/>.</param>
    /// <param name="generation">The token from <see cref="CaptureGeneration(string)"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> or <paramref name="candidates"/> is null.</exception>
    public void Store(
        string repoId,
        EmbeddingSpaceTag space,
        IReadOnlyList<RepoContextVectorCandidate> candidates,
        long generation)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(candidates);

        if (_options.VectorCacheTtl <= TimeSpan.Zero)
        {
            return;
        }

        var entry = _repos.GetOrAdd(repoId, static _ => new RepoEntry());
        if (Volatile.Read(ref entry.Generation) != generation)
        {
            // An invalidation raced the gather; the result may predate the write, so
            // it is dropped rather than cached.
            return;
        }

        var line = new CacheLine(candidates, _timeProvider.GetUtcNow());
        entry.Lines[space] = line;

        // Re-check after the store: an invalidation that landed between the guard and
        // the store must not leave a stale line behind.
        if (Volatile.Read(ref entry.Generation) != generation)
        {
            entry.Lines.TryRemove(new KeyValuePair<EmbeddingSpaceTag, CacheLine>(space, line));
        }
    }

    /// <summary>
    /// Drops every cached candidate set for a repository and advances its generation,
    /// so a search that is mid-gather against the old generation cannot cache its
    /// now-stale result. Called after any local mutation of the repository's vectors.
    /// </summary>
    /// <param name="repoId">The repository identifier. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    public void Invalidate(string repoId)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        if (_repos.TryGetValue(repoId, out var entry))
        {
            Interlocked.Increment(ref entry.Generation);
            entry.Lines.Clear();
        }
    }

    private sealed class RepoEntry
    {
        public long Generation;

        public System.Collections.Concurrent.ConcurrentDictionary<EmbeddingSpaceTag, CacheLine> Lines { get; } =
            new();
    }

    private sealed record CacheLine(
        IReadOnlyList<RepoContextVectorCandidate> Candidates, DateTimeOffset StoredAt);
}
