using System.Collections.Concurrent;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Remembers, per repository, that an exact k-nearest-neighbour gather has
/// already proved it cannot finish, so the next query does not have to prove it
/// again.
/// <para>
/// <b>Why observing beats predicting.</b>
/// <see cref="RepoContextExactScanBudget"/> predicts affordability from a corpus
/// size, and a prediction is only as good as the count it reads. The count is
/// published by the approximate plane's build, so it is <c>0</c> for the whole
/// interval between process start and the build's first published progress -
/// which is exactly the interval a doomed gather has to be prevented in - and
/// even once published it covers a single embedding space while the gather scans
/// the repository's whole vector prefix. Both errors point the same way: the
/// number compared against the threshold is smaller than the quantity being
/// bounded, so the budget clears a scan it was written to skip. A breaker reads
/// no count at all. It reads the <see cref="ScanPageStalledException"/> the
/// gather actually threw, which is the failure itself rather than a proxy for it,
/// and no miscount can defeat it.
/// </para>
/// <para>
/// <b>Keyed by repository, because the scan is.</b> The gather range-scans
/// <see cref="RepoContextKeys.VectorsPrefix(string)"/> and filters by embedding
/// space in memory, so every space in a repository walks identical rows and
/// stalls identically. Keying the breaker per <c>(repository, space)</c> would
/// make each space pay its own victim query for a fact the first one already
/// established.
/// </para>
/// <para>
/// <b>It only ever governs the fallback.</b> A trip is consulted solely while the
/// plane reports <see cref="RepoContextAnnServingState.Bootstrapping"/>, and is
/// cleared the moment the plane answers for itself - so it can never outlive the
/// build whose contention caused it, and a repository that starts serving gets
/// its exact fallback back with no cooldown to wait out.
/// </para>
/// </summary>
internal sealed class RepoContextExactScanBreaker
{
    private readonly ConcurrentDictionary<string, byte> _tripped =
        new(StringComparer.Ordinal);

    /// <summary>
    /// Whether an exact gather over this repository has already stalled and
    /// should not be started again while the plane is still building.
    /// </summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <returns><see langword="true"/> when the breaker is open.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    public bool IsTripped(string repoId)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        return _tripped.ContainsKey(repoId);
    }

    /// <summary>
    /// Records that an exact gather over this repository stalled.
    /// </summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <returns>
    /// <see langword="true"/> when this call opened the breaker, so only the
    /// query that actually paid the ceiling reports it.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    public bool Trip(string repoId)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        return _tripped.TryAdd(repoId, 0);
    }

    /// <summary>
    /// Closes the breaker for a repository, restoring the exact fallback. Called
    /// when the plane answers for itself, which is the evidence that the build
    /// the gather was competing with is no longer holding the tree.
    /// </summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <returns><see langword="true"/> when an open breaker was closed.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    public bool Reset(string repoId)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        return !_tripped.IsEmpty && _tripped.TryRemove(repoId, out _);
    }
}
