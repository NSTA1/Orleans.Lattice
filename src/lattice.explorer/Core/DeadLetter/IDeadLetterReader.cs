namespace Orleans.Lattice.Explorer.Core.DeadLetter;

/// <summary>
/// Reads a tree's strict-mode dead-letter queue for the DLQ tab and badge over
/// the public state-API dead-letter surface (<c>GetDeadLetterCountAsync</c> /
/// <c>ListDeadLettersAsync</c>). Strictly read-only: it never mutates tree data
/// and never requeues or replays a dead-lettered item.
/// </summary>
/// <remarks>
/// Replay / requeue of a corrected item is deliberately out of scope for this
/// reader (tracked as follow-up work); it would require a write surface the
/// state API does not expose.
/// </remarks>
public interface IDeadLetterReader
{
    /// <summary>
    /// Counts the dead-letter entries currently held for
    /// <paramref name="treeId"/>, for the at-a-glance DLQ badge. Returns <c>0</c>
    /// when the tree has no dead-letter queue or schema enforcement is not
    /// registered on the cluster.
    /// </summary>
    Task<int> CountAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists a page of <paramref name="treeId"/>'s dead-letter entries in append
    /// (time) order. Pass the <paramref name="continuationToken"/> from a prior
    /// page to resume, or <see langword="null"/> to start from the oldest entry.
    /// Returns an empty page when the tree has no dead-letter queue.
    /// </summary>
    Task<DeadLetterPage> ListAsync(
        string treeId,
        int pageSize,
        string? continuationToken = null,
        CancellationToken cancellationToken = default);
}
