using Orleans.Lattice.Explorer.Core.DeadLetter;

namespace Orleans.Lattice.Explorer.Plugins.DeadLetter;

/// <summary>
/// The controlled domain model of the dead-letter surface: a count and a paged
/// list, and nothing else.
/// <para>
/// This is the whole of the plugin's reach (epic decision D3). It is
/// deliberately read-only in the type system as well as in behaviour - there is
/// no replay, requeue or delete on this contract, so the surface could not
/// mutate a dead-lettered entry even if a future view tried to.
/// </para>
/// </summary>
public interface IDeadLetterSurface
{
    /// <summary>
    /// Counts the dead-letter entries currently held for
    /// <paramref name="treeId"/>. Returns <c>0</c> when the tree has no queue.
    /// </summary>
    /// <param name="treeId">The selected tree or view id. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancelled when the view is torn down.</param>
    Task<int> CountAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists a page of <paramref name="treeId"/>'s dead-letter entries in append
    /// order. Pass the <paramref name="continuationToken"/> from a prior page to
    /// resume, or <see langword="null"/> to start from the oldest entry.
    /// </summary>
    /// <param name="treeId">The selected tree or view id. Must not be <see langword="null"/>.</param>
    /// <param name="pageSize">The maximum number of entries to return.</param>
    /// <param name="continuationToken">The prior page's cursor, or <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancelled when the view is torn down.</param>
    Task<DeadLetterPage> ListAsync(
        string treeId,
        int pageSize,
        string? continuationToken = null,
        CancellationToken cancellationToken = default);
}
