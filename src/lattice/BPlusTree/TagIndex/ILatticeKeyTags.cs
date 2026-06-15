namespace Orleans.Lattice;

/// <summary>
/// The per-key tag surface of a tag index. Reads the tags currently associated
/// with a single key and mutates that association (replace / add / remove).
/// </summary>
public interface ILatticeKeyTags
{
    /// <summary>
    /// Returns the tags currently associated with the key, in ascending
    /// ordinal order. Returns an empty list when the key has no tags.
    /// </summary>
    /// <param name="cancellationToken">Cancels the index scan.</param>
    Task<IReadOnlyList<string>> GetAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Replaces the key's tag set with <paramref name="tags"/>: the current set
    /// is read, diffed against the desired set, stale membership rows are
    /// deleted, and new ones are added. A tag that is already present is left
    /// untouched.
    /// </summary>
    /// <param name="tags">The exact set of tags the key should carry afterwards.</param>
    /// <param name="cancellationToken">Cancels the read-modify-write.</param>
    Task SetAsync(IEnumerable<string> tags, CancellationToken cancellationToken = default);

    /// <summary>
    /// Associates <paramref name="tags"/> with the key in addition to any
    /// existing tags. Adding an already-present tag is a no-op.
    /// </summary>
    /// <param name="tags">The tags to add.</param>
    /// <param name="cancellationToken">Cancels the membership writes.</param>
    Task AddAsync(IEnumerable<string> tags, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes the association between the key and each of <paramref name="tags"/>.
    /// Removing a tag the key does not carry is a no-op.
    /// </summary>
    /// <param name="tags">The tags to remove.</param>
    /// <param name="cancellationToken">Cancels the membership deletes.</param>
    Task RemoveAsync(IEnumerable<string> tags, CancellationToken cancellationToken = default);
}
