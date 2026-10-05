namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// A leaf's kept-snapshot coverage marker, keyed by the leaf's own Guid (issue
/// #4634): the durable record that the leaf's snapshot store once kept a
/// snapshot covering a WAL prefix.
/// <para>
/// Under coverage-gated trim the WAL GC removes a prefix precisely because a
/// snapshot covers it, so a cold start that finds no snapshot must be able to
/// tell a snapshot that vanished from one that never existed. The record lives
/// apart from both the leaf's state row and its snapshot, so it neither adds to
/// the leaf's checkpoint writes nor disappears with the snapshot. The leaf
/// raises it before publishing any durable pin that licenses a trim behind the
/// snapshot.
/// </para>
/// </summary>
[Alias(TypeAliases.ILeafSnapshotCoverageMarkerGrain)]
internal interface ILeafSnapshotCoverageMarkerGrain : IGrainWithGuidKey
{
    /// <summary>The recorded coverage per partition, or <see langword="null"/> when none was ever kept.</summary>
    Task<long[]?> GetAsync();

    /// <summary>
    /// Raises the record to <paramref name="covered"/>, per partition, and
    /// persists it when anything rose. Returns once the provider has durably
    /// accepted the write.
    /// </summary>
    /// <param name="covered">Covered offsets per partition; <c>-1</c> for none.</param>
    Task RaiseAsync(long[] covered);

    /// <summary>
    /// Deletes the record: the leaf is being removed, or an operator rebuild has
    /// accepted the loss of what its snapshot held.
    /// </summary>
    Task ClearAsync();
}
