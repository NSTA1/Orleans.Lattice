using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The row-record sidecar of one leaf, keyed by the leaf's own Guid (issue #4654).
/// <para>
/// A leaf whose state row is missing has lost its only link to its tree, key
/// range, checkpoint and kept-snapshot record, so on its own it cannot tell a row
/// that vanished from one that was never written - and the second is every leaf's
/// first activation. This record answers that question: the leaf writes it before
/// its first row write and deletes it only after deliberately clearing that row,
/// so a missing row with the record present is a row that was lost.
/// </para>
/// </summary>
[Alias(TypeAliases.ILeafRowRecordGrain)]
internal interface ILeafRowRecordGrain : IGrainWithGuidKey
{
    /// <summary>
    /// Returns the recorded state, or <see langword="null"/> when no row has been
    /// recorded for the leaf (or the record was cleared).
    /// </summary>
    Task<LeafRowRecordState?> GetAsync();

    /// <summary>
    /// Records that a state row is being written for the leaf, and resets any
    /// <see cref="LeafRowRecordState.PurgeCleared"/> mark. Idempotent; returns once
    /// the provider has durably accepted the record.
    /// </summary>
    /// <param name="treeId">The tree the leaf is bound to, for diagnostics; <see langword="null"/> when unbound.</param>
    Task RecordAsync(string? treeId);

    /// <summary>
    /// Marks the record <see cref="LeafRowRecordState.PurgeCleared"/>: a purge has
    /// committed to clear the leaf (issue #4700). Written before the leaf's row is
    /// deleted; returns once the provider has durably accepted it.
    /// </summary>
    Task MarkPurgeClearedAsync();

    /// <summary>Deletes the record, after the leaf's row has been deliberately cleared.</summary>
    Task ClearAsync();
}
