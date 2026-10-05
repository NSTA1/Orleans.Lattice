namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// The persisted state of one leaf's row-record sidecar
/// (<see cref="ILeafRowRecordGrain"/>, issue #4654): proof that a state row was
/// once written for the leaf.
/// <para>
/// It lives outside <see cref="LeafNodeState"/> on purpose: it is the evidence a
/// leaf consults when that row is missing, so it must not share the row's fate.
/// It is written once, before the leaf's first row write, and deleted only by the
/// leaf's deliberate clear. A purge's clear does not delete it but marks it
/// <see cref="PurgeCleared"/> (issue #4700), so recovery can tell the purge's own
/// clear apart from a lost row.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LeafRowRecordState)]
internal sealed class LeafRowRecordState : ILatticeBinaryPersistedState
{
    /// <summary>
    /// The tree the leaf was bound to when its row was first recorded, for
    /// diagnostics; <see langword="null"/> for a leaf whose row was written
    /// before it was bound.
    /// </summary>
    [Id(0)] public string? TreeId { get; set; }

    /// <summary>
    /// Whether a purge committed to clear the leaf (issue #4700). Written, as the
    /// first step of the purge's clear, before the leaf's row is deleted, and kept
    /// after it, so a rowless leaf carrying it is one the purge cleared on purpose:
    /// recovery of the tree may re-create it empty. Reset when the leaf's row is
    /// written again. A rowless leaf without it is one whose row may have been lost,
    /// and is never re-created empty.
    /// </summary>
    [Id(1)] public bool PurgeCleared { get; set; }
}
