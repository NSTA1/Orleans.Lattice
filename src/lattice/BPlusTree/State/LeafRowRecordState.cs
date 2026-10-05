namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// The persisted state of one leaf's row-record sidecar
/// (<see cref="ILeafRowRecordGrain"/>, issue #4654): proof that a state row was
/// once written for the leaf.
/// <para>
/// It lives outside <see cref="LeafNodeState"/> on purpose: it is the evidence a
/// leaf consults when that row is missing, so it must not share the row's fate.
/// It is written once, before the leaf's first row write, and deleted only by the
/// leaf's deliberate clear.
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
}
