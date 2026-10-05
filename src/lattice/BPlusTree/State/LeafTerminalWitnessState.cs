namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// The persisted state of one leaf's applied-terminal witness sidecar
/// (<see cref="ILeafTerminalWitnessGrain"/>, issue #4545): which keys each
/// saga's terminal settled on the leaf without a marked prepare stamp.
/// <para>
/// Kept in its own row, not in <see cref="LeafNodeState"/>, so its size - the
/// rate of such settlements times the time the registry keeps reporting their
/// sagas - never rides the leaf's hot checkpoint writes, and so it needs no cap.
/// It is written only when the witness changes.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LeafTerminalWitnessState)]
internal sealed class LeafTerminalWitnessState : ILatticeBinaryPersistedState
{
    /// <summary>The recorded witnesses, one per saga; <see langword="null"/> when none.</summary>
    [Id(0)] public List<AppliedTerminalWitness>? Witnesses { get; set; }
}
