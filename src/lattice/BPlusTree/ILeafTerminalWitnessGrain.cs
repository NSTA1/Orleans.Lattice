using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The durable applied-terminal witness sidecar of one leaf, keyed by the
/// leaf's own Guid (issue #4545).
/// <para>
/// A leaf records which keys each saga's terminal settled on it without a marked
/// prepare stamp, so that a destination-side shadow marker arriving after that
/// terminal - a delayed shadow forward or sweep replay, after a reactivation, or
/// on a split sibling - is recognised and does not hide the key. The record is
/// written here rather than in the leaf's own state row so its size never rides
/// the leaf's checkpoint writes; the leaf writes it before any state write that
/// could advance its checkpoint past the terminal, so the write-ahead log keeps
/// it recoverable until then.
/// </para>
/// </summary>
[Alias(TypeAliases.ILeafTerminalWitnessGrain)]
internal interface ILeafTerminalWitnessGrain : IGrainWithGuidKey
{
    /// <summary>Returns every recorded witness; empty when none.</summary>
    Task<AppliedTerminalWitness[]> LoadAsync();

    /// <summary>
    /// Merges <paramref name="add"/> into the record (a union of keys per saga),
    /// then drops every saga in <paramref name="remove"/>, and persists the
    /// result when it changed. Returns once the provider has durably accepted
    /// the write.
    /// </summary>
    /// <param name="add">Witnesses to add; <see langword="null"/> for none.</param>
    /// <param name="remove">Sagas to drop; <see langword="null"/> for none.</param>
    Task ApplyAsync(AppliedTerminalWitness[]? add, Guid[]? remove);

    /// <summary>Deletes the record, for a leaf that is being removed.</summary>
    Task ClearAsync();
}
