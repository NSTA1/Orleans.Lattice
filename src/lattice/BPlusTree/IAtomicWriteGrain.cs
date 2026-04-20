using System.ComponentModel;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Saga coordinator for atomic multi-key writes. One grain activation
/// per in-flight batch, keyed by <c>{treeId}/{operationId}</c>. Applies each
/// write sequentially through <see cref="ILattice"/>, persists progress after
/// every step, and compensates already-committed keys when a step throws.
/// <para>
/// Compensation rewrites the pre-saga value (or tombstones the key when it was
/// absent before the saga) with a freshly-ticked <c>HybridLogicalClock</c>, so
/// LWW merge semantics guarantee the rollback wins over the partial write.
/// Crash recovery is reminder-driven: on reactivation the grain consults its
/// persisted <see cref="State.AtomicWriteState.Phase"/> and resumes.
/// </para>
/// <para>
/// Readers may observe a brief partial-visibility window between the first
/// and last committed write; callers needing strict isolation should layer
/// version-guarded reads (<see cref="ILattice.GetWithVersionAsync"/> +
/// <see cref="ILattice.SetIfVersionAsync"/>) on top.
/// </para>
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
[Alias(TypeAliases.IAtomicWriteGrain)]
public interface IAtomicWriteGrain : IGrainWithStringKey
{
    /// <summary>
    /// Starts (or resumes) the atomic write saga for <paramref name="entries"/>
    /// against the tree identified by <paramref name="treeId"/>. Returns when
    /// every entry has been committed. Throws the originating exception after
    /// successful compensation if any step fails mid-flight.
    /// </summary>
    /// <param name="treeId">Logical tree ID to write into.</param>
    /// <param name="entries">Key-value pairs to commit atomically. Must not contain duplicate keys.</param>
    Task ExecuteAsync(string treeId, List<KeyValuePair<string, byte[]>> entries);

    /// <summary>
    /// Returns <c>true</c> when the saga has finished (either all writes
    /// committed or compensation completed) or has not been started.
    /// </summary>
    Task<bool> IsCompleteAsync();
}
