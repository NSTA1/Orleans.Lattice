namespace Orleans.Lattice;

/// <summary>
/// Identifies the kind of mutation reported to an <see cref="IMutationObserver"/>.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.MutationKind)]
public enum MutationKind
{
    /// <summary>A single key was written.</summary>
    Set = 0,

    /// <summary>A single key was deleted (tombstoned).</summary>
    Delete = 1,

    /// <summary>A key range was deleted - matching live keys in <c>[StartKey, EndExclusiveKey)</c> were tombstoned in bulk.</summary>
    DeleteRange = 2,

    /// <summary>
    /// A saga commit terminal mark - broadcast once per WAL shard a
    /// <c>SetManyAtomicAsync</c> saga touched. When this mutation surfaces
    /// to a leaf via the WAL replay path, every pending-transaction entry
    /// under the same <see cref="LatticeMutation.TransactionId"/> is
    /// flipped from the per-leaf pending-tx map into the visible
    /// projection via <c>LwwValue&lt;byte[]&gt;.Merge</c>. The terminal
    /// mark is the single linearization point for the whole batch on its
    /// WAL shard - every reader on that shard observes either pre-saga
    /// state for every key the saga touched on that shard, or post-saga
    /// state for every such key, never a split.
    /// </summary>
    TxCommit = 3,

    /// <summary>
    /// A saga abort terminal mark - broadcast once per WAL shard a
    /// failed <c>SetManyAtomicAsync</c> saga touched. When this mutation
    /// surfaces to a leaf via the WAL replay path, every pending-transaction
    /// entry under the same <see cref="LatticeMutation.TransactionId"/> is
    /// dropped from the per-leaf pending-tx map without ever becoming
    /// visible to readers, so the saga's prepare-phase writes are
    /// undone in a single linearization step.
    /// </summary>
    TxAbort = 4,
}
