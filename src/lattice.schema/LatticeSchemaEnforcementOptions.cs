namespace Orleans.Lattice.Schema;

/// <summary>
/// Silo-wide options for schema enforcement, populated through
/// <c>AddLatticeSchemaEnforcement(...)</c>. Per-tree behaviour (the rules and the
/// per-tree strict flag) lives in each tree's <see cref="LatticeSchemaPolicy"/>;
/// these options are the global switches that must be known before any policy is
/// loaded.
/// </summary>
public sealed class LatticeSchemaEnforcementOptions
{
    /// <summary>
    /// Globally enables strict-mode ingest. When <c>false</c> (the default), the
    /// enforcement interceptor never inspects system-origin (replication apply /
    /// restore) writes, so trusted ingest pays zero overhead. When <c>true</c>,
    /// system-origin writes are inspected and a non-compliant item is
    /// dead-lettered for any tree whose policy also sets
    /// <see cref="LatticeSchemaPolicy.StrictIngest"/>. A tree whose policy leaves
    /// strict off is still trusted even when this global switch is on.
    /// </summary>
    public bool StrictIngest { get; set; }

    /// <summary>
    /// Enables the CRDT merge-result observer. Off by default so the merge path
    /// keeps its zero-overhead property; turn on only when merge-result violation
    /// events are wanted. See <see cref="LatticeSchemaMergeObserver"/> for the
    /// current wiring limitation.
    /// </summary>
    public bool ValidateCrdtMergeResults { get; set; }

    /// <summary>
    /// The maximum number of leading value bytes copied into a
    /// <see cref="LatticeSchemaDeadLetterEntry.ValuePreview"/>. Bounds the storage
    /// cost of retaining a diverted item. Must be positive. Defaults to 4096.
    /// </summary>
    public int DeadLetterPreviewMaxBytes { get; set; } = 4096;
}
