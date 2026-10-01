namespace Orleans.Lattice.Schema;

/// <summary>
/// Silo-wide options for schema versioning, populated through
/// <c>AddLatticeSchemaVersioning(...)</c>. Per-tree behaviour (the schema id, the
/// target version, and the per-tree strict flag) lives in each tree's
/// <see cref="LatticeSchemaVersionConfig"/>; these options are the global switches
/// that must be known before any config is loaded. Mirrors
/// <c>LatticeSchemaEnforcementOptions</c>.
/// </summary>
public sealed class LatticeSchemaVersioningOptions
{
    /// <summary>
    /// Globally enables strict-mode ingest for system-origin writes that reach the
    /// versioning interceptor. When <c>false</c> (the default), the interceptor
    /// skips those system-origin writes, so they pay zero overhead and values keep
    /// whatever version tag they carry. When <c>true</c>, intercepted
    /// system-origin writes are inspected and an item whose version cannot be
    /// upcast to the tree's target is dead-lettered for any tree whose config also
    /// sets <see cref="LatticeSchemaVersionConfig.StrictIngest"/>. Replicated
    /// typed-CRDT deltas and replicated atomic-batch entries reach the interceptor;
    /// a plain last-writer-wins replication apply, a backup restore and a tree
    /// merge bypass write interception and are not made schema-version checked by
    /// this switch.
    /// </summary>
    public bool StrictIngest { get; set; }

    /// <summary>
    /// The maximum number of leading value bytes copied into a
    /// <see cref="LatticeSchemaDeadLetterEntry.ValuePreview"/> when a strict-ingest
    /// item is dead-lettered. Bounds the storage cost of retaining a diverted item.
    /// Values below 1 are clamped to 1. Defaults to 4096.
    /// </summary>
    public int DeadLetterPreviewMaxBytes { get; set; } = 4096;
}
