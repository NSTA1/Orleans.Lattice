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
    /// Globally enables strict-mode ingest. When <c>false</c> (the default), the
    /// versioning interceptor never inspects system-origin (replication apply /
    /// restore) writes, so trusted ingest pays zero overhead and items are stored
    /// with whatever version tag they carry. When <c>true</c>, system-origin writes
    /// are inspected and an ingested item whose version cannot be upcast to the
    /// tree's target is dead-lettered for any tree whose config also sets
    /// <see cref="LatticeSchemaVersionConfig.StrictIngest"/>.
    /// </summary>
    public bool StrictIngest { get; set; }

    /// <summary>
    /// The maximum number of leading value bytes copied into a
    /// <see cref="LatticeSchemaDeadLetterEntry.ValuePreview"/> when a strict-ingest
    /// item is dead-lettered. Bounds the storage cost of retaining a diverted item.
    /// Must be positive. Defaults to 4096.
    /// </summary>
    public int DeadLetterPreviewMaxBytes { get; set; } = 4096;
}
