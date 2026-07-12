namespace Orleans.Lattice.Schema;

/// <summary>
/// Identifies the ingest path that produced a
/// <see cref="LatticeSchemaDeadLetterEntry"/> when strict-mode ingest diverted a
/// non-compliant item rather than applying it.
/// </summary>
[GenerateSerializer]
[Alias(SchemaTypeAliases.LatticeSchemaDeadLetterSource)]
public enum LatticeSchemaDeadLetterSource : byte
{
    /// <summary>The item arrived via cross-cluster replication apply.</summary>
    Replication = 0,

    /// <summary>The item arrived via a backup restore / bulk load.</summary>
    Restore = 1,

    /// <summary>
    /// The item was a rejected local write, optionally captured for inspection.
    /// Local writes fail closed with <see cref="LatticeSchemaViolationException"/>
    /// by default; this source is used only when a deployment opts to also retain
    /// the rejected value.
    /// </summary>
    LocalRejected = 2,
}
