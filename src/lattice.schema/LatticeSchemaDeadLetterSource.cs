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
    /// Reserved for a local-write rejection source. The current built-in strict
    /// ingest paths record replication and restore sources; ordinary local writes
    /// fail closed with <see cref="LatticeSchemaViolationException"/> and are not
    /// recorded as dead letters by this enum member.
    /// </summary>
    LocalRejected = 2,
}
