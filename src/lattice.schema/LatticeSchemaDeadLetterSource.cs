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
    /// <summary>The item arrived through an intercepted system-origin replication-class write.</summary>
    Replication = 0,

    /// <summary>The item arrived through an intercepted bulk-load or restore-class write.</summary>
    Restore = 1,

    /// <summary>
    /// Reserved for a local-write rejection source. The current built-in strict
    /// ingest paths record intercepted replication-class and restore-class sources; ordinary local writes
    /// fail closed with <see cref="LatticeSchemaViolationException"/> and are not
    /// recorded as dead letters by this enum member.
    /// </summary>
    LocalRejected = 2,
}
