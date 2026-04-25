namespace Orleans.Lattice.Replication;

/// <summary>
/// Identifies the kind of mutation captured in a <see cref="ReplogEntry"/>.
/// Mirrors <see cref="MutationKind"/> from the core library; a separate
/// enum is exposed in the replication package so the wire contract is not
/// coupled to the core mutation-observer API.
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ReplogOp)]
public enum ReplogOp
{
    /// <summary>A single key was written.</summary>
    Set = 0,

    /// <summary>A single key was deleted (tombstoned).</summary>
    Delete = 1,

    /// <summary>A key range was deleted; matching live keys in <c>[Key, EndExclusiveKey)</c> were tombstoned in bulk.</summary>
    DeleteRange = 2,
}
