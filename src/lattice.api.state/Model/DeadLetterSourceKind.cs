namespace Orleans.Lattice.Api.State;

/// <summary>
/// Identifies the ingest path that produced a dead-letter entry when strict-mode
/// schema enforcement diverted a non-compliant item instead of applying it. This
/// is the state-API-owned projection of the schema package's dead-letter source,
/// so the read surface never leaks the enforcement type across the API boundary.
/// </summary>
[GenerateSerializer]
[Alias(ApiStateTypeAliases.DeadLetterSourceKind)]
public enum DeadLetterSourceKind
{
    /// <summary>The item arrived via cross-cluster replication apply.</summary>
    Replication = 0,

    /// <summary>The item arrived via a backup restore / bulk load.</summary>
    Restore = 1,

    /// <summary>
    /// The item was a rejected local write that a deployment opted to retain for
    /// inspection. Local writes fail closed by default; this source appears only
    /// when the host chose to also capture the rejected value.
    /// </summary>
    LocalRejected = 2,

    /// <summary>
    /// The source could not be mapped to a known kind (a forward-compatibility
    /// fallback for an enforcement source added after this projection).
    /// </summary>
    Unknown = 3,
}
