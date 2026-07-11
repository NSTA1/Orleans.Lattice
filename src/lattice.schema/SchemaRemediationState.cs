namespace Orleans.Lattice.Schema;

/// <summary>
/// Durable coordinator state for <see cref="LatticeSchemaRemediationGrain"/>.
/// Persisted before every external side effect so an in-flight remediation resumes
/// at its last recorded phase after a silo restart, and so a re-trigger with the
/// same parameters is idempotent.
/// </summary>
[GenerateSerializer]
[Alias(SchemaTypeAliases.SchemaRemediationState)]
internal sealed class SchemaRemediationState
{
    /// <summary>Whether a remediation build is currently in flight.</summary>
    [Id(0)] public bool InProgress { get; set; }

    /// <summary>The current phase of the remediation.</summary>
    [Id(1)] public LatticeSchemaRemediationPhase Phase { get; set; }

    /// <summary>
    /// The unique operation id for the in-flight (or last) remediation. Derives the
    /// destination physical tree id and disambiguates idempotent retries.
    /// </summary>
    [Id(2)] public string? OperationId { get; set; }

    /// <summary>The destination physical tree id the shadow build populates and the logical tree is cut over to.</summary>
    [Id(3)] public string? DestinationTreeId { get; set; }

    /// <summary>The remediation transform applied to each value during the dry-run and build.</summary>
    [Id(4)] public LatticeValueTransform Transform { get; set; }

    /// <summary>The target policy the transformed values must satisfy and that governs the tree after cutover.</summary>
    [Id(5)] public LatticeSchemaPolicy? TargetPolicy { get; set; }

    /// <summary>The last terminal report (completed or aborted). <c>null</c> until the first remediation finishes.</summary>
    [Id(6)] public LatticeSchemaRemediationReport? LastReport { get; set; }

    /// <summary>The number of entries scanned so far in the current phase, for in-flight reporting.</summary>
    [Id(7)] public int ScannedCount { get; set; }
}
