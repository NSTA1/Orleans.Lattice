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

    /// <summary>
    /// The source tree's physical id, resolved and persisted at initiation - before
    /// any alias swap. Cutover arms this physical tree's shards to redirect
    /// logical-alias-routed traffic onto the destination; capturing it up front
    /// (rather than re-resolving the logical name after the swap, which would
    /// follow the new alias to the destination) makes a resume after a partial
    /// cutover arm the correct shards.
    /// </summary>
    [Id(8)] public string? SourcePhysicalTreeId { get; set; }

    /// <summary>
    /// The build mode. <see cref="SchemaRemediationMode.Transform"/> (the default,
    /// so pre-existing durable state deserializes unchanged) applies the static
    /// <see cref="Transform"/> and installs <see cref="TargetPolicy"/> at cutover;
    /// <see cref="SchemaRemediationMode.SchemaVersionMigration"/> re-stamps each
    /// value to <see cref="MigrationTargetVersion"/> through the schema registry and
    /// leaves the tree's existing policy untouched.
    /// </summary>
    [Id(9)] public SchemaRemediationMode Mode { get; set; }

    /// <summary>
    /// The schema-family id an eager version migration re-stamps values to. Meaningful
    /// only when <see cref="Mode"/> is
    /// <see cref="SchemaRemediationMode.SchemaVersionMigration"/>. Persisted so a
    /// failover resumes and re-evaluates the migration identically.
    /// </summary>
    [Id(10)] public uint MigrationSchemaId { get; set; }

    /// <summary>
    /// The target schema version an eager version migration re-stamps values to.
    /// Meaningful only when <see cref="Mode"/> is
    /// <see cref="SchemaRemediationMode.SchemaVersionMigration"/>. Persisted so a
    /// failover resumes and re-evaluates the migration identically.
    /// </summary>
    [Id(11)] public uint MigrationTargetVersion { get; set; }

    /// <summary>
    /// The schema version the last successful eager migration re-stamped the tree
    /// to (<c>0</c> until the first migration completes). Lets a repeat
    /// <c>MigrateToTargetVersionAsync</c> to an already-migrated target short-circuit
    /// to a no-op success instead of rebuilding an identical destination.
    /// </summary>
    [Id(12)] public uint LastCompletedMigrationVersion { get; set; }
}
