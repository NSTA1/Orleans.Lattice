namespace Orleans.Lattice.Schema;

/// <summary>
/// The mode a <see cref="LatticeSchemaRemediationGrain"/> shadow build runs in. The
/// coordinator's dry-run / build / cutover / durable-state / idempotent-resume /
/// abort machinery is shared across both modes; the mode only selects how each
/// value is rewritten and whether a target policy is installed at cutover.
/// </summary>
/// <remarks>
/// The default (<c>0</c>) is <see cref="Transform"/> so durable state persisted by
/// the original enforcement-remediation path deserializes unchanged.
/// </remarks>
[GenerateSerializer]
[Alias(SchemaTypeAliases.SchemaRemediationMode)]
internal enum SchemaRemediationMode
{
    /// <summary>
    /// Enforcement remediation: apply one static <see cref="LatticeValueTransform"/>
    /// to every value, revalidate against a new target policy, and install that
    /// policy at cutover. This is the <c>RemediateAsync</c> path.
    /// </summary>
    Transform = 0,

    /// <summary>
    /// Eager schema-version migration: re-stamp every value to the tree's target
    /// schema version by upcasting each value from its own stamped version through
    /// the registered upcaster chain, validate the upcast value against the tree's
    /// existing enforcement policy (when it has one), and leave that policy
    /// untouched at cutover. This is the <c>AdvanceAndMigrateAsync</c> /
    /// <c>MigrateToTargetVersionAsync</c> path.
    /// </summary>
    SchemaVersionMigration = 1,
}
