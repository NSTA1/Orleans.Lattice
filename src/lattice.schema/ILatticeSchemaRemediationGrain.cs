namespace Orleans.Lattice.Schema;

/// <summary>
/// The durable, per-tree background schema-remediation coordinator grain. One
/// activation exists per logical tree, keyed by <c>{treeId}</c>. It runs the
/// dry-run gate, builds a remediated destination physical tree, and cuts the
/// logical tree over to it. This is internal infrastructure; operators drive it
/// through the <see cref="LatticeOperation.SchemaAdmin"/>-gated
/// <see cref="ILatticeSchemaRemediationAdmin"/> surface.
/// </summary>
[Alias(SchemaTypeAliases.ILatticeSchemaRemediationGrain)]
internal interface ILatticeSchemaRemediationGrain : IGrainWithStringKey
{
    /// <summary>
    /// Starts a remediation of the tree with <paramref name="transform"/> and
    /// <paramref name="targetPolicy"/>, driving it synchronously through the
    /// dry-run gate, the destination build, and cutover, and returning the
    /// resulting report.
    /// <para>
    /// Idempotent: a call with the same parameters while a remediation is in flight
    /// resumes it; a call with different parameters while one is in flight throws
    /// <see cref="InvalidOperationException"/>.
    /// </para>
    /// </summary>
    /// <param name="transform">The per-value remediation transform.</param>
    /// <param name="targetPolicy">The policy the transformed values must satisfy and that governs the tree after cutover. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the operation.</param>
    /// <returns>The terminal report (completed or aborted).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="targetPolicy"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="targetPolicy"/> carries an uncompilable regex rule.</exception>
    /// <exception cref="InvalidOperationException">A remediation with different parameters is already in flight.</exception>
    Task<LatticeSchemaRemediationReport> StartAsync(
        LatticeValueTransform transform, LatticeSchemaPolicy targetPolicy, CancellationToken cancellationToken = default);

    /// <summary>
    /// Drives an in-flight remediation through its remaining phases synchronously,
    /// in a single call. A no-op when no remediation is in flight. Intended for
    /// resumption after a restart and for manual / test triggers.
    /// </summary>
    Task RunRemediationPassAsync();

    /// <summary>
    /// Starts (or idempotently resumes / no-ops) an eager schema-version migration
    /// of the tree: re-stamp every existing value to <paramref name="targetVersion"/>
    /// by upcasting each value from its own stamped version through the registered
    /// upcaster chain, validate the upcast value against the tree's existing
    /// enforcement policy (when it has one), and - only if every value re-stamps -
    /// cut the tree over to the re-stamped destination, leaving the tree's policy
    /// untouched. Aborts on the first value that cannot be upcast, naming the
    /// offending key and a value preview and leaving the original tree untouched.
    /// <para>
    /// Idempotent: a call while a migration to the same <c>(schemaId, targetVersion)</c>
    /// is in flight resumes it; a call after the tree is already fully migrated to
    /// <paramref name="targetVersion"/> is a no-op success; a call while a build with
    /// different parameters is in flight throws <see cref="InvalidOperationException"/>.
    /// </para>
    /// </summary>
    /// <param name="schemaId">The schema-family id to stamp legacy un-enveloped values with.</param>
    /// <param name="targetVersion">The target schema version to re-stamp every value to.</param>
    /// <param name="cancellationToken">Cancels the operation.</param>
    /// <returns>The terminal report (completed or aborted).</returns>
    /// <exception cref="InvalidOperationException">Schema versioning is not registered, or a build with different parameters is already in flight.</exception>
    Task<LatticeSchemaRemediationReport> StartVersionMigrationAsync(
        uint schemaId, uint targetVersion, CancellationToken cancellationToken = default);

    /// <summary>Reads the current or last-known remediation status for the tree.</summary>
    Task<LatticeSchemaRemediationReport> GetStatusAsync();
}
