namespace Orleans.Lattice.Schema;

/// <summary>
/// The <see cref="LatticeOperation.SchemaAdmin"/>-gated control plane for schema
/// versioning: opt a tree in to envelope versioning, read its current config,
/// advance its monotonic target version, and opt it back out. Advancing the target
/// takes effect immediately for new writes (they are stamped at the new version);
/// existing values stamped at an older version are upcast lazily on read through
/// the registered upcaster chain.
/// </summary>
/// <remarks>
/// This is the operator-facing surface over the durable
/// <see cref="ILatticeSchemaVersionStore"/> and the cached
/// <see cref="ILatticeSchemaVersionProvider"/>. Inspecting version state stays on
/// read authority; changing it (set / advance / clear) is the schema-management
/// control plane gated by <see cref="LatticeOperation.SchemaAdmin"/>.
/// </remarks>
public interface ILatticeSchemaVersionAdmin
{
    /// <summary>
    /// Opts <paramref name="treeId"/> in to envelope versioning (or replaces its
    /// existing config) with <paramref name="config"/>. New writes are stamped at
    /// <see cref="LatticeSchemaVersionConfig.TargetVersion"/> immediately.
    /// (<see cref="LatticeOperation.SchemaAdmin"/>.)
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c>, empty, or reserved.</param>
    /// <param name="config">The version configuration to install.</param>
    /// <param name="cancellationToken">Cancels the operation.</param>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c>, empty, or reserved.</exception>
    Task SetVersionConfigAsync(
        string treeId, LatticeSchemaVersionConfig config, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the current version config for <paramref name="treeId"/>, or <c>null</c>
    /// when the tree is unversioned. (Read authority.)
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    Task<LatticeSchemaVersionConfig?> GetVersionConfigAsync(
        string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Advances <paramref name="treeId"/>'s target schema version to
    /// <paramref name="newTargetVersion"/>. The target is <b>monotonic</b>: the new
    /// version must be strictly greater than the current target. New writes are
    /// stamped at the new version immediately; existing values are upcast lazily on
    /// read. (<see cref="LatticeOperation.SchemaAdmin"/>.)
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c>, empty, or reserved.</param>
    /// <param name="newTargetVersion">The new target version. Must be greater than the current target.</param>
    /// <param name="cancellationToken">Cancels the operation.</param>
    /// <returns>The updated config.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c>, empty, or reserved.</exception>
    /// <exception cref="InvalidOperationException">The tree is unversioned, or <paramref name="newTargetVersion"/> does not advance the current target.</exception>
    Task<LatticeSchemaVersionConfig> AdvanceTargetVersionAsync(
        string treeId, uint newTargetVersion, CancellationToken cancellationToken = default);

    /// <summary>
    /// Advances <paramref name="treeId"/>'s target schema version to
    /// <paramref name="newTargetVersion"/> <b>and</b> kicks off a background eager
    /// migration that re-stamps every existing value to the new target, in one call.
    /// The advance reuses <see cref="AdvanceTargetVersionAsync"/>'s validation (the
    /// tree must be versioned and the new target strictly greater) and takes effect
    /// immediately for new writes; the migration then re-stamps existing values by
    /// upcasting each through the registered upcaster chain and re-enveloping it at
    /// the new target, so steady-state reads stop paying the per-read upcast cost.
    /// It aborts on the first value that cannot be upcast, naming the offending key
    /// and a value preview and leaving the tree's data untouched. If the tree has an
    /// enforcement policy, the re-stamped values are validated against it during the
    /// build; the policy itself is left unchanged. (<see cref="LatticeOperation.SchemaAdmin"/>.)
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c>, empty, or reserved.</param>
    /// <param name="newTargetVersion">The new target version. Must be greater than the current target.</param>
    /// <param name="cancellationToken">Cancels the operation.</param>
    /// <returns>The terminal migration report: <see cref="LatticeSchemaRemediationReport.Succeeded"/> on cutover, or <see cref="LatticeSchemaRemediationReport.DidAbort"/> with the first offending entry.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c>, empty, or reserved.</exception>
    /// <exception cref="InvalidOperationException">The tree is unversioned, <paramref name="newTargetVersion"/> does not advance the current target, or schema versioning is not registered.</exception>
    Task<LatticeSchemaRemediationReport> AdvanceAndMigrateAsync(
        string treeId, uint newTargetVersion, CancellationToken cancellationToken = default);

    /// <summary>
    /// Runs (or idempotently resumes / no-ops) an eager migration that re-stamps
    /// every existing value of <paramref name="treeId"/> to the tree's <b>current</b>
    /// target version - the eager pass an operator or a retry can invoke repeatedly
    /// without advancing the target. It is a no-op success when the tree is already
    /// fully migrated, resumes an in-flight migration idempotently, and aborts on the
    /// first value that cannot be upcast, leaving the tree's data untouched.
    /// (<see cref="LatticeOperation.SchemaAdmin"/>.)
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c>, empty, or reserved.</param>
    /// <param name="cancellationToken">Cancels the operation.</param>
    /// <returns>The terminal migration report.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c>, empty, or reserved.</exception>
    /// <exception cref="InvalidOperationException">The tree is unversioned, or schema versioning is not registered.</exception>
    Task<LatticeSchemaRemediationReport> MigrateToTargetVersionAsync(
        string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Opts <paramref name="treeId"/> back out of envelope versioning. New writes are
    /// no longer stamped; already-stamped values remain self-describing and are still
    /// stripped on read. (<see cref="LatticeOperation.SchemaAdmin"/>.)
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the operation.</param>
    /// <returns><c>true</c> when a config was removed; <c>false</c> when the tree was already unversioned.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    Task<bool> ClearVersionConfigAsync(string treeId, CancellationToken cancellationToken = default);
}
