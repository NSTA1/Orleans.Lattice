using Orleans.Lattice.Explorer.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Plugins.Schema.Domain;

/// <summary>
/// The Schema plugin's <em>controlled domain model</em>: the single contract the
/// host resolves for it, and therefore the whole of its reach.
/// <para>
/// The plugin declares this type through
/// <see cref="IExplorerPlugin{TDomain}"/> and receives it through
/// <see cref="IExplorerPluginHostContext.GetDomain{TDomain}"/>. It never holds
/// the cluster connection, a gRPC channel, the Explorer's catalog reader, or
/// another plugin's services, so its blast radius is exactly the members below
/// and is reviewable from this one file (epic decision D3).
/// </para>
/// <para>
/// Every member folds a server denial or a transport failure into a non-success
/// envelope rather than throwing, so a component always has something to render
/// and never leaks an unhandled error.
/// </para>
/// </summary>
public interface ISchemaPluginDomain
{
    /// <summary>
    /// Lists the trees this area can govern, projected to the plugin's own
    /// shape. Restore-shadow trees are internal restore artifacts and are never
    /// governance targets, so they are excluded.
    /// </summary>
    /// <param name="cancellationToken">Cancels the listing.</param>
    Task<SchemaTreeCatalog> ListGovernableTreesAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Probes which schema actions the caller may perform on
    /// <paramref name="treeId"/> and files one scoped access decision per
    /// <see cref="SchemaCapability"/>, then returns the read side of those
    /// decisions. Fails closed on a denial or transport fault; never throws.
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancels the probe.</param>
    Task<SchemaTreeGrants> ProbeTreeAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>Reads the enforcement policy for <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    Task<SchemaReadView<LatticeSchemaPolicy>> GetPolicyAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>Sets or replaces the enforcement policy for <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="policy">The policy to apply. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    Task<SchemaOperationResult> SetPolicyAsync(string treeId, LatticeSchemaPolicy policy, CancellationToken cancellationToken = default);

    /// <summary>Clears the enforcement policy for <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    Task<SchemaOperationResult> ClearPolicyAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>Reads the envelope-version config for <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    Task<SchemaReadView<LatticeSchemaVersionConfig>> GetVersionConfigAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>Installs or replaces the envelope-version config for <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="config">The version configuration to install.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    Task<SchemaOperationResult> SetVersionConfigAsync(string treeId, LatticeSchemaVersionConfig config, CancellationToken cancellationToken = default);

    /// <summary>Advances <paramref name="treeId"/>'s target schema version.</summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="newTargetVersion">The new target version.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    Task<SchemaOperationResult> AdvanceTargetVersionAsync(string treeId, uint newTargetVersion, CancellationToken cancellationToken = default);

    /// <summary>Advances <paramref name="treeId"/>'s target version and eagerly re-stamps stored values.</summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="newTargetVersion">The new target version.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    Task<SchemaOperationResult> AdvanceAndMigrateAsync(string treeId, uint newTargetVersion, CancellationToken cancellationToken = default);

    /// <summary>Re-stamps <paramref name="treeId"/>'s stored values to its current target version.</summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    Task<SchemaOperationResult> MigrateToTargetVersionAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>Opts <paramref name="treeId"/> back out of envelope versioning.</summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    Task<SchemaOperationResult> ClearVersionConfigAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>Reads the current or last-known remediation status for <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    Task<SchemaReadView<LatticeSchemaRemediationReport>> GetRemediationStatusAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>Runs a read-only compliance audit of <paramref name="treeId"/> against its policy.</summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancels the scan.</param>
    Task<SchemaReadView<LatticeSchemaComplianceReport>> ScanComplianceAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>Reads the dead-letter count and a bounded page of entries for <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="maxEntries">The maximum number of entries to page in. Must be greater than zero.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    Task<SchemaDeadLetterView> ListDeadLettersAsync(string treeId, int maxEntries, CancellationToken cancellationToken = default);
}
