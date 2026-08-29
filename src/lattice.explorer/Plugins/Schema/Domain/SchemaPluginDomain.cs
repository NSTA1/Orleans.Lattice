using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Plugins.Schema.Domain;

/// <summary>
/// The host-side adapter behind <see cref="ISchemaPluginDomain"/>: it composes
/// the Schema feature services and the Explorer's catalog reader into the single
/// contract the plugin is allowed to see, and publishes the per-tree probe as
/// scoped decisions in the keyed access store.
/// <para>
/// It lives on the host's side of the seam by construction: the components never
/// resolve it themselves, they receive it from
/// <see cref="IExplorerPluginHostContext.GetDomain{TDomain}"/>, and it is the
/// only type in the plugin that touches a service the plugin did not declare.
/// </para>
/// </summary>
/// <param name="catalog">The Explorer catalog reader governable trees are discovered through.</param>
/// <param name="policy">The enforcement-policy service.</param>
/// <param name="versioning">The envelope-versioning and remediation service.</param>
/// <param name="compliance">The compliance-audit and dead-letter service.</param>
/// <param name="capabilities">The per-tree capability probe.</param>
/// <param name="access">The keyed access store scoped decisions are filed in.</param>
public sealed class SchemaPluginDomain(
    ICatalogReader catalog,
    ISchemaPolicyService policy,
    ISchemaVersioningService versioning,
    ISchemaComplianceService compliance,
    ISchemaAdminCapabilityService capabilities,
    IExplorerPluginAccessStore access) : ISchemaPluginDomain
{
    /// <summary>
    /// The catalog page size used to enumerate trees. Discovery pages to
    /// completion, so this only trades round trips against page size.
    /// </summary>
    private const int TreePageSize = 200;

    private readonly ICatalogReader _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));
    private readonly ISchemaPolicyService _policy = policy ?? throw new ArgumentNullException(nameof(policy));
    private readonly ISchemaVersioningService _versioning = versioning ?? throw new ArgumentNullException(nameof(versioning));
    private readonly ISchemaComplianceService _compliance = compliance ?? throw new ArgumentNullException(nameof(compliance));
    private readonly ISchemaAdminCapabilityService _capabilities = capabilities ?? throw new ArgumentNullException(nameof(capabilities));
    private readonly IExplorerPluginAccessStore _access = access ?? throw new ArgumentNullException(nameof(access));

    /// <inheritdoc />
    public async Task<SchemaTreeCatalog> ListGovernableTreesAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var trees = new List<SchemaTreeSummary>();
            string? token = null;
            do
            {
                var page = await _catalog.LoadAsync(CatalogKind.Trees, token, TreePageSize, cancellationToken)
                    .ConfigureAwait(false);

                foreach (var item in page.Items)
                {
                    // Restore-shadow trees are an internal restore artifact, never a
                    // governance target; they are surfaced only in the Backups area.
                    if (!item.IsRestoreShadow)
                    {
                        trees.Add(new SchemaTreeSummary(item.Id, item.Label, item.Lifecycle, item.ShardCount));
                    }
                }

                token = page.NextPageToken;
            }
            while (token is not null);

            return SchemaTreeCatalog.Succeeded(trees);
        }
        catch (Exception ex)
        {
            return SchemaTreeCatalog.Failed(ex.Message);
        }
    }

    /// <inheritdoc />
    public async Task<SchemaTreeGrants> ProbeTreeAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        // The probe itself is fail-closed and never throws: a denial or a
        // transport fault yields the all-denied snapshot, which then publishes as
        // eight denied scopes rather than as stale admissions.
        var snapshot = await _capabilities.ProbeTreeAsync(treeId, cancellationToken).ConfigureAwait(false);
        Publish(treeId, snapshot);
        return SchemaTreeGrants.For(_access, treeId);
    }

    /// <inheritdoc />
    public Task<SchemaReadView<LatticeSchemaPolicy>> GetPolicyAsync(string treeId, CancellationToken cancellationToken = default) =>
        _policy.GetPolicyAsync(treeId, cancellationToken);

    /// <inheritdoc />
    public Task<SchemaOperationResult> SetPolicyAsync(string treeId, LatticeSchemaPolicy policy, CancellationToken cancellationToken = default) =>
        _policy.SetPolicyAsync(treeId, policy, cancellationToken);

    /// <inheritdoc />
    public Task<SchemaOperationResult> ClearPolicyAsync(string treeId, CancellationToken cancellationToken = default) =>
        _policy.ClearPolicyAsync(treeId, cancellationToken);

    /// <inheritdoc />
    public Task<SchemaReadView<LatticeSchemaVersionConfig>> GetVersionConfigAsync(string treeId, CancellationToken cancellationToken = default) =>
        _versioning.GetVersionConfigAsync(treeId, cancellationToken);

    /// <inheritdoc />
    public Task<SchemaOperationResult> SetVersionConfigAsync(string treeId, LatticeSchemaVersionConfig config, CancellationToken cancellationToken = default) =>
        _versioning.SetVersionConfigAsync(treeId, config, cancellationToken);

    /// <inheritdoc />
    public Task<SchemaOperationResult> AdvanceTargetVersionAsync(string treeId, uint newTargetVersion, CancellationToken cancellationToken = default) =>
        _versioning.AdvanceTargetVersionAsync(treeId, newTargetVersion, cancellationToken);

    /// <inheritdoc />
    public Task<SchemaOperationResult> AdvanceAndMigrateAsync(string treeId, uint newTargetVersion, CancellationToken cancellationToken = default) =>
        _versioning.AdvanceAndMigrateAsync(treeId, newTargetVersion, cancellationToken);

    /// <inheritdoc />
    public Task<SchemaOperationResult> MigrateToTargetVersionAsync(string treeId, CancellationToken cancellationToken = default) =>
        _versioning.MigrateToTargetVersionAsync(treeId, cancellationToken);

    /// <inheritdoc />
    public Task<SchemaOperationResult> ClearVersionConfigAsync(string treeId, CancellationToken cancellationToken = default) =>
        _versioning.ClearVersionConfigAsync(treeId, cancellationToken);

    /// <inheritdoc />
    public Task<SchemaReadView<LatticeSchemaRemediationReport>> GetRemediationStatusAsync(string treeId, CancellationToken cancellationToken = default) =>
        _versioning.GetRemediationStatusAsync(treeId, cancellationToken);

    /// <inheritdoc />
    public Task<SchemaReadView<LatticeSchemaComplianceReport>> ScanComplianceAsync(string treeId, CancellationToken cancellationToken = default) =>
        _compliance.ScanComplianceAsync(treeId, cancellationToken);

    /// <inheritdoc />
    public Task<SchemaDeadLetterView> ListDeadLettersAsync(string treeId, int maxEntries, CancellationToken cancellationToken = default) =>
        _compliance.ListDeadLettersAsync(treeId, maxEntries, cancellationToken);

    /// <summary>
    /// Files one scoped decision per capability. Writing every capability - not
    /// only the permitted ones - is what makes a re-probe of a tree whose grants
    /// shrank actually revoke the controls it previously opened.
    /// </summary>
    private void Publish(string treeId, SchemaCapabilitySnapshot snapshot)
    {
        Publish(treeId, SchemaCapability.ViewPolicy, snapshot.CanViewPolicy);
        Publish(treeId, SchemaCapability.ManagePolicy, snapshot.CanManagePolicy);
        Publish(treeId, SchemaCapability.ViewVersionConfig, snapshot.CanViewVersionConfig);
        Publish(treeId, SchemaCapability.ManageVersion, snapshot.CanManageVersion);
        Publish(treeId, SchemaCapability.ViewRemediationStatus, snapshot.CanViewRemediationStatus);
        Publish(treeId, SchemaCapability.Remediate, snapshot.CanRemediate);
        Publish(treeId, SchemaCapability.ScanCompliance, snapshot.CanScanCompliance);
        Publish(treeId, SchemaCapability.ViewDeadLetters, snapshot.CanViewDeadLetters);
    }

    private void Publish(string treeId, SchemaCapability capability, bool permitted) =>
        _access.Set(
            SchemaTreeGrants.KeyFor(treeId, capability),
            permitted ? ExplorerPluginAccess.Allowed : ExplorerPluginAccess.Denied);
}
