using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Operations;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Schema;

/// <summary>
/// The accept-then-poll half of the schema facade (#4123): the
/// <see cref="ILatticeSchemaOperations"/> verbs. Every start scopes the tree under
/// the caller's tenant and authorizes schema-management on it exactly as the
/// blocking verb always has, before the work is handed to the shared coordinator.
/// </summary>
internal sealed partial class LatticeSchemaControl
{
    /// <inheritdoc />
    public async Task<LatticeOperationHandle> StartRemediationAsync(
        string treeId,
        LatticeValueTransform transform,
        LatticeSchemaPolicy targetPolicy,
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(targetPolicy);
        var id = LatticeOperationKey.ValidateOrGenerate(operationId);
        var operations = RequireOperations();
        treeId = await _tenantResolver.ResolveEffectiveTreeIdAsync(treeId, cancellationToken).ConfigureAwait(false);
        await _authorizer.AuthorizeManageAsync(treeId, cancellationToken).ConfigureAwait(false);
        var tenantId = await ResolveActiveTenantIdAsync(cancellationToken).ConfigureAwait(false);
        var launch = await operations.StartRemediationAsync(tenantId, id, treeId, transform, targetPolicy)
            .ConfigureAwait(false);
        return LatticeOperationMapping.ToHandle(launch);
    }

    /// <inheritdoc />
    public async Task<LatticeOperationHandle> StartMigrationAsync(
        string treeId,
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        var id = LatticeOperationKey.ValidateOrGenerate(operationId);
        var operations = RequireOperations();
        RequireVersionAdmin();
        treeId = await _tenantResolver.ResolveEffectiveTreeIdAsync(treeId, cancellationToken).ConfigureAwait(false);
        await _authorizer.AuthorizeManageAsync(treeId, cancellationToken).ConfigureAwait(false);
        var tenantId = await ResolveActiveTenantIdAsync(cancellationToken).ConfigureAwait(false);
        var launch = await operations.StartMigrationAsync(tenantId, id, treeId).ConfigureAwait(false);
        return LatticeOperationMapping.ToHandle(launch);
    }

    /// <inheritdoc />
    public async Task<LatticeOperationHandle> StartAdvanceAndMigrateAsync(
        string treeId,
        uint newTargetVersion,
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        var id = LatticeOperationKey.ValidateOrGenerate(operationId);
        var operations = RequireOperations();
        RequireVersionAdmin();
        treeId = await _tenantResolver.ResolveEffectiveTreeIdAsync(treeId, cancellationToken).ConfigureAwait(false);
        await _authorizer.AuthorizeManageAsync(treeId, cancellationToken).ConfigureAwait(false);
        var tenantId = await ResolveActiveTenantIdAsync(cancellationToken).ConfigureAwait(false);
        var launch = await operations.StartAdvanceAndMigrateAsync(tenantId, id, treeId, newTargetVersion)
            .ConfigureAwait(false);
        return LatticeOperationMapping.ToHandle(launch);
    }

    /// <inheritdoc />
    public async Task<LatticeOperationStatus?> GetOperationStatusAsync(
        string operationId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        var operations = RequireOperations();
        var tenantId = await ResolveActiveTenantIdAsync(cancellationToken).ConfigureAwait(false);
        var record = await operations.Runner.GetAsync(tenantId, operationId).ConfigureAwait(false);
        return record is not null && await IsVisibleAsync(record, cancellationToken).ConfigureAwait(false)
            ? LatticeOperationMapping.ToStatus(record)
            : null;
    }

    /// <inheritdoc />
    public async Task<LatticeOperationPage> ListOperationsAsync(
        LatticeOperationListRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        var operations = RequireOperations();
        var tenantId = await ResolveActiveTenantIdAsync(cancellationToken).ConfigureAwait(false);
        var (records, next) = await operations.Runner
            .ListAsync(tenantId, SchemaOperationKinds.Prefix, request.PageToken, request.EffectivePageSize)
            .ConfigureAwait(false);
        var visible = new List<LatticeOperationStatus>(records.Count);
        foreach (var record in records)
        {
            if (await IsVisibleAsync(record, cancellationToken).ConfigureAwait(false))
            {
                visible.Add(LatticeOperationMapping.ToStatus(record));
            }
        }

        return new LatticeOperationPage { Operations = visible, NextPageToken = next };
    }

    /// <inheritdoc />
    public async Task<LatticeOperationStatus?> CancelOperationAsync(
        string operationId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        var operations = RequireOperations();
        var tenantId = await ResolveActiveTenantIdAsync(cancellationToken).ConfigureAwait(false);
        var record = await operations.Runner.GetAsync(tenantId, operationId).ConfigureAwait(false);
        if (record is null || !await IsVisibleAsync(record, cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        // Visible, so its existence is no secret; cancelling needs the grant that
        // starting it needed, and a caller without it is refused outright.
        foreach (var treeId in record.TreeIds)
        {
            await _authorizer.AuthorizeManageAsync(treeId, cancellationToken).ConfigureAwait(false);
        }

        var cancelled = await operations.Runner.RequestCancelAsync(tenantId, operationId).ConfigureAwait(false);
        return cancelled is null ? null : LatticeOperationMapping.ToStatus(cancelled);
    }

    /// <summary>
    /// Whether the caller may see an operation: it must be a schema kind over at
    /// least one tree, and the caller must hold read authority over every tree it
    /// records. Anything else is reported as not found, so an operation's existence
    /// is never disclosed.
    /// </summary>
    private async ValueTask<bool> IsVisibleAsync(LatticeOperationRecord record, CancellationToken cancellationToken)
    {
        if (!record.Kind.StartsWith(SchemaOperationKinds.Prefix, StringComparison.Ordinal) || record.TreeIds.Count == 0)
        {
            return false;
        }

        foreach (var treeId in record.TreeIds)
        {
            if (!await _authorizer.IsReadAuthorizedAsync(treeId, cancellationToken).ConfigureAwait(false))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Resolves the tenant the call acts as: the caller's validated active-tenant
    /// assertion, or the reserved default tenant when there is none. An assertion
    /// the caller may not make is refused, never defaulted.
    /// </summary>
    private async ValueTask<string> ResolveActiveTenantIdAsync(CancellationToken cancellationToken)
    {
        var tenant = _tenantResolver.TryResolveCurrent(out var warm)
            ? warm
            : await _tenantResolver.ResolveCurrentAsync(cancellationToken).ConfigureAwait(false);
        return tenant.Value ?? throw new LatticeTenantAccessDeniedException();
    }

    private SchemaOperationService RequireOperations() =>
        _operations ?? throw new InvalidOperationException(
            "Schema operations are not available on this silo; call AddLatticeSchemaEnforcement() to register them.");
}
