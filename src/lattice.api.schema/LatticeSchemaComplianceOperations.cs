using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Operations;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Schema;

/// <summary>
/// Default <see cref="ILatticeSchemaComplianceOperations"/> (#4126): runs a schema
/// compliance scan on the shared <see cref="LatticeOperationRunner"/>. A start
/// composes the caller's tree name under the active tenant and authorizes read
/// over that effective tree through the same <see cref="SchemaAccessAuthorizer"/>
/// the blocking scan uses, so the authorized tree and the scanned tree cannot
/// diverge. Status, list and cancel are scoped fail-closed to the
/// compliance-scan kind, the caller's tenant and the trees the caller may read.
/// </summary>
internal sealed class LatticeSchemaComplianceOperations : ILatticeSchemaComplianceOperations
{
    private static readonly IReadOnlyList<string> Phases =
        [SchemaComplianceScanOperation.CountingPhase, SchemaComplianceScanOperation.ScanningPhase];

    private readonly LatticeOperationRunner _runner;
    private readonly ILatticeSchemaComplianceAdmin _compliance;
    private readonly SchemaAccessAuthorizer _authorizer;
    private readonly ITenantContextResolver _tenantResolver;

    /// <summary>Initializes a new <see cref="LatticeSchemaComplianceOperations"/>.</summary>
    /// <param name="runner">The shared operation coordinator. Must not be <c>null</c>.</param>
    /// <param name="compliance">The compliance-scan engine. Must not be <c>null</c>.</param>
    /// <param name="authorizer">The fail-closed schema authorization seam. Must not be <c>null</c>.</param>
    /// <param name="tenantResolver">The active-tenant context resolver. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">A dependency is <c>null</c>.</exception>
    public LatticeSchemaComplianceOperations(
        LatticeOperationRunner runner,
        ILatticeSchemaComplianceAdmin compliance,
        SchemaAccessAuthorizer authorizer,
        ITenantContextResolver tenantResolver)
    {
        ArgumentNullException.ThrowIfNull(runner);
        ArgumentNullException.ThrowIfNull(compliance);
        ArgumentNullException.ThrowIfNull(authorizer);
        ArgumentNullException.ThrowIfNull(tenantResolver);
        _runner = runner;
        _compliance = compliance;
        _authorizer = authorizer;
        _tenantResolver = tenantResolver;
    }

    /// <inheritdoc />
    public async Task<LatticeOperationHandle> StartComplianceScanAsync(
        string treeId,
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        var id = ResolveOperationId(operationId);

        // Composed once under the active tenant, then used for the gate, the
        // record and the scan alike.
        treeId = await _tenantResolver.ResolveEffectiveTreeIdAsync(treeId, cancellationToken).ConfigureAwait(false);
        await _authorizer.AuthorizeReadAsync(treeId, cancellationToken).ConfigureAwait(false);

        var tenant = await ResolveActiveTenantAsync(cancellationToken).ConfigureAwait(false);
        var scanned = treeId;
        var launch = await _runner.StartAsync(
            new LatticeOperationStart
            {
                TenantId = tenant,
                OperationId = id,
                Kind = SchemaComplianceScanOperation.Kind,
                TreeIds = [scanned],
                Phases = Phases,
            },
            (_, ct) => _compliance.ScanComplianceAsync(scanned, ct),
            static report => LatticeOperationCompletion.Succeeded(
                report.TreeId, SchemaComplianceScanResults.ToResultMap(report))).ConfigureAwait(false);

        return LatticeOperationMapping.ToHandle(launch.Record, created: launch.Completion is not null);
    }

    /// <inheritdoc />
    public async Task<LatticeOperationStatus?> GetOperationStatusAsync(
        string operationId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);

        var tenant = await ResolveActiveTenantAsync(cancellationToken).ConfigureAwait(false);
        var record = await _runner.GetAsync(tenant, operationId).ConfigureAwait(false);
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

        var tenant = await ResolveActiveTenantAsync(cancellationToken).ConfigureAwait(false);

        // The exact kind is the listing prefix, so a sibling schema operation kind
        // (remediation, say) is never listed here.
        var (records, next) = await _runner
            .ListAsync(tenant, SchemaComplianceScanOperation.Kind, request.PageToken, request.EffectivePageSize)
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

        var tenant = await ResolveActiveTenantAsync(cancellationToken).ConfigureAwait(false);
        var record = await _runner.GetAsync(tenant, operationId).ConfigureAwait(false);
        if (record is null || !await IsVisibleAsync(record, cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        // Starting a scan needed read over its tree; so does cancelling it.
        await _authorizer.AuthorizeReadAsync(record.TreeIds[0], cancellationToken).ConfigureAwait(false);

        var cancelled = await _runner.RequestCancelAsync(tenant, operationId).ConfigureAwait(false);
        return cancelled is null ? null : LatticeOperationMapping.ToStatus(cancelled);
    }

    /// <summary>
    /// Whether the caller may see an operation: it must be a compliance scan of
    /// exactly one tree the caller may read. Anything else is reported as not
    /// found, so an operation's existence is never disclosed.
    /// </summary>
    private async ValueTask<bool> IsVisibleAsync(LatticeOperationRecord record, CancellationToken cancellationToken) =>
        string.Equals(record.Kind, SchemaComplianceScanOperation.Kind, StringComparison.Ordinal)
        && record.TreeIds.Count == 1
        && !string.IsNullOrEmpty(record.TreeIds[0])
        && await _authorizer.IsReadAuthorizedAsync(record.TreeIds[0], cancellationToken).ConfigureAwait(false);

    private async ValueTask<string> ResolveActiveTenantAsync(CancellationToken cancellationToken)
    {
        var tenant = _tenantResolver.TryResolveCurrent(out var warm)
            ? warm
            : await _tenantResolver.ResolveCurrentAsync(cancellationToken).ConfigureAwait(false);
        return tenant.Value ?? throw new LatticeTenantAccessDeniedException();
    }

    private static string ResolveOperationId(string? operationId)
    {
        if (operationId is null)
        {
            return LatticeOperationKey.NewId();
        }

        LatticeOperationKey.ThrowIfInvalid(operationId, nameof(operationId));
        return operationId;
    }
}
