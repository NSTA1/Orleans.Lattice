namespace Orleans.Lattice.Explorer.Plugins.Tenancy;

/// <summary>
/// The default <see cref="ITenantAdminService"/> over an
/// <see cref="ITenantAdminClient"/>. Every operation projects the control API's
/// reply onto the Explorer's domain model, and folds every fault the seam owns
/// into a classified <see cref="TenantOperationResult"/>, so a tenancy panel
/// never sees an exception and never sees a wire type.
/// </summary>
/// <remarks>
/// Each method catches through <see cref="TenantFaultMapper.IsFault"/>, which
/// declines a caller-requested cancellation (so it propagates) and anything
/// outside the known fault families (so a genuine defect is not disguised as a
/// server refusal). The filter runs inline, so the success path allocates
/// nothing beyond the result and its projection.
/// </remarks>
/// <param name="client">The transport seam onto the tenant-administration facades.</param>
public sealed class TenantAdminService(ITenantAdminClient client) : ITenantAdminService
{
    private readonly ITenantAdminClient _client = client ?? throw new ArgumentNullException(nameof(client));

    /// <inheritdoc />
    public async Task<TenantOperationResult<ExplorerTenantSummary>> GetCurrentTenantAsync(
        CancellationToken cancellationToken = default)
    {
        try
        {
            var descriptor = await _client.GetCurrentTenantAsync(cancellationToken).ConfigureAwait(false);
            return TenantOperationResult<ExplorerTenantSummary>.Success(
                TenantProjection.ToSummary(descriptor),
                "Resolved the current tenant.");
        }
        catch (Exception ex) when (TenantFaultMapper.IsFault(ex, cancellationToken))
        {
            return TenantFaultMapper.Fail<ExplorerTenantSummary>(ex);
        }
    }

    /// <inheritdoc />
    public async Task<TenantOperationResult<IReadOnlyList<ExplorerTenantSummary>>> ListAccessibleTenantsAsync(
        CancellationToken cancellationToken = default)
    {
        try
        {
            var tenants = await _client.ListAccessibleTenantsAsync(cancellationToken).ConfigureAwait(false);
            return TenantOperationResult<IReadOnlyList<ExplorerTenantSummary>>.Success(
                TenantProjection.ToSummaries(tenants),
                "Listed the accessible tenants.");
        }
        catch (Exception ex) when (TenantFaultMapper.IsFault(ex, cancellationToken))
        {
            return TenantFaultMapper.Fail<IReadOnlyList<ExplorerTenantSummary>>(ex);
        }
    }

    /// <inheritdoc />
    public async Task<TenantOperationResult<ExplorerTenantDetail>> GetTenantAsync(
        string tenantId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        try
        {
            var report = await _client.GetTenantAsync(tenantId, cancellationToken).ConfigureAwait(false);
            return TenantOperationResult<ExplorerTenantDetail>.Success(
                TenantProjection.ToDetail(report),
                "Read the tenant.");
        }
        catch (Exception ex) when (TenantFaultMapper.IsFault(ex, cancellationToken))
        {
            return TenantFaultMapper.Fail<ExplorerTenantDetail>(ex);
        }
    }

    /// <inheritdoc />
    public async Task<TenantOperationResult<ExplorerTenantCreation>> CreateTenantAsync(
        string tenantId,
        IReadOnlyCollection<string>? adminSubjects = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        try
        {
            var result = await _client
                .CreateTenantAsync(tenantId, adminSubjects, cancellationToken)
                .ConfigureAwait(false);
            return TenantOperationResult<ExplorerTenantCreation>.Success(
                TenantProjection.ToCreation(result),
                $"Created tenant '{tenantId}'.");
        }
        catch (Exception ex) when (TenantFaultMapper.IsFault(ex, cancellationToken))
        {
            return TenantFaultMapper.Fail<ExplorerTenantCreation>(ex);
        }
    }

    /// <inheritdoc />
    public async Task<TenantOperationResult<ExplorerTenantStatusChange>> SuspendTenantAsync(
        string tenantId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        try
        {
            var result = await _client.SuspendTenantAsync(tenantId, cancellationToken).ConfigureAwait(false);
            return TenantOperationResult<ExplorerTenantStatusChange>.Success(
                TenantProjection.ToStatusChange(result),
                $"Suspended tenant '{tenantId}'.");
        }
        catch (Exception ex) when (TenantFaultMapper.IsFault(ex, cancellationToken))
        {
            return TenantFaultMapper.Fail<ExplorerTenantStatusChange>(ex);
        }
    }

    /// <inheritdoc />
    public async Task<TenantOperationResult<ExplorerTenantStatusChange>> ResumeTenantAsync(
        string tenantId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        try
        {
            var result = await _client.ResumeTenantAsync(tenantId, cancellationToken).ConfigureAwait(false);
            return TenantOperationResult<ExplorerTenantStatusChange>.Success(
                TenantProjection.ToStatusChange(result),
                $"Resumed tenant '{tenantId}'.");
        }
        catch (Exception ex) when (TenantFaultMapper.IsFault(ex, cancellationToken))
        {
            return TenantFaultMapper.Fail<ExplorerTenantStatusChange>(ex);
        }
    }

    /// <inheritdoc />
    public async Task<TenantOperationResult<ExplorerTenantDeletion>> DeleteTenantAsync(
        string tenantId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        try
        {
            var result = await _client.DeleteTenantAsync(tenantId, cancellationToken).ConfigureAwait(false);
            return TenantOperationResult<ExplorerTenantDeletion>.Success(
                TenantProjection.ToDeletion(result),
                $"Deleted tenant '{tenantId}'.");
        }
        catch (Exception ex) when (TenantFaultMapper.IsFault(ex, cancellationToken))
        {
            return TenantFaultMapper.Fail<ExplorerTenantDeletion>(ex);
        }
    }

    /// <inheritdoc />
    public async Task<TenantOperationResult<ExplorerTenantQuotaLimits>> SetQuotasAsync(
        string tenantId,
        ExplorerTenantQuotaLimits limits,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        try
        {
            var result = await _client
                .SetTenantQuotasAsync(tenantId, TenantProjection.ToDescriptor(limits), cancellationToken)
                .ConfigureAwait(false);
            return TenantOperationResult<ExplorerTenantQuotaLimits>.Success(
                TenantProjection.ToLimits(result.Quotas),
                $"Updated the quotas of tenant '{tenantId}'.");
        }
        catch (Exception ex) when (TenantFaultMapper.IsFault(ex, cancellationToken))
        {
            return TenantFaultMapper.Fail<ExplorerTenantQuotaLimits>(ex);
        }
    }

    /// <inheritdoc />
    public async Task<TenantOperationResult<ExplorerTenantQuotaUsage>> GetQuotaUsageAsync(
        string tenantId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        try
        {
            var report = await _client.GetTenantQuotaUsageAsync(tenantId, cancellationToken).ConfigureAwait(false);
            return TenantOperationResult<ExplorerTenantQuotaUsage>.Success(
                TenantProjection.ToUsage(report),
                "Read usage against quota.");
        }
        catch (Exception ex) when (TenantFaultMapper.IsFault(ex, cancellationToken))
        {
            return TenantFaultMapper.Fail<ExplorerTenantQuotaUsage>(ex);
        }
    }

    /// <inheritdoc />
    public async Task<TenantOperationResult<IReadOnlyList<string>>> AuthorizeAllowedRegionsAsync(
        string tenantId,
        IReadOnlyCollection<string> allowedRegions,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentNullException.ThrowIfNull(allowedRegions);
        try
        {
            var result = await _client
                .AuthorizeAllowedRegionsAsync(tenantId, allowedRegions, cancellationToken)
                .ConfigureAwait(false);
            return TenantOperationResult<IReadOnlyList<string>>.Success(
                result.AllowedRegions,
                $"Authorized the allowed regions of tenant '{tenantId}'.");
        }
        catch (Exception ex) when (TenantFaultMapper.IsFault(ex, cancellationToken))
        {
            return TenantFaultMapper.Fail<IReadOnlyList<string>>(ex);
        }
    }

    /// <inheritdoc />
    public async Task<TenantOperationResult<ExplorerTenantResidencyChange>> SetResidencyAsync(
        string tenantId,
        IReadOnlyCollection<string> residencyRegions,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentNullException.ThrowIfNull(residencyRegions);
        try
        {
            var result = await _client
                .SetTenantResidencyAsync(tenantId, residencyRegions, cancellationToken)
                .ConfigureAwait(false);
            return TenantOperationResult<ExplorerTenantResidencyChange>.Success(
                TenantProjection.ToResidencyChange(result),
                $"Updated the residency of tenant '{tenantId}'.");
        }
        catch (Exception ex) when (TenantFaultMapper.IsFault(ex, cancellationToken))
        {
            return TenantFaultMapper.Fail<ExplorerTenantResidencyChange>(ex);
        }
    }

    /// <inheritdoc />
    public async Task<TenantOperationResult<IReadOnlyList<ExplorerTenantRegion>>> GetRegionStatusAsync(
        string tenantId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        try
        {
            var report = await _client.GetTenantRegionStatusAsync(tenantId, cancellationToken).ConfigureAwait(false);
            return TenantOperationResult<IReadOnlyList<ExplorerTenantRegion>>.Success(
                TenantProjection.ToRegions(report.Regions),
                "Read the tenant's per-region status.");
        }
        catch (Exception ex) when (TenantFaultMapper.IsFault(ex, cancellationToken))
        {
            return TenantFaultMapper.Fail<IReadOnlyList<ExplorerTenantRegion>>(ex);
        }
    }

    /// <inheritdoc />
    public async Task<TenantOperationResult<ExplorerTenantAdmins>> ListAdminSubjectsAsync(
        string tenantId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        try
        {
            var report = await _client.ListTenantAdminSubjectsAsync(tenantId, cancellationToken).ConfigureAwait(false);
            return TenantOperationResult<ExplorerTenantAdmins>.Success(
                TenantProjection.ToAdmins(report),
                "Listed the tenant's admin subjects.");
        }
        catch (Exception ex) when (TenantFaultMapper.IsFault(ex, cancellationToken))
        {
            return TenantFaultMapper.Fail<ExplorerTenantAdmins>(ex);
        }
    }

    /// <inheritdoc />
    public async Task<TenantOperationResult<ExplorerTenantAdminChange>> AddAdminSubjectAsync(
        string tenantId,
        string subjectId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentException.ThrowIfNullOrEmpty(subjectId);
        try
        {
            var result = await _client
                .AddTenantAdminSubjectAsync(tenantId, subjectId, cancellationToken)
                .ConfigureAwait(false);
            return TenantOperationResult<ExplorerTenantAdminChange>.Success(
                TenantProjection.ToAdminChange(result),
                $"Granted '{subjectId}' admin authority over tenant '{tenantId}'.");
        }
        catch (Exception ex) when (TenantFaultMapper.IsFault(ex, cancellationToken))
        {
            return TenantFaultMapper.Fail<ExplorerTenantAdminChange>(ex);
        }
    }

    /// <inheritdoc />
    public async Task<TenantOperationResult<ExplorerTenantAdminChange>> RemoveAdminSubjectAsync(
        string tenantId,
        string subjectId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentException.ThrowIfNullOrEmpty(subjectId);
        try
        {
            var result = await _client
                .RemoveTenantAdminSubjectAsync(tenantId, subjectId, cancellationToken)
                .ConfigureAwait(false);
            return TenantOperationResult<ExplorerTenantAdminChange>.Success(
                TenantProjection.ToAdminChange(result),
                $"Revoked '{subjectId}' admin authority over tenant '{tenantId}'.");
        }
        catch (Exception ex) when (TenantFaultMapper.IsFault(ex, cancellationToken))
        {
            return TenantFaultMapper.Fail<ExplorerTenantAdminChange>(ex);
        }
    }

    /// <inheritdoc />
    public async Task<TenantOperationResult<ExplorerTenantGrants>> ListGrantsAsync(
        string tenantId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        try
        {
            var report = await _client.ListCrossTenantGrantsAsync(tenantId, cancellationToken).ConfigureAwait(false);
            return TenantOperationResult<ExplorerTenantGrants>.Success(
                TenantProjection.ToGrants(report),
                "Listed the tenant's cross-tenant grants.");
        }
        catch (Exception ex) when (TenantFaultMapper.IsFault(ex, cancellationToken))
        {
            return TenantFaultMapper.Fail<ExplorerTenantGrants>(ex);
        }
    }

    /// <inheritdoc />
    public async Task<TenantOperationResult<ExplorerTenantGrantChange>> OfferGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        ExplorerTenantGrantAccess operations,
        CancellationToken cancellationToken = default)
    {
        ValidateGrantKey(granterTenantId, granteeTenantId, scope);
        try
        {
            var result = await _client
                .OfferCrossTenantGrantAsync(
                    granterTenantId,
                    granteeTenantId,
                    scope,
                    TenantProjection.ToWireGrantAccess(operations),
                    cancellationToken)
                .ConfigureAwait(false);
            return TenantOperationResult<ExplorerTenantGrantChange>.Success(
                TenantProjection.ToGrantChange(result),
                $"Offered a grant from tenant '{granterTenantId}' to tenant '{granteeTenantId}'. "
                + "It authorizes nothing until the grantee approves it.");
        }
        catch (Exception ex) when (TenantFaultMapper.IsFault(ex, cancellationToken))
        {
            return TenantFaultMapper.Fail<ExplorerTenantGrantChange>(ex);
        }
    }

    /// <inheritdoc />
    public async Task<TenantOperationResult<ExplorerTenantGrantChange>> ApproveGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        CancellationToken cancellationToken = default)
    {
        ValidateGrantKey(granterTenantId, granteeTenantId, scope);
        try
        {
            var result = await _client
                .ApproveCrossTenantGrantAsync(granterTenantId, granteeTenantId, scope, cancellationToken)
                .ConfigureAwait(false);
            return TenantOperationResult<ExplorerTenantGrantChange>.Success(
                TenantProjection.ToGrantChange(result),
                $"Approved the grant from tenant '{granterTenantId}' to tenant '{granteeTenantId}'.");
        }
        catch (Exception ex) when (TenantFaultMapper.IsFault(ex, cancellationToken))
        {
            return TenantFaultMapper.Fail<ExplorerTenantGrantChange>(ex);
        }
    }

    /// <inheritdoc />
    public async Task<TenantOperationResult<ExplorerTenantGrantChange>> RejectGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        CancellationToken cancellationToken = default)
    {
        ValidateGrantKey(granterTenantId, granteeTenantId, scope);
        try
        {
            var result = await _client
                .RejectCrossTenantGrantAsync(granterTenantId, granteeTenantId, scope, cancellationToken)
                .ConfigureAwait(false);
            return TenantOperationResult<ExplorerTenantGrantChange>.Success(
                TenantProjection.ToGrantChange(result),
                $"Rejected the grant from tenant '{granterTenantId}' to tenant '{granteeTenantId}'.");
        }
        catch (Exception ex) when (TenantFaultMapper.IsFault(ex, cancellationToken))
        {
            return TenantFaultMapper.Fail<ExplorerTenantGrantChange>(ex);
        }
    }

    /// <inheritdoc />
    public async Task<TenantOperationResult<ExplorerTenantGrantChange>> RevokeGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        CancellationToken cancellationToken = default)
    {
        ValidateGrantKey(granterTenantId, granteeTenantId, scope);
        try
        {
            var result = await _client
                .RevokeCrossTenantGrantAsync(granterTenantId, granteeTenantId, scope, cancellationToken)
                .ConfigureAwait(false);
            return TenantOperationResult<ExplorerTenantGrantChange>.Success(
                TenantProjection.ToGrantChange(result),
                $"Revoked the grant from tenant '{granterTenantId}' to tenant '{granteeTenantId}'.");
        }
        catch (Exception ex) when (TenantFaultMapper.IsFault(ex, cancellationToken))
        {
            return TenantFaultMapper.Fail<ExplorerTenantGrantChange>(ex);
        }
    }

    private static void ValidateGrantKey(string granterTenantId, string granteeTenantId, string scope)
    {
        ArgumentException.ThrowIfNullOrEmpty(granterTenantId);
        ArgumentException.ThrowIfNullOrEmpty(granteeTenantId);
        ArgumentException.ThrowIfNullOrEmpty(scope);
    }
}
