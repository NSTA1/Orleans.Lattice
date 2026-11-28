using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Default <see cref="ILatticeTenantAuditLog"/> implementation. Registered as a silo
/// singleton by <c>AddLatticeTenantAdminApi</c>.
/// </summary>
/// <remarks>
/// The caller is authorized through
/// <see cref="TenantRegionResidencyAuthorizer.AuthorizeTenantAdminAsync(TenantId, string, CancellationToken)"/>
/// (a platform operator, or a live admin subject of the active tenant), with a refusal
/// recorded to the admin audit trail as a denied <see cref="LatticeAdminAuditAction.Read"/>.
/// The read is then fixed to the validated tenant's own <c>t/{tenant}</c> partition, whatever
/// the query's <see cref="AdminAuditQuery.Scope"/> says, and returns only final records:
/// in-flight intents are never shown to a tenant administrator.
/// </remarks>
internal sealed class LatticeTenantAuditLog(
    TenantRegionResidencyAuthorizer authorizer,
    IGrainFactory grainFactory,
    ILatticeAdminAuditTrail? auditTrail = null) : ILatticeTenantAuditLog
{
    private const string AuditLogAction = "audit log";

    private readonly TenantRegionResidencyAuthorizer _authorizer = authorizer ?? throw new ArgumentNullException(nameof(authorizer));
    private readonly IGrainFactory _grainFactory = grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));

    /// <inheritdoc />
    public async Task<AdminAuditPage> ListAuditAsync(
        string tenantId, AdminAuditQuery query, CancellationToken cancellationToken = default)
    {
        var tenant = TenantAdminArguments.ParseTenantId(tenantId);
        ArgumentNullException.ThrowIfNull(query);

        await LatticeAdminAudit
            .AuthorizeAsync(
                auditTrail,
                TenantAdminAudit.Read(tenant),
                ct => _authorizer.AuthorizeTenantAdminAsync(tenant, AuditLogAction, ct),
                cancellationToken)
            .ConfigureAwait(false);

        var (records, next) = await AdminAuditReader.ReadPageAsync(
            _grainFactory,
            LatticeAdminAuditRequest.TenantScope(tenant.Value!),
            includePending: false,
            query.From,
            query.To,
            record => record.Outcome != LatticeAdminAuditOutcome.Pending
                && (query.Actor is null || string.Equals(record.ActorSubjectId, query.Actor, StringComparison.Ordinal))
                && (query.Action is null || record.Action == query.Action)
                && (query.Outcome is null || record.Outcome == query.Outcome),
            query.PageToken,
            query.EffectivePageSize,
            cancellationToken).ConfigureAwait(false);

        return new AdminAuditPage { Records = records, NextPageToken = next };
    }
}
