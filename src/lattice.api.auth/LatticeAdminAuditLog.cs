using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Api.Auth;

/// <summary>
/// Default <see cref="ILatticeAdminAuditLog"/> implementation. Registered as a silo
/// singleton by <c>AddLatticeAuthApi</c>.
/// </summary>
/// <remarks>
/// Every call is gated exactly like an <see cref="ILatticeAuthAdmin"/> call: an
/// <see cref="LatticeOperation.Admin"/> verdict on the reserved auth policy tree through
/// <see cref="LatticeAccessGateEnforcement.EnforceWholeTreeAsync"/>. A refusal is recorded
/// to the admin audit trail as a denied <see cref="LatticeAdminAuditAction.Read"/> before it
/// is rethrown; only then is the reserved audit tree read, under the system origin.
/// </remarks>
internal sealed class LatticeAdminAuditLog(
    ILatticeAccessGate gate,
    ILatticeMembershipContext membership,
    IGrainFactory grainFactory,
    ILatticeAdminAuditTrail? auditTrail = null) : ILatticeAdminAuditLog
{
    private static readonly LatticeAdminAuditRequest ReadRequest =
        new(LatticeAdminAuditAction.Read, LatticeAdminAuditRequest.ClusterScope, LatticeAdminAuditTargetKind.None, null);

    private readonly ILatticeAccessGate _gate = gate ?? throw new ArgumentNullException(nameof(gate));
    private readonly ILatticeMembershipContext _membership = membership ?? throw new ArgumentNullException(nameof(membership));
    private readonly IGrainFactory _grainFactory = grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));

    /// <inheritdoc />
    public async Task<AdminAuditPage> ListAuditAsync(AdminAuditQuery query, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);

        await LatticeAdminAudit.AuthorizeAsync(
            auditTrail,
            ReadRequest,
            ct => LatticeAccessGateEnforcement.EnforceWholeTreeAsync(
                _gate, _membership, LatticeAuthReservedTrees.PolicyTreeId, LatticeOperation.Admin, ct),
            cancellationToken).ConfigureAwait(false);

        var (records, next) = await AdminAuditReader.ReadPageAsync(
            _grainFactory,
            query.Scope,
            includePending: true,
            query.From,
            query.To,
            record => Matches(record, query),
            query.PageToken,
            query.EffectivePageSize,
            cancellationToken).ConfigureAwait(false);

        return new AdminAuditPage { Records = records, NextPageToken = next };
    }

    /// <summary>Applies the actor, action and outcome filters of <paramref name="query"/>.</summary>
    /// <param name="record">The candidate record.</param>
    /// <param name="query">The query.</param>
    internal static bool Matches(LatticeAdminAuditRecord record, AdminAuditQuery query) =>
        (query.Actor is null || string.Equals(record.ActorSubjectId, query.Actor, StringComparison.Ordinal))
        && (query.Action is null || record.Action == query.Action)
        && (query.Outcome is null || record.Outcome == query.Outcome);
}
