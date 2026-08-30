using Orleans.Lattice;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The fail-closed authorization seam for the <b>tenant-tier</b> control facades -
/// per-tenant region residency and tenant access (admin-subject) administration.
/// It enforces the spec's two authorization tiers using only the
/// integrated access-gate and tenant-registry primitives, and is <b>independent of
/// the data-plane default effect</b>: an unmatched request always resolves to
/// deny, even under <c>LatticeAuthOptions.DefaultEffect = Allow</c>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Operator tier</b> (authorize the allowed region set). A platform operator is
/// a caller granted cluster-wide <see cref="LatticeOperation.Admin"/> on the
/// reserved auth policy tree (<see cref="LatticeAuthReservedTrees.PolicyTreeId"/>).
/// The core auth gate routes that reserved control-plane tree through its
/// control-plane-isolated path, which never inherits the data-plane default
/// effect, so an unmatched caller is denied even when the data plane defaults to
/// allow. This is the same operator test <c>TenantObservabilityView</c> uses.
/// </para>
/// <para>
/// <b>Tenant-admin tier</b> (set residency within allowed, read status). Granted
/// when the caller is that platform operator <b>or</b> a live admin subject on the
/// tenant record (<see cref="TenantRecord.HasAdminSubject"/>). Admin-subject
/// membership is a CRDT set on the record, evaluated directly, so it too is
/// inherently independent of the data-plane default effect. This deliberately does
/// <b>not</b> reuse the cluster-wide <c>"*"</c> Admin authorizer, which can
/// fail-open under <c>DefaultEffect = Allow</c>.
/// </para>
/// <para>
/// The caller subject is resolved through the membership seam (anonymous when no
/// membership context is registered), and <see cref="LatticeSystemOrigin"/> bypass
/// is honoured for trusted co-hosted infrastructure, matching the sibling
/// tenant-admin authorizer.
/// </para>
/// </remarks>
public sealed class TenantRegionResidencyAuthorizer
{
    private readonly ILatticeAccessGate _gate;
    private readonly ITenantRegistry _registry;
    private readonly ILatticeMembershipContext? _membership;

    /// <summary>
    /// Initializes a new <see cref="TenantRegionResidencyAuthorizer"/>.
    /// </summary>
    /// <param name="gate">The registered core access gate used for the operator-tier check. Must not be <c>null</c>.</param>
    /// <param name="registry">The tenancy registry read to resolve the tenant record for the tenant-admin membership check. Must not be <c>null</c>.</param>
    /// <param name="membership">The membership context used to resolve the caller subject, or <c>null</c> when none is registered (every caller then resolves to <see cref="LatticeSubject.Anonymous"/>).</param>
    /// <exception cref="ArgumentNullException"><paramref name="gate"/> or <paramref name="registry"/> is <c>null</c>.</exception>
    public TenantRegionResidencyAuthorizer(
        ILatticeAccessGate gate, ITenantRegistry registry, ILatticeMembershipContext? membership = null)
    {
        ArgumentNullException.ThrowIfNull(gate);
        ArgumentNullException.ThrowIfNull(registry);
        _gate = gate;
        _registry = registry;
        _membership = membership;
    }

    /// <summary>
    /// Authorizes an <b>operator-tier</b> action (authorizing a tenant's allowed
    /// region set), throwing <see cref="LatticeAuthorizationDeniedException"/> when
    /// the caller is not a platform operator. Fail-closed and independent of the
    /// data-plane default effect.
    /// </summary>
    /// <param name="cancellationToken">Cancels the authorization.</param>
    /// <returns>A task that completes when the caller is authorized as a platform operator.</returns>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not a platform operator.</exception>
    public async ValueTask AuthorizeOperatorAsync(CancellationToken cancellationToken = default)
    {
        if (LatticeSystemOrigin.IsActive)
        {
            return;
        }

        var subject = await ResolveSubjectAsync(cancellationToken).ConfigureAwait(false);
        if (!await IsPlatformOperatorAsync(subject, cancellationToken).ConfigureAwait(false))
        {
            throw new LatticeAuthorizationDeniedException(
                LatticeAuthReservedTrees.PolicyTreeId,
                LatticeOperation.Admin,
                subject.SubjectId,
                "Authorizing a tenant's allowed region set requires platform-operator authority.");
        }
    }

    /// <summary>
    /// Authorizes a <b>tenant-admin-tier</b> action (setting residency within the
    /// allowed set, or reading per-region status) for <paramref name="tenant"/> and
    /// returns the tenant's current record so the facade reuses this single read.
    /// The caller is authorized when it is a platform operator or a live admin
    /// subject on the tenant record. Fail-closed and independent of the data-plane
    /// default effect.
    /// </summary>
    /// <param name="tenant">The tenant the action targets.</param>
    /// <param name="cancellationToken">Cancels the authorization.</param>
    /// <returns>The authorized tenant's current record.</returns>
    /// <exception cref="TenantNotFoundException">The caller is a platform operator but no tenant with that id is registered.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is neither a platform operator nor a tenant admin (a non-existent tenant is reported as a denial, never a not-found, to a non-operator caller).</exception>
    public ValueTask<TenantRecord> AuthorizeTenantAdminAsync(
        TenantId tenant, CancellationToken cancellationToken = default) =>
        AuthorizeTenantAdminAsync(tenant, RegionResidencyAction, cancellationToken);

    /// <summary>
    /// The default action description used by the region-residency overload, so the
    /// denial message names the surface the caller was refused on.
    /// </summary>
    private const string RegionResidencyAction = "region residency";

    /// <summary>
    /// Authorizes a <b>tenant-admin-tier</b> action on <paramref name="tenant"/>,
    /// naming <paramref name="action"/> in the denial message so each tenant-tier
    /// surface reports the authority it actually required, and returns the tenant's
    /// current record so the facade reuses this single read. Identical in every
    /// other respect to
    /// <see cref="AuthorizeTenantAdminAsync(TenantId, CancellationToken)"/>: the
    /// caller is authorized when it is a platform operator or a live admin subject
    /// on the tenant record, fail-closed and independent of the data-plane default
    /// effect.
    /// </summary>
    /// <param name="tenant">The tenant the action targets.</param>
    /// <param name="action">
    /// A short description of the tenant-scoped surface being administered (for
    /// example <c>"region residency"</c> or <c>"admin subjects"</c>), interpolated
    /// into the denial message. Must not be <c>null</c>.
    /// </param>
    /// <param name="cancellationToken">Cancels the authorization.</param>
    /// <returns>The authorized tenant's current record.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="action"/> is <c>null</c>.</exception>
    /// <exception cref="TenantNotFoundException">The caller is a platform operator but no tenant with that id is registered.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is neither a platform operator nor a tenant admin (a non-existent tenant is reported as a denial, never a not-found, to a non-operator caller).</exception>
    public async ValueTask<TenantRecord> AuthorizeTenantAdminAsync(
        TenantId tenant, string action, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(action);

        if (LatticeSystemOrigin.IsActive)
        {
            return await LoadOrThrowAsync(tenant, cancellationToken).ConfigureAwait(false);
        }

        var subject = await ResolveSubjectAsync(cancellationToken).ConfigureAwait(false);
        var isOperator = await IsPlatformOperatorAsync(subject, cancellationToken).ConfigureAwait(false);

        var record = await _registry.GetAsync(tenant, cancellationToken).ConfigureAwait(false);

        if (isOperator)
        {
            // The operator is trusted to learn whether the tenant exists.
            return record ?? throw new TenantNotFoundException(tenant.Value);
        }

        // A non-operator is authorized only as a live admin subject on an existing
        // record. A missing record is reported as a denial (never a not-found), so
        // a non-admin caller cannot probe tenant existence.
        if (record is not null && !subject.IsAnonymous && record.HasAdminSubject(subject.SubjectId))
        {
            return record;
        }

        throw new LatticeAuthorizationDeniedException(
            tenant.Value,
            LatticeOperation.Admin,
            subject.SubjectId,
            $"Administering a tenant's {action} requires platform-operator or tenant-admin authority.");
    }

    /// <summary>
    /// The non-throwing counterpart of
    /// <see cref="AuthorizeTenantAdminAsync(TenantId, string, CancellationToken)"/>:
    /// returns <paramref name="tenant"/>'s record when the caller holds
    /// tenant-admin-tier authority over it (platform operator, or a live admin
    /// subject on the record), and <see langword="null"/> when it does not - which
    /// also covers a tenant that is not registered, so it can never be used to
    /// distinguish the two.
    /// </summary>
    /// <remarks>
    /// <b>A <see langword="null"/> result means DENIED.</b> It exists only for an
    /// operation that admits <em>either</em> of two tenants' admins (revoking a
    /// cross-tenant grant, which either party may do), where probing one side must
    /// not throw before the other side has been considered. A caller must either
    /// authorize through another tenant or refuse; treating
    /// <see langword="null"/> as anything but a denial would open the fail-closed
    /// posture this class exists to hold.
    /// </remarks>
    /// <param name="tenant">The tenant the action targets.</param>
    /// <param name="cancellationToken">Cancels the authorization.</param>
    /// <returns>The tenant's record when the caller is authorized over it; otherwise <see langword="null"/>.</returns>
    public async ValueTask<TenantRecord?> TryAuthorizeTenantAdminAsync(
        TenantId tenant, CancellationToken cancellationToken = default)
    {
        if (LatticeSystemOrigin.IsActive)
        {
            return await _registry.GetAsync(tenant, cancellationToken).ConfigureAwait(false);
        }

        var record = await _registry.GetAsync(tenant, cancellationToken).ConfigureAwait(false);
        if (record is null)
        {
            // No record means no authority to grant, for an operator as much as
            // for anyone else, and reporting it identically keeps the caller from
            // learning whether the tenant exists.
            return null;
        }

        var subject = await ResolveSubjectAsync(cancellationToken).ConfigureAwait(false);
        if (await IsPlatformOperatorAsync(subject, cancellationToken).ConfigureAwait(false))
        {
            return record;
        }

        return !subject.IsAnonymous && record.HasAdminSubject(subject.SubjectId) ? record : null;
    }

    private async ValueTask<bool> IsPlatformOperatorAsync(LatticeSubject subject, CancellationToken cancellationToken)
    {
        if (subject.IsAnonymous)
        {
            return false;
        }

        var request = new LatticeAccessRequest(
            LatticeAuthReservedTrees.PolicyTreeId, LatticeOperation.Admin, subject);
        var decision = await _gate.AuthorizeAsync(in request, cancellationToken).ConfigureAwait(false);

        // A whole-scope operator capability can never be narrowed to a key subset,
        // so a key-filtered allow is refused fail-closed.
        return decision.Allowed && decision.KeyFilter is null;
    }

    private async ValueTask<TenantRecord> LoadOrThrowAsync(TenantId tenant, CancellationToken cancellationToken)
    {
        var record = await _registry.GetAsync(tenant, cancellationToken).ConfigureAwait(false);
        return record ?? throw new TenantNotFoundException(tenant.Value);
    }

    private ValueTask<LatticeSubject> ResolveSubjectAsync(CancellationToken cancellationToken)
    {
        if (_membership is null)
        {
            return new ValueTask<LatticeSubject>(LatticeSubject.Anonymous);
        }

        if (_membership.TryResolveCurrent(out var subject))
        {
            return new ValueTask<LatticeSubject>(subject);
        }

        return ResolveUncachedAsync(cancellationToken);
    }

    private async ValueTask<LatticeSubject> ResolveUncachedAsync(CancellationToken cancellationToken)
    {
        using (LatticeSystemOrigin.Enter())
        {
            return await _membership!.ResolveCurrentAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
