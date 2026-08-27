using System.Runtime.CompilerServices;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The fail-closed implementation of <see cref="ITenantObservabilityView"/>. The
/// default read path resolves the caller's ambient active tenant
/// (<see cref="LatticeActiveTenantContext"/>); the cluster-wide path is admitted
/// only when the asserted operator subject authorizes as <c>Admin</c> on the
/// reserved auth policy tree, which the auth gate's control-plane isolation grants
/// only to a platform operator (a bootstrap administrator or an explicitly
/// delegated access administrator) and denies to every other caller regardless of
/// the data-plane default effect.
/// </summary>
/// <remarks>
/// The cluster-wide check reuses the existing auth root of trust rather than
/// introducing a new platform-operator primitive: posing the operator subject as
/// <c>Admin</c> on <see cref="LatticeAuthReservedTrees.PolicyTreeId"/> is the same
/// control-plane gate the tenancy enumeration seam uses for cross-tenant reads. A
/// denied or anonymous assertion falls through, fail-closed, to the active-tenant
/// scope, so a tenant can never observe another tenant's series.
/// </remarks>
internal sealed class TenantObservabilityView(
    TenantObservabilitySource source,
    ILatticeAccessGate accessGate,
    ITenantPolicyEngine engine,
    ILatticeMembershipContext membership) : ITenantObservabilityView
{
    private readonly TenantObservabilitySource _source =
        source ?? throw new ArgumentNullException(nameof(source));

    private readonly ILatticeAccessGate _accessGate =
        accessGate ?? throw new ArgumentNullException(nameof(accessGate));

    private readonly ITenantPolicyEngine _engine =
        engine ?? throw new ArgumentNullException(nameof(engine));

    private readonly ILatticeMembershipContext _membership =
        membership ?? throw new ArgumentNullException(nameof(membership));

    /// <inheritdoc />
    public async Task<TenantObservabilitySnapshot?> GetActiveTenantAsync(CancellationToken cancellationToken = default)
    {
        var tenant = await ResolveValidatedActiveTenantAsync(cancellationToken).ConfigureAwait(false);
        if (tenant is not { Value: not null } validated)
        {
            return null;
        }

        return await _source.SnapshotOneAsync(validated, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<TenantObservabilitySnapshot> ListAsync(
        TenantObservabilityScope scope,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (scope.IsClusterWide &&
            await IsPlatformOperatorAsync(scope.Subject, cancellationToken).ConfigureAwait(false))
        {
            foreach (var snapshot in await _source.SnapshotAllAsync(cancellationToken).ConfigureAwait(false))
            {
                yield return snapshot;
            }

            yield break;
        }

        // Fail-closed default: the caller's own active tenant only, and only once
        // the assertion has been validated against the caller's own membership.
        var tenant = await ResolveValidatedActiveTenantAsync(cancellationToken).ConfigureAwait(false);
        if (tenant is { Value: not null } validated)
        {
            var snapshot = await _source.SnapshotOneAsync(validated, cancellationToken).ConfigureAwait(false);
            if (snapshot is { } own)
            {
                yield return own;
            }
        }
    }

    /// <summary>
    /// Resolves the ambient active tenant and re-validates it against the calling
    /// subject's own membership, returning <c>null</c> when none is asserted or the
    /// caller may not act as the asserted tenant.
    /// </summary>
    /// <remarks>
    /// The active tenant is a caller-supplied assertion lifted from the
    /// <c>lattice-active-tenant</c> header, never a fact. Every other consumer
    /// re-validates it - <see cref="TenantGateEnforcer"/> calls
    /// <see cref="ITenantPolicyEngine.ValidateActiveTenant"/> before admitting
    /// anything - and this read path previously did not, so the "fail-closed"
    /// fall-through this class documents was itself the unvalidated path: a caller
    /// could name any tenant and read its usage, quota, and overage series.
    /// </remarks>
    private async ValueTask<TenantId?> ResolveValidatedActiveTenantAsync(CancellationToken cancellationToken)
    {
        if (LatticeActiveTenantContext.Current is not { Value: not null } asserted)
        {
            return null;
        }

        var subject = _membership.TryResolveCurrent(out var warm)
            ? warm
            : await _membership.ResolveCurrentAsync(cancellationToken).ConfigureAwait(false);

        if (subject.IsAnonymous)
        {
            return null;
        }

        return _engine.ValidateActiveTenant(subject.SubjectId, asserted).Allowed
            ? asserted
            : null;
    }

    private async Task<bool> IsPlatformOperatorAsync(LatticeSubject subject, CancellationToken cancellationToken)
    {
        if (subject.IsAnonymous)
        {
            return false;
        }

        var request = new LatticeAccessRequest(
            LatticeAuthReservedTrees.PolicyTreeId,
            LatticeOperation.Admin,
            subject);

        var decision = await _accessGate.AuthorizeAsync(in request, cancellationToken).ConfigureAwait(false);

        // A whole-scope administrative capability can never be satisfied by an
        // allow that was narrowed to a subset of keys, so a key-filtered decision
        // is refused here exactly as every sibling platform-operator check refuses
        // it (TenantAdminAccessAuthorizer, LatticeTenantAdminAuthorizer, and
        // TenantRegionResidencyAuthorizer all require KeyFilter to be null).
        return decision.Allowed && decision.KeyFilter is null;
    }
}
