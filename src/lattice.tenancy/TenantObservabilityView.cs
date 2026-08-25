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
    ILatticeAccessGate accessGate) : ITenantObservabilityView
{
    private readonly TenantObservabilitySource _source =
        source ?? throw new ArgumentNullException(nameof(source));

    private readonly ILatticeAccessGate _accessGate =
        accessGate ?? throw new ArgumentNullException(nameof(accessGate));

    /// <inheritdoc />
    public Task<TenantObservabilitySnapshot?> GetActiveTenantAsync(CancellationToken cancellationToken = default)
    {
        if (LatticeActiveTenantContext.Current is { Value: not null } tenant)
        {
            return _source.SnapshotOneAsync(tenant, cancellationToken);
        }

        return Task.FromResult<TenantObservabilitySnapshot?>(null);
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

        // Fail-closed default: the caller's own active tenant only.
        if (LatticeActiveTenantContext.Current is { Value: not null } tenant)
        {
            var snapshot = await _source.SnapshotOneAsync(tenant, cancellationToken).ConfigureAwait(false);
            if (snapshot is { } own)
            {
                yield return own;
            }
        }
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
        return decision.Allowed;
    }
}
