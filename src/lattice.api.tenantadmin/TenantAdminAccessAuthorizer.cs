using Orleans.Lattice;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The tenant-administration authorization seam. Given the resolved caller, it
/// consults the registered core <see cref="ILatticeAccessGate"/> for the
/// platform-operator <see cref="LatticeOperation.Admin"/> capability a tenant
/// lifecycle operation requires - <see cref="LatticeOperation.Admin"/> on the
/// reserved authorization policy tree (<see cref="PlatformOperatorScope"/>) - and
/// fails closed by throwing <see cref="LatticeAuthorizationDeniedException"/> when
/// the request is not authorized. It is the single choke point the
/// tenant-administration facade consults before touching the tenant registry or a
/// tenant's trees.
/// </summary>
/// <remarks>
/// <para>
/// <b>Control-plane isolation.</b> Tenant lifecycle (create / suspend / resume /
/// delete) is a platform-operator control-plane action, not data-plane traffic, so
/// it authorizes over the reserved policy tree exactly as the sibling
/// <see cref="TenantRegionResidencyAuthorizer"/> operator tier and the
/// tenant-observability view do. The core gate routes the reserved namespace
/// through its control-plane-isolated path, which never inherits the data-plane
/// default effect, so an unmatched caller is denied even under
/// <c>LatticeAuthOptions.DefaultEffect = Allow</c>. A cluster-wide <c>"*"</c> data
/// scope is deliberately <b>not</b> used: it takes the ordinary data-plane path and
/// would fail open to any caller (including an anonymous one) under
/// <c>DefaultEffect = Allow</c>, silently handing full tenant administration to
/// every caller.
/// </para>
/// <para>
/// The enforcement uses only the public access-gate seams, because this add-on
/// package is not on the core library's <c>InternalsVisibleTo</c> list and must not
/// edit the core to add itself: the <b>system-origin</b> gate bypass
/// (<see cref="LatticeSystemOrigin.IsActive"/>), caller-subject resolution through
/// the membership seam (anonymous when no membership context is registered), a
/// single <see cref="LatticeAccessRequest"/> for
/// <see cref="LatticeOperation.Admin"/>, and a fail-closed treatment of a
/// partial / filtered allow (a whole-scope operation can never be narrowed, so a
/// key-filtered allow is refused).
/// </para>
/// <para>
/// The no-op core gate registered by <c>AddLattice</c> in a host with no
/// authorization add-on always allows, so every check short-circuits to allow at
/// negligible control-plane cost without needing to detect the no-op gate type
/// (which is internal to the core).
/// </para>
/// </remarks>
public sealed class TenantAdminAccessAuthorizer
{
    /// <summary>
    /// The control-plane scope tenant-administration operations authorize against:
    /// the reserved authorization policy tree
    /// (<see cref="LatticeTenantAdminScope.PlatformScopeId"/>, which equals
    /// <see cref="LatticeAuthReservedTrees.PolicyTreeId"/>). Tenant lifecycle is a
    /// platform-operator control-plane action, so it is authored as whole-scope
    /// <see cref="LatticeOperation.Admin"/> on this reserved tree, which the core
    /// gate governs with control-plane isolation (fail-closed independent of the
    /// data-plane default effect). Mirrors the platform-operator scope the sibling
    /// <see cref="TenantRegionResidencyAuthorizer"/> and the tenant-observability
    /// view use.
    /// </summary>
    public const string PlatformOperatorScope = LatticeTenantAdminScope.PlatformScopeId;

    private readonly ILatticeAccessGate _gate;
    private readonly ILatticeMembershipContext? _membership;

    /// <summary>
    /// Initializes a new <see cref="TenantAdminAccessAuthorizer"/>.
    /// </summary>
    /// <param name="gate">
    /// The registered core access gate to consult. Must not be <c>null</c>. In a
    /// host with no authorization add-on this is the no-op gate, so every check
    /// short-circuits to allow.
    /// </param>
    /// <param name="membership">
    /// The membership context used to resolve the caller subject, or <c>null</c>
    /// when none is registered (every caller then resolves to
    /// <see cref="LatticeSubject.Anonymous"/>).
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="gate"/> is <c>null</c>.</exception>
    public TenantAdminAccessAuthorizer(ILatticeAccessGate gate, ILatticeMembershipContext? membership = null)
    {
        ArgumentNullException.ThrowIfNull(gate);
        _gate = gate;
        _membership = membership;
    }

    /// <summary>
    /// Authorizes a tenant-administration <b>mutation</b> (create, suspend,
    /// resume, delete) for the current caller over the platform-operator
    /// control-plane scope, throwing
    /// <see cref="LatticeAuthorizationDeniedException"/> when
    /// <see cref="LatticeOperation.Admin"/> authority on the reserved policy tree
    /// is not granted. A partial / filtered allow is refused, fail-closed.
    /// </summary>
    /// <param name="cancellationToken">Cancels the authorization.</param>
    /// <returns>A task that completes when the operation is authorized.</returns>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to administer tenants.</exception>
    public async ValueTask AuthorizeTenantAdminAsync(CancellationToken cancellationToken = default)
    {
        // System-origin (trusted co-hosted infrastructure) bypasses the gate, the
        // same short-circuit the core enforcement applies. This is the public
        // equivalent of the core's internal gate-bypass check; the internal
        // view-maintenance scopes it additionally folds in never wrap a
        // tenant-admin facade call, so the two are equivalent here.
        if (LatticeSystemOrigin.IsActive)
        {
            return;
        }

        var subject = await ResolveSubjectAsync(cancellationToken).ConfigureAwait(false);

        // Build the request outside any system-origin scope so the gate sees the
        // real caller, then authorize the platform-operator Admin capability on the
        // reserved policy tree. The gate routes this reserved id through its
        // control-plane-isolated path, so an unmatched request is denied even under
        // DefaultEffect=Allow (a cluster-wide "*" data scope would fail open there).
        var request = new LatticeAccessRequest(
            PlatformOperatorScope, LatticeOperation.Admin, subject, key: null, rangeStart: null, rangeEnd: null);
        var decision = await _gate.AuthorizeAsync(in request, cancellationToken).ConfigureAwait(false);

        if (!decision.Allowed)
        {
            throw new LatticeAuthorizationDeniedException(
                PlatformOperatorScope,
                LatticeOperation.Admin,
                subject.SubjectId,
                decision.Reason ?? "Tenant administration is not authorized for the caller.");
        }

        // A whole-scope administrative operation can never be narrowed to a subset
        // of keys, so a key-filtered allow is treated as a deny, fail-closed.
        if (decision.KeyFilter is not null)
        {
            throw new LatticeAuthorizationDeniedException(
                PlatformOperatorScope,
                LatticeOperation.Admin,
                subject.SubjectId,
                decision.Reason ?? "Tenant administration is not fully authorized over the cluster; "
                    + "a cluster-wide administrative operation cannot be narrowed and is refused.");
        }
    }

    /// <summary>
    /// Probes whether the current caller may perform tenant-administration
    /// mutations, returning <c>true</c> when authorized and <c>false</c> when
    /// denied. Never throws for a plain authorization denial; other failures
    /// propagate. Read-only, no side effects.
    /// </summary>
    /// <param name="cancellationToken">Cancels the probe.</param>
    /// <returns><c>true</c> when the caller may administer tenants; otherwise <c>false</c>.</returns>
    public async ValueTask<bool> IsTenantAdminAuthorizedAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await AuthorizeTenantAdminAsync(cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (LatticeAuthorizationDeniedException)
        {
            return false;
        }
    }

    private ValueTask<LatticeSubject> ResolveSubjectAsync(CancellationToken cancellationToken)
    {
        if (_membership is null)
        {
            return new ValueTask<LatticeSubject>(LatticeSubject.Anonymous);
        }

        // Warm fast path: a cached or anonymous subject resolves synchronously
        // with no directory read, so the system-origin scope is unnecessary.
        if (_membership.TryResolveCurrent(out var subject))
        {
            return new ValueTask<LatticeSubject>(subject);
        }

        // Cache miss: resolution reads the membership directory's own dogfooded
        // trees through the gated surface, so it must run under a system-origin
        // scope to bypass the gate and avoid re-entering it.
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
