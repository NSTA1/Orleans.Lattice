namespace Orleans.Lattice;

/// <summary>
/// The target of a tenant-administration authorization: either the cluster-wide
/// <see cref="Platform"/> scope (a platform operator that may administer every
/// tenant) or a single tenant's delegated-admin scope produced by
/// <see cref="ForTenant"/> (a delegated administrator confined to one tenant).
/// The distinction is expressed purely as a <em>scope</em> over the existing
/// <see cref="LatticeOperation.Admin"/> capability - there is no separate
/// platform-operator or delegated-admin capability bit - so a delegated per-tenant
/// administrator is structurally unable to escalate its capabilities or to act on
/// another tenant, because each scope resolves to a distinct, exactly-matched
/// <see cref="TreeScope"/> id.
/// </summary>
/// <remarks>
/// <para>
/// This is in-process authorization vocabulary. Like <see cref="LatticeAccessRequest"/>
/// it is never persisted or sent on the wire by the core library, so it carries no
/// Orleans serialization attributes. It is a small <c>readonly record struct</c> so a
/// scope can be built and turned into a request without a heap allocation.
/// </para>
/// <para>
/// <b>Both scopes are control-plane capabilities, governed fail-closed.</b> Each scope
/// resolves to an id the <c>Orleans.Lattice.Auth</c> gate treats as a control-plane
/// namespace: an unmatched request is denied <em>independently of the data-plane
/// default effect</em> (a caller is never granted an administrative capability merely
/// because the deployment runs allow-by-default). A capability is held only by a
/// bootstrap administrator (the cluster root of trust) or an explicit matched
/// <see cref="LatticeOperation.Admin"/> allow rule on the scope's exact id.
/// </para>
/// <para>
/// <b>Platform-operator scope.</b> <see cref="Platform"/> resolves to the reserved
/// authorization policy tree id (<see cref="PlatformScopeId"/>, <c>"sys-auth-policy"</c>),
/// the cluster-wide root of trust the auth gate governs with control-plane isolation.
/// A platform operator is a bootstrap administrator, or a subject holding the
/// access-administration delegation grant (a whole-tree <see cref="LatticeOperation.Admin"/>
/// rule on the policy tree). This is the same platform-operator model the tenant-admin
/// facade and reserved-namespace surface already use; it deliberately does <em>not</em>
/// reuse the all-trees (<c>"*"</c>) data-plane sentinel, whose reach would otherwise
/// depend on the default effect and the all-trees grant tier.
/// </para>
/// <para>
/// <b>Delegated per-tenant scope.</b> <see cref="ForTenant"/> resolves to a
/// deterministic id <c><see cref="TenantScopePrefix"/> + tenant</c> (for example
/// <c>_lattice_tenant_admin_acme</c>). The id lives under the platform-owned
/// <c>_lattice_</c> namespace deliberately: it names a control-plane <em>capability</em>,
/// not tenant data, so it is governed purely by the auth policy and is never entangled
/// with tenant data-residency isolation. The auth gate treats this namespace as
/// control-plane too, so an unmatched request fails closed regardless of the default
/// effect, yet the grant remains an ordinary authorable <see cref="LatticeOperation.Admin"/>
/// rule (the id is not the reserved <c>sys-auth-</c> policy namespace). Because the
/// policy engine matches a rule to a request by <em>exact</em> tree id, a grant on one
/// tenant's scope id can never match another tenant's scope id (no cross-tenant reach)
/// nor the platform <see cref="PlatformScopeId"/> (no capability escalation).
/// </para>
/// </remarks>
public readonly record struct LatticeTenantAdminScope
{
    /// <summary>
    /// The cluster-wide platform-operator scope id (<c>"sys-auth-policy"</c>): the
    /// reserved authorization policy tree that is the cluster-wide root of trust. The
    /// <c>Orleans.Lattice.Auth</c> gate governs this id with control-plane isolation,
    /// so the platform capability is held only by a bootstrap administrator or an
    /// explicit access-administration delegation grant, fail-closed independently of
    /// the data-plane default effect. Matches the auth package's
    /// <c>LatticeAuthReservedTrees.PolicyTreeId</c> by value (kept in sync by an
    /// auth-package drift guard).
    /// </summary>
    public const string PlatformScopeId = "sys-auth-policy";

    /// <summary>
    /// The reserved id prefix for a delegated per-tenant admin scope. A tenant's
    /// scope id is this prefix followed by the tenant id (for example
    /// <c>_lattice_tenant_admin_acme</c>). The <c>_lattice_</c> lead marks it as a
    /// platform-owned control-plane id, distinct from the tenant's <c>t/{tenantId}/</c>
    /// data namespace.
    /// </summary>
    public const string TenantScopePrefix = "_lattice_tenant_admin_";

    private LatticeTenantAdminScope(bool isPlatformWide, TenantId tenant)
    {
        IsPlatformWide = isPlatformWide;
        Tenant = tenant;
    }

    /// <summary>
    /// <c>true</c> for the cluster-wide <see cref="Platform"/> scope; <c>false</c>
    /// for a delegated per-tenant scope produced by <see cref="ForTenant"/>.
    /// </summary>
    public bool IsPlatformWide { get; }

    /// <summary>
    /// The tenant a delegated scope is confined to. <c>default(TenantId)</c> (no
    /// tenant) for the cluster-wide <see cref="Platform"/> scope.
    /// </summary>
    public TenantId Tenant { get; }

    /// <summary>
    /// The cluster-wide platform-operator scope. A caller authorized for this scope
    /// may administer the cluster and, transitively, every tenant.
    /// </summary>
    public static LatticeTenantAdminScope Platform { get; } = new(isPlatformWide: true, tenant: default);

    /// <summary>
    /// Creates the delegated-admin scope confined to <paramref name="tenant"/>.
    /// </summary>
    /// <param name="tenant">The tenant the delegated administrator may administer. Must be an initialised tenant id (not <c>default(TenantId)</c>).</param>
    /// <returns>A scope whose <see cref="IsPlatformWide"/> is <c>false</c> and whose <see cref="TreeScope"/> is the tenant's reserved admin id.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenant"/> is the uninitialised <c>default(TenantId)</c> "no tenant" value.</exception>
    public static LatticeTenantAdminScope ForTenant(TenantId tenant)
    {
        if (tenant.Value is null)
        {
            throw new ArgumentException(
                "A delegated per-tenant admin scope requires an initialised tenant id.",
                nameof(tenant));
        }

        return new LatticeTenantAdminScope(isPlatformWide: false, tenant);
    }

    /// <summary>
    /// The exact tree-scope id this scope authorizes <see cref="LatticeOperation.Admin"/>
    /// on: <see cref="PlatformScopeId"/> for the cluster-wide scope, or
    /// <c><see cref="TenantScopePrefix"/> + tenant</c> for a delegated per-tenant scope.
    /// </summary>
    public string TreeScope => IsPlatformWide
        ? PlatformScopeId
        : string.Concat(TenantScopePrefix, Tenant.Value);

    /// <summary>
    /// Builds the <see cref="LatticeAccessRequest"/> that authorizes
    /// <paramref name="subject"/> for <see cref="LatticeOperation.Admin"/> on this
    /// scope's <see cref="TreeScope"/>. The request is what an
    /// <see cref="ILatticeAccessGate"/> evaluates.
    /// </summary>
    /// <param name="subject">The resolved caller identity, or <see cref="LatticeSubject.Anonymous"/>.</param>
    /// <returns>An admin-operation request targeting <see cref="TreeScope"/>.</returns>
    public LatticeAccessRequest ToAdminRequest(LatticeSubject subject) =>
        new(TreeScope, LatticeOperation.Admin, subject);
}
