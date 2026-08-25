namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The visibility scope a caller asserts when reading per-tenant observability
/// through <see cref="ITenantObservabilityView.ListAsync"/>. It is the explicit,
/// fail-closed channel by which a platform operator opts out of the per-tenant
/// default and into the cluster-wide, all-tenant view.
/// </summary>
/// <remarks>
/// <para>
/// There is no ambient all-tenant view. The default,
/// <see cref="ActiveTenant"/>, always resolves to the caller's ambient active
/// tenant (<see cref="LatticeActiveTenantContext"/>), so a tenant - and a
/// platform operator that has not asserted otherwise - sees only its own series.
/// To read across tenants the caller must explicitly construct the
/// <see cref="ClusterWide(LatticeSubject)"/> assertion naming the operator
/// subject, which <see cref="ITenantObservabilityView"/> validates against the
/// auth gate's platform-operator root of trust before exposing any other tenant's
/// series. An unvalidated assertion fails closed to the active tenant.
/// </para>
/// <para>
/// A <c>readonly record struct</c> that is in-process request vocabulary only; it
/// is never persisted or sent on the wire, so it carries no Orleans serialization
/// attributes.
/// </para>
/// </remarks>
public readonly record struct TenantObservabilityScope
{
    private TenantObservabilityScope(bool isClusterWide, LatticeSubject subject)
    {
        IsClusterWide = isClusterWide;
        Subject = subject;
    }

    /// <summary>
    /// <c>true</c> when this is the explicit cluster-wide platform-operator
    /// assertion; <c>false</c> for the default active-tenant scope.
    /// </summary>
    public bool IsClusterWide { get; }

    /// <summary>
    /// The operator subject asserting cluster-wide scope, validated against the
    /// auth gate's platform-operator root of trust. Meaningful only when
    /// <see cref="IsClusterWide"/> is <c>true</c>; for the default scope it is the
    /// well-known <see cref="LatticeSubject.Anonymous"/> subject and is never
    /// consulted.
    /// </summary>
    public LatticeSubject Subject { get; }

    /// <summary>
    /// The default scope: resolve the caller's own active tenant only. Never
    /// exposes another tenant's series.
    /// </summary>
    public static TenantObservabilityScope ActiveTenant { get; } =
        new(isClusterWide: false, LatticeSubject.Anonymous);

    /// <summary>
    /// The explicit cluster-wide platform-operator assertion for
    /// <paramref name="subject"/>. Exposes every tenant's series only when
    /// <paramref name="subject"/> validates as a platform operator; otherwise the
    /// read falls back, fail-closed, to the caller's active tenant.
    /// </summary>
    /// <param name="subject">The operator subject asserting the scope.</param>
    /// <returns>The cluster-wide scope assertion.</returns>
    public static TenantObservabilityScope ClusterWide(LatticeSubject subject) =>
        new(isClusterWide: true, subject);
}
