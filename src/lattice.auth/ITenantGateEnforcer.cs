namespace Orleans.Lattice.Auth;

/// <summary>
/// The tenant-isolation enforcement seam the auth gate consults after it has
/// computed its policy decision. It is the null-seam that lets the auth gate
/// compose tenant isolation <b>without</b> a project reference into the tenancy
/// add-on: <c>Orleans.Lattice.Tenancy</c> references <c>Orleans.Lattice.Auth</c>
/// (and the core), so the arrow can only point that way. The gate depends on
/// this abstraction; the tenancy add-on supplies the active implementation and
/// replaces the null default, exactly as the core tenant seams (for example
/// <see cref="NullTenantContextResolver"/>) do.
/// </summary>
/// <remarks>
/// <para>
/// The default registration is the allow-everything
/// <c>NullTenantGateEnforcer</c>, whose <see cref="IsActive"/> is <c>false</c>.
/// A cluster without the tenancy add-on therefore behaves byte-for-byte as it
/// did before this seam existed: the gate reads a single <see cref="IsActive"/>
/// bool, sees it is <c>false</c>, and never calls <see cref="Enforce"/>. The
/// enforcement path stays synchronous and allocation-free on the tenancy-off
/// fast path.
/// </para>
/// <para>
/// <see cref="Enforce"/> composes with, and never weakens, the policy decision:
/// the gate calls it only for a request the policy engine already allowed, and a
/// deny from either side denies. It is a warm, in-memory decision (no storage
/// I/O), so it is safe to call on the per-request hot path.
/// </para>
/// </remarks>
public interface ITenantGateEnforcer
{
    /// <summary>
    /// <c>true</c> when tenant enforcement is wired in (the tenancy add-on
    /// replaced the null default); <c>false</c> for the null default. The gate
    /// reads this first and skips <see cref="Enforce"/> entirely when it is
    /// <c>false</c>, so the tenancy-off path adds only a single bool read.
    /// </summary>
    bool IsActive { get; }

    /// <summary>
    /// Applies tenant isolation to a request the policy gate already allowed,
    /// returning an allow when the active tenant may touch the tree or a deny
    /// carrying the reason when tenant isolation forbids it.
    /// </summary>
    /// <param name="request">The request under authorization, passed by reference to avoid a copy.</param>
    /// <returns>
    /// An allow decision when tenant isolation admits the request, or a deny
    /// decision carrying the reason when it does not.
    /// </returns>
    LatticeAccessDecision Enforce(in LatticeAccessRequest request);
}
