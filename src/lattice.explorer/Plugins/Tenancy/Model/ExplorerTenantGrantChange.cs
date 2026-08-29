namespace Orleans.Lattice.Explorer.Tenancy;

/// <summary>
/// The outcome of a cross-tenant grant transition - an offer, approval,
/// rejection, or revocation - carrying the grant as committed and whether the
/// call changed anything.
/// <para>
/// Read <see cref="Grant"/>'s state rather than assuming the transition landed
/// the grant where the call asked: the transitions are idempotent, so a repeat
/// reports <see cref="Changed"/> <see langword="false"/> with the grant already
/// in its target state.
/// </para>
/// </summary>
/// <param name="Grant">The grant as committed, carrying its resulting state.</param>
/// <param name="Changed">
/// <see langword="true"/> when the call moved the grant; <see langword="false"/>
/// when it was already in the requested state.
/// </param>
public readonly record struct ExplorerTenantGrantChange(
    ExplorerTenantGrant Grant,
    bool Changed);
