namespace Orleans.Lattice.Explorer.Core.Connection;

/// <summary>
/// A live source of the tenant an Explorer circuit asserts on its calls to the
/// cluster, through the <see cref="LatticeActiveTenantAssertion.DefaultHeaderName"/>
/// metadata header. The call pipeline reads <see cref="AssertedTenant"/> on every
/// call, the same way it asks an <see cref="ILatticeCallCredentialProvider"/> for
/// the credential, so a tenant switch changes the very next call and nothing
/// about the tenant is ever captured on a channel.
/// </summary>
/// <remarks>
/// <para>
/// <b>An assertion, not a grant.</b> The cluster re-validates the asserted tenant
/// against the caller's own membership before it scopes anything, so asserting a
/// tenant gives the caller no standing in it, and a call that succeeds proves
/// nothing about the caller's tenancy.
/// </para>
/// <para>
/// <b>Per circuit.</b> An implementation belongs to one circuit and reads that
/// circuit's tenant, so it must be registered scoped and never captured by a
/// singleton. It must be thread-safe for reads: a call may start on any thread.
/// </para>
/// </remarks>
public interface ILatticeActiveTenantProvider
{
    /// <summary>
    /// The tenant id to assert on the call about to be made, or
    /// <see langword="null"/> to assert none, in which case the header is not sent
    /// at all and the cluster serves the call exactly as it would for a
    /// tenant-unaware client.
    /// </summary>
    string? AssertedTenant { get; }
}
