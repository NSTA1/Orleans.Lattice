using Grpc.Core;

namespace Orleans.Lattice.Api.Data.Grpc;

/// <summary>
/// Bridges the caller's asserted active tenant carried on an inbound gRPC
/// <see cref="ServerCallContext"/> (a request header) into an ambient
/// <see cref="LatticeActiveTenantContext"/> scope, which the data-API service
/// stamps for the duration of a call so the tenant-aware data plane (the
/// per-tenant admission / quota controller consulted inside the
/// <c>LatticeGrain</c> write path, and the tenant-scoped tree resolution) sees
/// the caller's active tenant rather than the reserved default tenant.
/// </summary>
/// <remarks>
/// <para>
/// This is the active-tenant seam for the write-capable data API, the sibling of
/// <see cref="ILatticeDataApiCredentialBridge"/>. A host that needs a bespoke
/// tenant source (a claim on a pre-resolved principal, a signed edge header, and
/// so on) registers its own implementation before <c>AddLatticeDataApiGrpc</c>
/// runs; the built-in default reads a single configurable header
/// (<see cref="LatticeDataApiGrpcOptions.ActiveTenantHeaderName"/>).
/// </para>
/// <para>
/// <b>Assertion, not fact.</b> The tenant a caller asserts on the wire is only an
/// assertion: the tenancy add-on's real <c>ITenantContextResolver</c> and the
/// per-tenant admission controller re-validate it against the caller's
/// authenticated subject membership downstream. This bridge only lifts the
/// assertion onto the ambient scope; it performs no authorization of its own.
/// </para>
/// <para>
/// <b>Fail-closed.</b> Returning <see langword="null"/> (no asserted tenant, or a
/// syntactically invalid one) leaves the call with no active tenant asserted, so
/// the resolver applies its own membership rules (a single-membership subject
/// defaults implicitly; a multi-membership subject is denied). A missing or
/// malformed tenant header can never cause a call to be attributed to a tenant
/// the caller did not assert.
/// </para>
/// </remarks>
public interface ILatticeDataApiActiveTenantBridge
{
    /// <summary>
    /// Resolves the caller's asserted active tenant from <paramref name="context"/>,
    /// or <see langword="null"/> when the call carries no recognisable, valid
    /// tenant assertion (the call then proceeds with no active tenant asserted).
    /// </summary>
    /// <param name="context">The inbound gRPC server call context.</param>
    /// <returns>
    /// The asserted <see cref="TenantId"/>, or <see langword="null"/> when none is
    /// present or the header value is not a valid tenant id.
    /// </returns>
    TenantId? Resolve(ServerCallContext context);
}
