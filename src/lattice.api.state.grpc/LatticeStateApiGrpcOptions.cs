namespace Orleans.Lattice.Api.State.Grpc;

/// <summary>
/// Options controlling the <c>Orleans.Lattice.Api.State.Grpc</c> server-side
/// binding.
/// </summary>
public sealed class LatticeStateApiGrpcOptions
{
    /// <summary>
    /// Whether the authorization interceptor enforces
    /// <see cref="ILatticeStateApiAuthorizer"/> on every inbound state-API
    /// call. Defaults to <see langword="true"/> (default-deny): the binding
    /// fails closed unless a host either registers a permissive authorizer or
    /// explicitly turns enforcement off. Set to <see langword="false"/> only
    /// when an outer authentication boundary already guards the endpoint.
    /// </summary>
    public bool RequireAuthorization { get; set; } = true;
}
