namespace Orleans.Lattice.Api.Replication.Grpc;

/// <summary>
/// Process-wide holder for the resolved <see cref="LatticeReplicationStatusGrpcMethods"/>.
/// Bridges the DI graph to the static <c>BindService</c> callback that
/// <c>Grpc.AspNetCore</c> invokes at startup (which cannot accept DI dependencies
/// directly). Setting it more than once is allowed; the last registration wins,
/// matching the sibling <see cref="LatticeReplicationGrpcMethodsHolder"/>.
/// </summary>
internal static class LatticeReplicationStatusGrpcMethodsHolder
{
    /// <summary>The current resolved methods, or <see langword="null"/> before registration.</summary>
    public static LatticeReplicationStatusGrpcMethods? Current { get; set; }
}
