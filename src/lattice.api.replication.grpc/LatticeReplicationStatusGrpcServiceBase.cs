using Grpc.Core;

namespace Orleans.Lattice.Api.Replication.Grpc;

/// <summary>
/// Abstract base for the replication peer-status gRPC service. Carries the
/// <see cref="BindServiceMethodAttribute"/> that <c>Grpc.AspNetCore</c> reflects
/// against to register the unary <c>GetPeerStatus</c> RPC, mirroring the
/// base/derived split of <see cref="LatticeReplicationGrpcServiceBase"/>.
/// </summary>
[BindServiceMethod(typeof(LatticeReplicationStatusGrpcServiceBase), nameof(BindService))]
internal abstract class LatticeReplicationStatusGrpcServiceBase
{
    /// <summary>Reads one page of peer status. Implemented in <see cref="LatticeReplicationStatusGrpcService"/>.</summary>
    /// <param name="request">The query.</param>
    /// <param name="context">The server call context.</param>
    /// <returns>The page.</returns>
    public abstract Task<ReplicationPeerStatusPage> GetPeerStatus(ReplicationPeerStatusQuery request, ServerCallContext context);

    /// <summary>
    /// gRPC binding hook invoked by <c>Grpc.AspNetCore</c>. Called once at startup
    /// with <paramref name="serviceImpl"/> set to <see langword="null"/> to record
    /// method metadata; the actual service instance is resolved per request.
    /// </summary>
    /// <param name="binder">The service binder. Must not be <see langword="null"/>.</param>
    /// <param name="serviceImpl">The service instance, or <see langword="null"/> when recording metadata.</param>
    /// <exception cref="ArgumentNullException"><paramref name="binder"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">The method definitions were not resolved before binding.</exception>
    public static void BindService(ServiceBinderBase binder, LatticeReplicationStatusGrpcServiceBase? serviceImpl)
    {
        ArgumentNullException.ThrowIfNull(binder);

        var methods = LatticeReplicationStatusGrpcMethodsHolder.Current
            ?? throw new InvalidOperationException(
                "LatticeReplicationStatusGrpcMethodsHolder.Current was not initialised before BindService. "
                + $"Ensure {nameof(LatticeReplicationApiGrpcServiceCollectionExtensions.AddLatticeReplicationStatusApiGrpc)} ran and that "
                + $"{nameof(LatticeReplicationApiGrpcServiceCollectionExtensions.MapLatticeReplicationStatusApiGrpc)} pre-resolved "
                + "LatticeReplicationStatusGrpcMethods before Grpc.AspNetCore reflected on the service type.");

        if (serviceImpl is null)
        {
            binder.AddMethod(methods.GetPeerStatus, (UnaryServerMethod<ReplicationPeerStatusQuery, ReplicationPeerStatusPage>?)null);
            return;
        }

        binder.AddMethod(
            methods.GetPeerStatus,
            new UnaryServerMethod<ReplicationPeerStatusQuery, ReplicationPeerStatusPage>(serviceImpl.GetPeerStatus));
    }
}
