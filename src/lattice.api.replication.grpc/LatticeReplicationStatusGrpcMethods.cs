using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Replication.Grpc;

/// <summary>
/// Holds the gRPC <see cref="Method{TRequest, TResponse}"/> definition for the
/// replication peer-status API: one unary, code-first RPC whose request and
/// response are the transport-agnostic
/// <see cref="ReplicationPeerStatusQuery"/> and
/// <see cref="ReplicationPeerStatusPage"/> contracts themselves, serialized with
/// the Orleans serializer. Constructed from DI-resolved serializers so the public
/// client and the server-side binder wire identical marshallers.
/// </summary>
/// <remarks>
/// A separate service from the replication control API
/// (<see cref="LatticeReplicationGrpcMethods.ServiceName"/>) so a host can map the
/// read-only status surface without the control surface, and so the control
/// service stays exactly as it is. It sits behind the same authorization
/// interceptor, which recognises both service names. The contract is
/// additive-only: fields are never renumbered.
/// </remarks>
internal sealed class LatticeReplicationStatusGrpcMethods
{
    /// <summary>The fully-qualified gRPC service name.</summary>
    public const string ServiceName = "orleans.lattice.api.replication.status";

    /// <summary>The unary get-peer-status RPC method name.</summary>
    public const string GetPeerStatusMethodName = "GetPeerStatus";

    /// <summary>Initialises the method definition from DI-resolved serializers.</summary>
    /// <param name="querySerializer">The request serializer. Must not be <see langword="null"/>.</param>
    /// <param name="pageSerializer">The response serializer. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">A serializer is <see langword="null"/>.</exception>
    public LatticeReplicationStatusGrpcMethods(
        Serializer<ReplicationPeerStatusQuery> querySerializer,
        Serializer<ReplicationPeerStatusPage> pageSerializer)
    {
        ArgumentNullException.ThrowIfNull(querySerializer);
        ArgumentNullException.ThrowIfNull(pageSerializer);

        GetPeerStatus = new Method<ReplicationPeerStatusQuery, ReplicationPeerStatusPage>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetPeerStatusMethodName,
            requestMarshaller: LatticeReplicationGrpcMarshallers.Create(querySerializer),
            responseMarshaller: LatticeReplicationGrpcMarshallers.Create(pageSerializer));
    }

    /// <summary>The unary <c>GetPeerStatus</c> RPC.</summary>
    public Method<ReplicationPeerStatusQuery, ReplicationPeerStatusPage> GetPeerStatus { get; }

    /// <summary>
    /// Builds the method definition from the Orleans serializers resolved out of
    /// <paramref name="serializerProvider"/>. Shared by the server-side DI factory
    /// and the public client.
    /// </summary>
    /// <param name="serializerProvider">A provider with Orleans serialization registered. Must not be <see langword="null"/>.</param>
    /// <returns>The method definitions.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="serializerProvider"/> is <see langword="null"/>.</exception>
    public static LatticeReplicationStatusGrpcMethods FromServiceProvider(IServiceProvider serializerProvider)
    {
        ArgumentNullException.ThrowIfNull(serializerProvider);

        return new LatticeReplicationStatusGrpcMethods(
            serializerProvider.GetRequiredService<Serializer<ReplicationPeerStatusQuery>>(),
            serializerProvider.GetRequiredService<Serializer<ReplicationPeerStatusPage>>());
    }
}
