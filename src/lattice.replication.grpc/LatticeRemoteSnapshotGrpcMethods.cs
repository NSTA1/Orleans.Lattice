using Grpc.Core;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc;

/// <summary>
/// Code-first gRPC method holder for the cross-cluster snapshot
/// transport RPCs. Hosts the two methods the receiver-side
/// <see cref="GrpcRemoteSnapshotTransport"/> client and the
/// sender-side <see cref="LatticeRemoteSnapshotGrpcService"/> server
/// share: a unary <c>GetMetadata</c> RPC that captures the snapshot
/// cut-point, and a server-streaming <c>RequestSnapshot</c> RPC that
/// drains the entries at that cut-point.
/// </summary>
/// <remarks>
/// The methods are lazily constructed from the Orleans-serialiser
/// dependencies via
/// <see cref="LatticeRemoteSnapshotGrpcMarshallers"/>. Both the client
/// transport and the server-side service consume the same
/// <see cref="LatticeRemoteSnapshotGrpcMethods"/> singleton so they
/// share a single method instance across every call - a code-first
/// gRPC method's identity is its
/// <see cref="Method.FullName"/>, so a single registration is enough
/// for any number of channels and bind hooks.
/// </remarks>
internal sealed class LatticeRemoteSnapshotGrpcMethods
{
    /// <summary>
    /// Logical service name carried in the
    /// <see cref="Method.ServiceName"/> slot of every snapshot RPC.
    /// The receiver-side auth interceptor scopes the secret-header
    /// check by this prefix.
    /// </summary>
    public const string ServiceName = "orleans.lattice.replication.LatticeRemoteSnapshot";

    /// <summary>The <c>GetMetadata</c> unary RPC method name.</summary>
    public const string GetMetadataMethodName = "GetMetadata";

    /// <summary>The <c>RequestSnapshot</c> server-streaming RPC method name.</summary>
    public const string RequestSnapshotMethodName = "RequestSnapshot";

    private readonly Method<RemoteSnapshotMetadataRequestBox, RemoteSnapshotMetadataBox> _getMetadata;
    private readonly Method<RemoteSnapshotMetadataRequestBox, RemoteSnapshotStreamItemBox> _requestSnapshot;

    /// <summary>
    /// Initialises the method holder by composing the request and
    /// response marshallers from the supplied Orleans serialisers.
    /// </summary>
    public LatticeRemoteSnapshotGrpcMethods(
        Serializer<RemoteSnapshotMetadataRequest> requestSerializer,
        Serializer<RemoteSnapshotMetadata> metadataSerializer,
        Serializer<RemoteSnapshotStreamItem> streamItemSerializer)
    {
        ArgumentNullException.ThrowIfNull(requestSerializer);
        ArgumentNullException.ThrowIfNull(metadataSerializer);
        ArgumentNullException.ThrowIfNull(streamItemSerializer);

        var requestMarshaller = LatticeRemoteSnapshotGrpcMarshallers.CreateRequestMarshaller(requestSerializer);
        var metadataMarshaller = LatticeRemoteSnapshotGrpcMarshallers.CreateMetadataMarshaller(metadataSerializer);
        var streamItemMarshaller = LatticeRemoteSnapshotGrpcMarshallers.CreateStreamItemMarshaller(streamItemSerializer);

        _getMetadata = new Method<RemoteSnapshotMetadataRequestBox, RemoteSnapshotMetadataBox>(
            MethodType.Unary,
            ServiceName,
            GetMetadataMethodName,
            requestMarshaller,
            metadataMarshaller);

        _requestSnapshot = new Method<RemoteSnapshotMetadataRequestBox, RemoteSnapshotStreamItemBox>(
            MethodType.ServerStreaming,
            ServiceName,
            RequestSnapshotMethodName,
            requestMarshaller,
            streamItemMarshaller);
    }

    /// <summary>
    /// The unary <c>GetMetadata</c> RPC. The receiver invokes it once
    /// to capture the sender-side cut-point before draining the
    /// streaming RPC.
    /// </summary>
    public Method<RemoteSnapshotMetadataRequestBox, RemoteSnapshotMetadataBox> GetMetadata => _getMetadata;

    /// <summary>
    /// The server-streaming <c>RequestSnapshot</c> RPC. Each yielded
    /// message wraps one <see cref="SnapshotEntry"/>; the stream
    /// terminates when the sender's
    /// <see cref="ISnapshotProvider"/> export completes.
    /// </summary>
    public Method<RemoteSnapshotMetadataRequestBox, RemoteSnapshotStreamItemBox> RequestSnapshot => _requestSnapshot;
}