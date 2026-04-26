using Grpc.Core;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc;

/// <summary>
/// Holds the gRPC <see cref="Method{TRequest, TResponse}"/> definition
/// for the replication push RPC. The method is constructed lazily on
/// first access so the encoder and ack serializer can be supplied via
/// DI without a static initialiser race.
/// </summary>
/// <remarks>
/// The wire contract is intentionally a single unary RPC:
/// <c>Push(ReplicationBatchEnvelope) -&gt; ReplicationAck</c>. The
/// underlying <see cref="GrpcChannel"/> multiplexes every batch over a
/// long-lived HTTP/2 connection per peer, so the unary shape achieves
/// the sub-second-latency target without the additional
/// state machine of a bidi stream. Future iterations may promote the
/// method to client-streaming if measurement shows benefit; that
/// promotion is a wire-format-compatible extension because the
/// envelope and ack types are unchanged.
/// </remarks>
internal sealed class LatticeReplicationGrpcMethod
{
    /// <summary>The fully-qualified gRPC service name.</summary>
    public const string ServiceName = "orleans.lattice.replication.LatticeReplication";

    /// <summary>The unary push RPC method name.</summary>
    public const string PushMethodName = "Push";

    private readonly Method<ReplicationBatchEnvelopeBox, ReplicationAckBox> _push;

    /// <summary>
    /// Initialises the holder with the supplied
    /// <paramref name="encoder"/> and <paramref name="ackSerializer"/>.
    /// Resolved from DI in the standard registration path.
    /// </summary>
    public LatticeReplicationGrpcMethod(
        IReplicationBatchEncoder encoder,
        Serializer<ReplicationAck> ackSerializer)
    {
        ArgumentNullException.ThrowIfNull(encoder);
        ArgumentNullException.ThrowIfNull(ackSerializer);

        _push = new Method<ReplicationBatchEnvelopeBox, ReplicationAckBox>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: PushMethodName,
            requestMarshaller: LatticeReplicationGrpcMarshallers.CreateEnvelopeMarshaller(encoder),
            responseMarshaller: LatticeReplicationGrpcMarshallers.CreateAckMarshaller(ackSerializer));
    }

    /// <summary>
    /// The unary <c>Push</c> RPC method. Used by both the client-side
    /// invoker (<see cref="GrpcPushTransport"/>) and the server-side
    /// service binder (<see cref="LatticeReplicationGrpcService"/>) so
    /// both ends are guaranteed to wire up identical marshallers.
    /// </summary>
    public Method<ReplicationBatchEnvelopeBox, ReplicationAckBox> Push => _push;
}

