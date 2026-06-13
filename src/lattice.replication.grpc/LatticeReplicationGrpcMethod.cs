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

    /// <summary>The unary anti-entropy digest-probe RPC method name.</summary>
    public const string ProbeDigestMethodName = "ProbeDigest";

    /// <summary>
    /// The unary content-hash payload-elision manifest-exchange RPC
    /// method name. The sender advertises a per-batch content-hash
    /// manifest and the receiver replies with the entries it is
    /// missing.
    /// </summary>
    public const string ExchangeContentManifestMethodName = "ExchangeContentManifest";

    private readonly Method<ReplicationBatchEnvelopeBox, ReplicationAckBox> _push;
    private readonly Method<DigestProbeRequestBox, DigestProbeResponseBox> _probeDigest;
    private readonly Method<ContentManifestRequestBox, ContentManifestResponseBox> _exchangeContentManifest;

    /// <summary>
    /// Initialises the holder with the supplied
    /// <paramref name="encoder"/> and serializers.
    /// Resolved from DI in the standard registration path.
    /// </summary>
    public LatticeReplicationGrpcMethod(
        IReplicationBatchEncoder encoder,
        IWalRecordEncoder walRecordEncoder,
        Serializer<ReplicationAck> ackSerializer,
        Serializer<DigestProbeRequest> probeRequestSerializer,
        Serializer<DigestProbeResponse> probeResponseSerializer,
        Serializer<ContentManifestRequest> contentManifestRequestSerializer,
        Serializer<ContentManifestResponse> contentManifestResponseSerializer)
    {
        ArgumentNullException.ThrowIfNull(encoder);
        ArgumentNullException.ThrowIfNull(walRecordEncoder);
        ArgumentNullException.ThrowIfNull(ackSerializer);
        ArgumentNullException.ThrowIfNull(probeRequestSerializer);
        ArgumentNullException.ThrowIfNull(probeResponseSerializer);
        ArgumentNullException.ThrowIfNull(contentManifestRequestSerializer);
        ArgumentNullException.ThrowIfNull(contentManifestResponseSerializer);

        _push = new Method<ReplicationBatchEnvelopeBox, ReplicationAckBox>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: PushMethodName,
            requestMarshaller: LatticeReplicationGrpcMarshallers.CreateEnvelopeMarshaller(encoder, walRecordEncoder),
            responseMarshaller: LatticeReplicationGrpcMarshallers.CreateAckMarshaller(ackSerializer));

        _probeDigest = new Method<DigestProbeRequestBox, DigestProbeResponseBox>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: ProbeDigestMethodName,
            requestMarshaller: LatticeReplicationGrpcMarshallers.CreateProbeRequestMarshaller(probeRequestSerializer),
            responseMarshaller: LatticeReplicationGrpcMarshallers.CreateProbeResponseMarshaller(probeResponseSerializer));

        _exchangeContentManifest = new Method<ContentManifestRequestBox, ContentManifestResponseBox>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: ExchangeContentManifestMethodName,
            requestMarshaller: LatticeReplicationGrpcMarshallers.CreateContentManifestRequestMarshaller(contentManifestRequestSerializer),
            responseMarshaller: LatticeReplicationGrpcMarshallers.CreateContentManifestResponseMarshaller(contentManifestResponseSerializer));
    }

    /// <summary>
    /// The unary <c>Push</c> RPC method. Used by both the client-side
    /// invoker (<see cref="GrpcPushTransport"/>) and the server-side
    /// service binder (<see cref="LatticeReplicationGrpcService"/>) so
    /// both ends are guaranteed to wire up identical marshallers.
    /// </summary>
    public Method<ReplicationBatchEnvelopeBox, ReplicationAckBox> Push => _push;

    /// <summary>
    /// The unary anti-entropy <c>ProbeDigest</c> RPC method. Used by both
    /// the client-side invoker (<see cref="GrpcPushTransport"/>) and the
    /// server-side service binder so both ends wire up identical
    /// marshallers.
    /// </summary>
    public Method<DigestProbeRequestBox, DigestProbeResponseBox> ProbeDigest => _probeDigest;

    /// <summary>
    /// The unary <c>ExchangeContentManifest</c> RPC method for the
    /// content-hash payload-elision round trip. Used by both the
    /// client-side invoker (<see cref="GrpcPushTransport"/>) and the
    /// server-side service binder so both ends wire up identical
    /// marshallers.
    /// </summary>
    public Method<ContentManifestRequestBox, ContentManifestResponseBox> ExchangeContentManifest => _exchangeContentManifest;
}

