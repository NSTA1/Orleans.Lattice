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

    /// <summary>
    /// The unary self-distributing shared-dictionary pull RPC method
    /// name. The caller pulls the bytes behind a peer-advertised
    /// dictionary id it does not yet hold so an auto-training cluster
    /// converges onto a peer's trained dictionary.
    /// </summary>
    public const string PullCompressionDictionaryMethodName = "PullCompressionDictionary";

    /// <summary>
    /// The unary anti-entropy Merkle-walk RPC method name. The caller
    /// asks the peer for its content digest over a cluster-stable
    /// separator-key range so a divergent shard can be localised to a
    /// leaf or small key range.
    /// </summary>
    public const string ProbeMerkleWalkMethodName = "ProbeMerkleWalk";

    /// <summary>
    /// The unary anti-entropy peer high-water-mark RPC method name. The
    /// caller asks the peer for the clock it has durably applied for a
    /// given (tree, origin) stream so targeted leaf re-replay bounds its
    /// re-ship set to entries above that watermark.
    /// </summary>
    public const string GetPeerHighWaterMarkMethodName = "GetPeerHighWaterMark";

    private readonly Method<ReplicationBatchEnvelopeBox, ReplicationAckBox> _push;
    private readonly Method<DigestProbeRequestBox, DigestProbeResponseBox> _probeDigest;
    private readonly Method<ContentManifestRequestBox, ContentManifestResponseBox> _exchangeContentManifest;
    private readonly Method<CompressionDictionaryPullRequestBox, CompressionDictionaryPullResponseBox> _pullCompressionDictionary;
    private readonly Method<MerkleWalkProbeRequestBox, MerkleWalkProbeResponseBox> _probeMerkleWalk;
    private readonly Method<PeerHighWaterMarkRequestBox, PeerHighWaterMarkResponseBox> _getPeerHighWaterMark;

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
        Serializer<ContentManifestResponse> contentManifestResponseSerializer,
        Serializer<CompressionDictionaryPullRequest> compressionDictionaryPullRequestSerializer,
        Serializer<CompressionDictionaryPullResponse> compressionDictionaryPullResponseSerializer,
        Serializer<MerkleWalkProbeRequest> merkleWalkProbeRequestSerializer,
        Serializer<MerkleWalkProbeResponse> merkleWalkProbeResponseSerializer,
        Serializer<PeerHighWaterMarkRequest> peerHighWaterMarkRequestSerializer,
        Serializer<PeerHighWaterMarkResponse> peerHighWaterMarkResponseSerializer)
    {
        ArgumentNullException.ThrowIfNull(encoder);
        ArgumentNullException.ThrowIfNull(walRecordEncoder);
        ArgumentNullException.ThrowIfNull(ackSerializer);
        ArgumentNullException.ThrowIfNull(probeRequestSerializer);
        ArgumentNullException.ThrowIfNull(probeResponseSerializer);
        ArgumentNullException.ThrowIfNull(contentManifestRequestSerializer);
        ArgumentNullException.ThrowIfNull(contentManifestResponseSerializer);
        ArgumentNullException.ThrowIfNull(compressionDictionaryPullRequestSerializer);
        ArgumentNullException.ThrowIfNull(compressionDictionaryPullResponseSerializer);
        ArgumentNullException.ThrowIfNull(merkleWalkProbeRequestSerializer);
        ArgumentNullException.ThrowIfNull(merkleWalkProbeResponseSerializer);
        ArgumentNullException.ThrowIfNull(peerHighWaterMarkRequestSerializer);
        ArgumentNullException.ThrowIfNull(peerHighWaterMarkResponseSerializer);

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

        _pullCompressionDictionary = new Method<CompressionDictionaryPullRequestBox, CompressionDictionaryPullResponseBox>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: PullCompressionDictionaryMethodName,
            requestMarshaller: LatticeReplicationGrpcMarshallers.CreateCompressionDictionaryPullRequestMarshaller(compressionDictionaryPullRequestSerializer),
            responseMarshaller: LatticeReplicationGrpcMarshallers.CreateCompressionDictionaryPullResponseMarshaller(compressionDictionaryPullResponseSerializer));

        _probeMerkleWalk = new Method<MerkleWalkProbeRequestBox, MerkleWalkProbeResponseBox>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: ProbeMerkleWalkMethodName,
            requestMarshaller: LatticeReplicationGrpcMarshallers.CreateMerkleWalkProbeRequestMarshaller(merkleWalkProbeRequestSerializer),
            responseMarshaller: LatticeReplicationGrpcMarshallers.CreateMerkleWalkProbeResponseMarshaller(merkleWalkProbeResponseSerializer));

        _getPeerHighWaterMark = new Method<PeerHighWaterMarkRequestBox, PeerHighWaterMarkResponseBox>(
            type: MethodType.Unary,
            serviceName: ServiceName,
            name: GetPeerHighWaterMarkMethodName,
            requestMarshaller: LatticeReplicationGrpcMarshallers.CreatePeerHighWaterMarkRequestMarshaller(peerHighWaterMarkRequestSerializer),
            responseMarshaller: LatticeReplicationGrpcMarshallers.CreatePeerHighWaterMarkResponseMarshaller(peerHighWaterMarkResponseSerializer));
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

    /// <summary>
    /// The unary <c>PullCompressionDictionary</c> RPC method for the
    /// self-distributing shared-dictionary pull. Used by both the
    /// client-side invoker (<see cref="GrpcPushTransport"/>) and the
    /// server-side service binder so both ends wire up identical
    /// marshallers.
    /// </summary>
    public Method<CompressionDictionaryPullRequestBox, CompressionDictionaryPullResponseBox> PullCompressionDictionary => _pullCompressionDictionary;

    /// <summary>
    /// The unary anti-entropy <c>ProbeMerkleWalk</c> RPC method for the
    /// cross-cluster Merkle-walk drift localisation. Used by both the
    /// client-side invoker (<see cref="GrpcPushTransport"/>) and the
    /// server-side service binder so both ends wire up identical
    /// marshallers.
    /// </summary>
    public Method<MerkleWalkProbeRequestBox, MerkleWalkProbeResponseBox> ProbeMerkleWalk => _probeMerkleWalk;

    /// <summary>
    /// The unary anti-entropy <c>GetPeerHighWaterMark</c> RPC method for
    /// bounding targeted leaf re-replay. Used by both the client-side
    /// invoker (<see cref="GrpcPushTransport"/>) and the server-side
    /// service binder so both ends wire up identical marshallers.
    /// </summary>
    public Method<PeerHighWaterMarkRequestBox, PeerHighWaterMarkResponseBox> GetPeerHighWaterMark => _getPeerHighWaterMark;
}

