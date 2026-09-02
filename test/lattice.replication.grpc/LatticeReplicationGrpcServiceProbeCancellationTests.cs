using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// Pins the cancellation arm of the two peer-read probe RPCs,
/// <see cref="LatticeReplicationGrpcService.ProbeDigest"/> and
/// <see cref="LatticeReplicationGrpcService.ProbeMerkleWalk"/>.
/// <para>
/// Both RPCs deliberately translate an
/// <see cref="InvalidOperationException"/> from the projection-digest read
/// into a "digest unavailable" answer, because a locally disabled digest is
/// a non-comparable outcome rather than a fault. A caller-initiated
/// cancellation must NOT be folded into that same answer: reporting
/// "unavailable" to a peer that has already gone away would have the
/// probing side record a false negative and skip a comparison it never
/// actually performed.
/// </para>
/// </summary>
[TestFixture]
public class LatticeReplicationGrpcServiceProbeCancellationTests
{
    private sealed class TestEncoder(Serializer<ReplicationBatchEnvelope> serializer) : IReplicationBatchEncoder
    {
        public string ContentType => "test/binary";
        public int CurrentWireVersion => 1;
        public void Encode(ReplicationBatchEnvelope envelope, System.Buffers.IBufferWriter<byte> writer) => serializer.Serialize(envelope, writer);
        public ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload) => serializer.Deserialize(payload.Span);
    }

    private static LatticeReplicationGrpcService CreateService(IGrainFactory grainFactory)
    {
        var sp = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var encoder = new TestEncoder(sp.GetRequiredService<Serializer<ReplicationBatchEnvelope>>());
        var method = new LatticeReplicationGrpcMethod(
            encoder,
            new OrleansBinaryWalRecordEncoder(sp.GetRequiredService<Serializer<WalRecord>>()),
            sp.GetRequiredService<Serializer<ReplicationAck>>(),
            sp.GetRequiredService<Serializer<DigestProbeRequest>>(),
            sp.GetRequiredService<Serializer<DigestProbeResponse>>(),
            sp.GetRequiredService<Serializer<ContentManifestRequest>>(),
            sp.GetRequiredService<Serializer<ContentManifestResponse>>(),
            sp.GetRequiredService<Serializer<CompressionDictionaryPullRequest>>(),
            sp.GetRequiredService<Serializer<CompressionDictionaryPullResponse>>(),
            sp.GetRequiredService<Serializer<MerkleWalkProbeRequest>>(),
            sp.GetRequiredService<Serializer<MerkleWalkProbeResponse>>(),
            sp.GetRequiredService<Serializer<PeerHighWaterMarkRequest>>(),
            sp.GetRequiredService<Serializer<PeerHighWaterMarkResponse>>());

        return new LatticeReplicationGrpcService(
            method,
            Substitute.For<IReplicationApplier>(),
            new InMemoryWalCursorRegistry(),
            NoOpReceiverFlowControlPolicy.Instance,
            grainFactory,
            new ReceiverAppliedContentIndex(),
            NullLogger<LatticeReplicationGrpcService>.Instance,
            dictionaryProvider: null,
            replicationContext: new EnrollAllReplicationContext());
    }

    private static IGrainFactory FactoryWhoseDigestReadCancels()
    {
        var lattice = Substitute.For<ILattice>();
        lattice.GetLeafProjectionDigestAsync(Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns<Task<LeafProjectionDigest>>(_ => throw new OperationCanceledException());
        lattice.GetLeafProjectionDigestForRangeAsync(
                Arg.Any<int>(), Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns<Task<LeafProjectionDigest>>(_ => throw new OperationCanceledException());

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILattice>("tree").Returns(lattice);
        return factory;
    }

    [Test]
    public void ProbeDigest_propagates_caller_cancellation_instead_of_reporting_digest_unavailable()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var service = CreateService(FactoryWhoseDigestReadCancels());

        Assert.That(
            async () => await service.ProbeDigest(
                new DigestProbeRequestBox { Value = new DigestProbeRequest { TreeName = "tree", ShardIndex = 0 } },
                new TestServerCallContext(cts.Token)),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void ProbeMerkleWalk_propagates_caller_cancellation_instead_of_reporting_range_unavailable()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var service = CreateService(FactoryWhoseDigestReadCancels());

        Assert.That(
            async () => await service.ProbeMerkleWalk(
                new MerkleWalkProbeRequestBox
                {
                    Value = new MerkleWalkProbeRequest
                    {
                        TreeName = "tree",
                        ShardIndex = 0,
                        RangeStartKey = "a",
                        RangeEndKey = "z",
                    },
                },
                new TestServerCallContext(cts.Token)),
            Throws.InstanceOf<OperationCanceledException>());
    }

    private sealed class TestServerCallContext(CancellationToken cancellationToken) : ServerCallContext
    {
        protected override string MethodCore => "Probe";
        protected override string HostCore => string.Empty;
        protected override string PeerCore => string.Empty;
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override global::Grpc.Core.Metadata RequestHeadersCore { get; } = new();
        protected override CancellationToken CancellationTokenCore => cancellationToken;
        protected override global::Grpc.Core.Metadata ResponseTrailersCore { get; } = new();
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => new(string.Empty, new Dictionary<string, List<AuthProperty>>());
        protected override IDictionary<object, object> UserStateCore { get; } = new Dictionary<object, object>();
        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options)
            => throw new NotSupportedException();
        protected override Task WriteResponseHeadersAsyncCore(global::Grpc.Core.Metadata responseHeaders) => Task.CompletedTask;
    }
}
