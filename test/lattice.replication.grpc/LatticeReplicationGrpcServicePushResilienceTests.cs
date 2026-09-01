using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// Pins how <see cref="LatticeReplicationGrpcService.Push"/> behaves when a
/// purely diagnostic dependency fails after the batch has already been
/// applied.
/// <para>
/// The blocked-floor read and the receiver flow-control evaluation both run
/// after the applier has committed. Their failures are therefore deliberately
/// swallowed: the entries are durable, so turning a registry or policy outage
/// into a transport fault would make the sender re-ship data the receiver
/// already holds, converting a diagnostic outage into a replication outage. A
/// caller-initiated cancellation is the one exception - it must still
/// propagate, because the caller is gone and there is nobody to ack to.
/// </para>
/// </summary>
[TestFixture]
public class LatticeReplicationGrpcServicePushResilienceTests
{
    private sealed class TestEncoder(Serializer<ReplicationBatchEnvelope> serializer) : IReplicationBatchEncoder
    {
        public string ContentType => "test/binary";
        public int CurrentWireVersion => 1;
        public void Encode(ReplicationBatchEnvelope envelope, System.Buffers.IBufferWriter<byte> writer) => serializer.Serialize(envelope, writer);
        public ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload) => serializer.Deserialize(payload.Span);
    }

    private static LatticeReplicationGrpcService CreateService(
        IWalCursorRegistry cursorRegistry,
        IReceiverFlowControlPolicy policy)
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

        var applier = Substitute.For<IReplicationApplier>();
        applier.ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new ApplyResult { Applied = true, HighWaterMark = HybridLogicalClock.Zero }));

        return new LatticeReplicationGrpcService(
            method,
            applier,
            cursorRegistry,
            policy,
            Substitute.For<IGrainFactory>(),
            new ReceiverAppliedContentIndex(),
            NullLogger<LatticeReplicationGrpcService>.Instance,
            dictionaryProvider: null,
            replicationContext: new EnrollAllReplicationContext());
    }

    private static IWalCursorRegistry HealthyRegistry()
    {
        // A substitute rather than InMemoryWalCursorRegistry: the in-memory
        // implementation honours the cancellation token, which would trip the
        // blocked-floor arm first and hide the flow-control arm under test.
        var registry = Substitute.For<IWalCursorRegistry>();
        registry.GetBlockedFloorAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<HybridLogicalClock?>(null));
        return registry;
    }

    private static IWalCursorRegistry RegistryFailingWith(Exception failure)
    {
        var registry = Substitute.For<IWalCursorRegistry>();
        registry.GetBlockedFloorAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns<Task<HybridLogicalClock?>>(_ => throw failure);
        return registry;
    }

    private static IReceiverFlowControlPolicy PolicyFailingWith(Exception failure)
    {
        var policy = Substitute.For<IReceiverFlowControlPolicy>();
        policy.EvaluateAsync(Arg.Any<ReceiverFlowControlContext>(), Arg.Any<CancellationToken>())
            .Returns<ValueTask<ReceiverFlowControlHint>>(_ => throw failure);
        return policy;
    }

    private static ReplicationBatchEnvelopeBox EmptyBox() => new()
    {
        Value = new ReplicationBatchEnvelope
        {
            TreeName = "tree",
            OriginClusterId = "remote",
            Entries = [],
        },
    };

    [Test]
    public async Task Push_still_acks_when_the_blocked_floor_read_faults()
    {
        var service = CreateService(
            RegistryFailingWith(new InvalidOperationException("cursor registry offline")),
            NoOpReceiverFlowControlPolicy.Instance);

        var ack = await service.Push(EmptyBox(), new TestServerCallContext(CancellationToken.None));

        // The entries are already durable, so a diagnostic-slot outage must
        // not be promoted into a transport failure the sender would retry.
        Assert.That(ack.Value.Accepted, Is.True);
    }

    [Test]
    public void Push_propagates_caller_cancellation_from_the_blocked_floor_read()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var service = CreateService(
            RegistryFailingWith(new OperationCanceledException()),
            NoOpReceiverFlowControlPolicy.Instance);

        Assert.That(
            async () => await service.Push(EmptyBox(), new TestServerCallContext(cts.Token)),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task Push_still_acks_when_the_flow_control_policy_faults()
    {
        var service = CreateService(
            HealthyRegistry(),
            PolicyFailingWith(new InvalidOperationException("policy exploded")));

        var ack = await service.Push(EmptyBox(), new TestServerCallContext(CancellationToken.None));

        Assert.Multiple(() =>
        {
            Assert.That(ack.Value.Accepted, Is.True);
            // The hint slots are omitted rather than guessed, so the sender
            // resumes at its configured batch size on the next pump tick.
            Assert.That(ack.Value.SuggestedBatchSize, Is.Null);
            Assert.That(ack.Value.PauseForMs, Is.Null);
        });
    }

    [Test]
    public void Push_propagates_caller_cancellation_from_the_flow_control_policy()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var service = CreateService(
            HealthyRegistry(),
            PolicyFailingWith(new OperationCanceledException()));

        Assert.That(
            async () => await service.Push(EmptyBox(), new TestServerCallContext(cts.Token)),
            Throws.InstanceOf<OperationCanceledException>());
    }

    private sealed class TestServerCallContext(CancellationToken cancellationToken) : ServerCallContext
    {
        protected override string MethodCore => "Push";
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
