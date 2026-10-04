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
/// Issue #4534: a push carrying the re-seed request header is answered with
/// the receiver's last completed bootstrap epoch from the (verified) sender,
/// and starts a bootstrap when none has completed past the requested epoch.
/// </summary>
[TestFixture]
public class LatticeReplicationGrpcServiceReseedTests
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

        var applier = Substitute.For<IReplicationApplier>();
        applier.ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new ApplyResult { Applied = true, HighWaterMark = HybridLogicalClock.Zero }));

        return new LatticeReplicationGrpcService(
            method,
            applier,
            HealthyRegistry(),
            NoOpReceiverFlowControlPolicy.Instance,
            grainFactory,
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

    private static ReplicationBatchEnvelopeBox EmptyBox() => new()
    {
        Value = new ReplicationBatchEnvelope
        {
            TreeName = "tree",
            OriginClusterId = "remote",
            Entries = [],
        },
    };

    private static (IGrainFactory Factory, ILatticeBootstrapCoordinatorGrain Coordinator) Coordinator(long? completed)
    {
        var coordinator = Substitute.For<ILatticeBootstrapCoordinatorGrain>();
        coordinator.GetCompletedExportEpochAsync("remote").Returns(Task.FromResult(completed));
        coordinator.GetStatusAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new BootstrapCoordinatorStatus(LatticeBootstrapState.Idle, null)));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeBootstrapCoordinatorGrain>("tree").Returns(coordinator);
        return (factory, coordinator);
    }

    [Test]
    public async Task Push_with_a_reseed_request_echoes_the_completed_epoch()
    {
        var (factory, coordinator) = Coordinator(completed: 4);

        var ack = await CreateService(factory).Push(EmptyBox(), new TestServerCallContext(reseedAfter: 2));

        Assert.That(ack.Value.BootstrapEpoch, Is.EqualTo(4));
        await coordinator.DidNotReceiveWithAnyArgs().BootstrapAsync(default!, default);
    }

    [Test]
    public async Task Push_with_a_reseed_request_past_every_completed_bootstrap_starts_one_from_the_sender()
    {
        var (factory, coordinator) = Coordinator(completed: null);

        var ack = await CreateService(factory).Push(EmptyBox(), new TestServerCallContext(reseedAfter: 0));

        Assert.That(ack.Value.BootstrapEpoch, Is.Null);
        await coordinator.Received(1).BootstrapAsync("remote", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Push_without_the_header_never_consults_the_bootstrap_coordinator()
    {
        var (factory, coordinator) = Coordinator(completed: 4);

        var ack = await CreateService(factory).Push(EmptyBox(), new TestServerCallContext(reseedAfter: null));

        Assert.That(ack.Value.BootstrapEpoch, Is.Null);
        await coordinator.DidNotReceiveWithAnyArgs().GetCompletedExportEpochAsync(default!);
    }

    private sealed class TestServerCallContext(long? reseedAfter) : ServerCallContext
    {
        protected override string MethodCore => "Push";
        protected override string HostCore => string.Empty;
        protected override string PeerCore => string.Empty;
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override global::Grpc.Core.Metadata RequestHeadersCore { get; } = Headers(reseedAfter);
        protected override CancellationToken CancellationTokenCore => CancellationToken.None;
        protected override global::Grpc.Core.Metadata ResponseTrailersCore { get; } = new();
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => new(string.Empty, new Dictionary<string, List<AuthProperty>>());
        protected override IDictionary<object, object> UserStateCore { get; } = new Dictionary<object, object>();
        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options)
            => throw new NotSupportedException();
        private static global::Grpc.Core.Metadata Headers(long? reseedAfter)
        {
            var headers = new global::Grpc.Core.Metadata { { LatticeReplicationGrpcMetadataNames.OriginClusterIdHeader, "remote" } };
            if (reseedAfter is { } epoch)
            {
                headers.Add(LatticeReplicationGrpcMetadataNames.ReseedAfterEpochHeader, epoch.ToString(System.Globalization.CultureInfo.InvariantCulture));
            }

            return headers;
        }

        protected override Task WriteResponseHeadersAsyncCore(global::Grpc.Core.Metadata responseHeaders) => Task.CompletedTask;
    }
}
