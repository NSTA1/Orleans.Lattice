using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// Issue #4673: the receiver reads the source lineage a push was read under from
/// the source-lineage call header, after the caller's origin is authenticated,
/// and refuses - not accepted, flagged - a batch stamped with a lineage this tree
/// did not drain from that sender, so its records never apply. A push without
/// the header (a sender that predates it) applies as before.
/// </summary>
[TestFixture]
public class LatticeReplicationGrpcServiceSourceLineageTests
{
    private const string Tree = "tree";
    private const string Origin = "remote";

    private static readonly Guid Epoch = Guid.Parse("9a8b7c6d-5e4f-4a3b-9c2d-1e0f9a8b7c6d");
    private static readonly Guid Drained = Guid.Parse("11111111-2222-4333-8444-555555555555");

    private sealed class TestEncoder(Serializer<ReplicationBatchEnvelope> serializer) : IReplicationBatchEncoder
    {
        public string ContentType => "test/binary";
        public int CurrentWireVersion => 1;
        public void Encode(ReplicationBatchEnvelope envelope, System.Buffers.IBufferWriter<byte> writer) => serializer.Serialize(envelope, writer);
        public ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload) => serializer.Deserialize(payload.Span);
    }

    private sealed class Harness
    {
        public IGrainFactory Factory { get; } = Substitute.For<IGrainFactory>();
        public IReplicationApplier Applier { get; } = Substitute.For<IReplicationApplier>();
        public ILatticeBootstrapCoordinatorGrain Coordinator { get; } = Substitute.For<ILatticeBootstrapCoordinatorGrain>();

        public Harness(ReplicationDrainedLineage? drained)
        {
            var frontier = Substitute.For<IReplicationTreeFrontierGrain>();
            Factory.GetGrain<IReplicationTreeFrontierGrain>(Tree, Arg.Any<string?>()).Returns(frontier);
            frontier.ObserveAsync(Origin, Arg.Any<ReplicationSourceFrontier?>(), Arg.Any<CancellationToken>()).Returns(Epoch);
            Factory.GetGrain<ILatticeBootstrapCoordinatorGrain>(Tree, Arg.Any<string?>()).Returns(Coordinator);
            Coordinator.GetDrainedLineageAsync(Origin).Returns(Task.FromResult(drained));
            Applier.ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
                .Returns(Task.FromResult(new ApplyResult { Applied = true, HighWaterMark = HybridLogicalClock.Zero }));
        }

        public LatticeReplicationGrpcService Service()
        {
            var sp = new ServiceCollection().AddSerializer().BuildServiceProvider();
            var encoder = new TestEncoder(sp.GetRequiredService<Serializer<ReplicationBatchEnvelope>>());
            var method = GrpcTestFactories.CreateMethod(encoder, sp.GetRequiredService<Serializer<ReplicationAck>>());
            var registry = Substitute.For<IWalCursorRegistry>();
            registry.GetBlockedFloorAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                .Returns(Task.FromResult<HybridLogicalClock?>(null));
            var topology = Substitute.For<IReplicationTopology>();
            topology.CurrentPeers.Returns([Origin]);
            return new LatticeReplicationGrpcService(
                method,
                Applier,
                registry,
                NoOpReceiverFlowControlPolicy.Instance,
                Factory,
                new ReceiverAppliedContentIndex(),
                NullLogger<LatticeReplicationGrpcService>.Instance,
                dictionaryProvider: null,
                replicationContext: new EnrollAllReplicationContext(),
                options: null,
                topology: topology);
        }
    }

    private static ReplicationBatchEnvelopeBox Box() => new()
    {
        Value = new ReplicationBatchEnvelope
        {
            TreeName = Tree,
            OriginClusterId = Origin,
            Entries =
            [
                new WalRecord
                {
                    TreeId = Tree,
                    Op = MutationKind.Set,
                    Key = "k",
                    Value = [1],
                    Timestamp = new HybridLogicalClock { WallClockTicks = 10 },
                    OriginClusterId = Origin,
                },
            ],
        },
    };

    [Test]
    public async Task A_batch_stamped_with_the_drained_lineage_applies()
    {
        var h = new Harness(new ReplicationDrainedLineage(Drained, Epoch));

        var ack = await h.Service().Push(Box(), new CallContext(Origin, Drained.ToString("D")));

        Assert.Multiple(() =>
        {
            Assert.That(ack.Value.Accepted, Is.True);
            Assert.That(ack.Value.SourceLineageRefused, Is.False);
        });
        await h.Applier.Received(1).ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_batch_read_under_another_lineage_is_refused_and_never_applied()
    {
        var h = new Harness(new ReplicationDrainedLineage(Drained, Epoch));

        var ack = await h.Service().Push(Box(), new CallContext(Origin, Guid.NewGuid().ToString("D")));

        Assert.Multiple(() =>
        {
            Assert.That(ack.Value.Accepted, Is.False, "the sender's cursor must hold");
            Assert.That(ack.Value.SourceLineageRefused, Is.True, "the sender learns why, and re-resolves its binding");
            Assert.That(ack.Value.ReceiverLineage, Is.EqualTo(Epoch));
        });
        await h.Applier.DidNotReceive().ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_batch_arriving_after_the_tree_was_replaced_since_its_drain_is_refused()
    {
        var h = new Harness(new ReplicationDrainedLineage(Drained, Guid.NewGuid()));

        var ack = await h.Service().Push(Box(), new CallContext(Origin, Drained.ToString("D")));

        Assert.That(ack.Value.SourceLineageRefused, Is.True,
            "the tree frontier re-minted its epoch since the drain, so the drain no longer describes the tree");
        await h.Applier.DidNotReceive().ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>());
    }

    [TestCase("not-a-guid")]
    [TestCase("{11111111-2222-4333-8444-555555555555}")]
    public async Task A_malformed_lineage_header_vouches_for_no_lineage_and_is_refused(string header)
    {
        var h = new Harness(new ReplicationDrainedLineage(Drained, Epoch));

        var ack = await h.Service().Push(Box(), new CallContext(Origin, header));

        Assert.That(ack.Value.Accepted, Is.False);
        await h.Applier.DidNotReceive().ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_push_without_the_header_applies_as_before_and_reads_no_drained_lineage()
    {
        var h = new Harness(new ReplicationDrainedLineage(Drained, Epoch));

        var ack = await h.Service().Push(Box(), new CallContext(Origin, lineage: null));

        Assert.That(ack.Value.Accepted, Is.True);
        await h.Coordinator.DidNotReceive().GetDrainedLineageAsync(Arg.Any<string>());
    }

    [Test]
    public async Task A_lineage_header_on_a_call_whose_origin_is_not_authenticated_is_never_read()
    {
        var h = new Harness(new ReplicationDrainedLineage(Drained, Epoch));

        Assert.ThrowsAsync<RpcException>(async () =>
            await h.Service().Push(Box(), new CallContext("someone-else", Drained.ToString("D"))));
        await h.Coordinator.DidNotReceive().GetDrainedLineageAsync(Arg.Any<string>());
    }

    [Test]
    public void The_lineage_header_name_is_stable()
    {
        Assert.That(LatticeReplicationGrpcMetadataNames.SourceLineageHeader, Is.EqualTo("x-lattice-replication-source-lineage"));
    }

    private sealed class CallContext(string? stampedOrigin, string? lineage) : ServerCallContext
    {
        protected override string MethodCore => "Push";
        protected override string HostCore => string.Empty;
        protected override string PeerCore => string.Empty;
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override global::Grpc.Core.Metadata RequestHeadersCore { get; } = Headers(stampedOrigin, lineage);
        protected override CancellationToken CancellationTokenCore => CancellationToken.None;
        protected override global::Grpc.Core.Metadata ResponseTrailersCore { get; } = new();
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => new(string.Empty, new Dictionary<string, List<AuthProperty>>());
        protected override IDictionary<object, object> UserStateCore { get; } = new Dictionary<object, object>();
        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options)
            => throw new NotSupportedException();
        protected override Task WriteResponseHeadersAsyncCore(global::Grpc.Core.Metadata responseHeaders) => Task.CompletedTask;

        private static global::Grpc.Core.Metadata Headers(string? stampedOrigin, string? lineage)
        {
            var headers = new global::Grpc.Core.Metadata();
            if (stampedOrigin is not null)
            {
                headers.Add(LatticeReplicationGrpcMetadataNames.OriginClusterIdHeader, stampedOrigin);
            }

            if (lineage is not null)
            {
                headers.Add(LatticeReplicationGrpcMetadataNames.SourceLineageHeader, lineage);
            }

            return headers;
        }
    }
}
