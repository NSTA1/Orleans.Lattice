using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// Issue #4586 part 2b: the receiver reads the sender's applied low watermark
/// from the source-frontier call header only after the caller's origin is
/// authenticated, only for an enrolled tree and a configured peer, and parses it
/// strictly; anything else vouches for nothing. Every ack reports the tree's
/// frontier epoch, and a frontier that cannot be reached reports none.
/// </summary>
[TestFixture]
public class LatticeReplicationGrpcServiceSourceFrontierTests
{
    private const string Tree = "tree";
    private const string Origin = "remote";

    private static readonly ReplicationSourceFrontier Frontier = new()
    {
        ReceiverLineage = Guid.Parse("0f8fad5b-d9cb-469f-a165-70867728950e"),
        TreeLowWatermark = new HybridLogicalClock { WallClockTicks = 500 },
        OriginLowWatermark = new HybridLogicalClock { WallClockTicks = 400 },
        OriginGeneration = 3,
    };

    private static readonly Guid Epoch = Guid.Parse("9a8b7c6d-5e4f-4a3b-9c2d-1e0f9a8b7c6d");

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
        public IReplicationTreeFrontierGrain TreeFrontier { get; } = Substitute.For<IReplicationTreeFrontierGrain>();
        public IReplicationApplier Applier { get; } = Substitute.For<IReplicationApplier>();

        public Harness()
        {
            Factory.GetGrain<IReplicationTreeFrontierGrain>(Tree, Arg.Any<string?>()).Returns(TreeFrontier);
            TreeFrontier.ObserveAsync(Origin, Arg.Any<ReplicationSourceFrontier?>(), Arg.Any<CancellationToken>()).Returns(Epoch);
            Applier.ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
                .Returns(Task.FromResult(new ApplyResult { Applied = true, HighWaterMark = HybridLogicalClock.Zero }));
        }

        public LatticeReplicationGrpcService Service(string[]? peers = null, bool enrolled = true, bool withTopology = true)
        {
            var sp = new ServiceCollection().AddSerializer().BuildServiceProvider();
            var encoder = new TestEncoder(sp.GetRequiredService<Serializer<ReplicationBatchEnvelope>>());
            var method = GrpcTestFactories.CreateMethod(encoder, sp.GetRequiredService<Serializer<ReplicationAck>>());
            var registry = Substitute.For<IWalCursorRegistry>();
            registry.GetBlockedFloorAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                .Returns(Task.FromResult<HybridLogicalClock?>(null));
            IReplicationTopology? topology = null;
            if (withTopology)
            {
                topology = Substitute.For<IReplicationTopology>();
                topology.CurrentPeers.Returns(peers ?? [Origin]);
            }

            ILatticeReplicationContext context = enrolled
                ? new EnrollAllReplicationContext()
                : Substitute.For<ILatticeReplicationContext>();
            return new LatticeReplicationGrpcService(
                method,
                Applier,
                registry,
                NoOpReceiverFlowControlPolicy.Instance,
                Factory,
                new ReceiverAppliedContentIndex(),
                NullLogger<LatticeReplicationGrpcService>.Instance,
                dictionaryProvider: null,
                replicationContext: context,
                options: null,
                topology: topology);
        }
    }

    private static ReplicationBatchEnvelopeBox EmptyBox() => new()
    {
        Value = new ReplicationBatchEnvelope { TreeName = Tree, OriginClusterId = Origin, Entries = [] },
    };

    [Test]
    public async Task A_configured_peers_frontier_is_recorded_and_the_ack_reports_the_epoch()
    {
        var h = new Harness();

        var ack = await h.Service().Push(EmptyBox(), new CallContext(Origin, Frontier.ToText()));

        Assert.That(ack.Value.ReceiverLineage, Is.EqualTo(Epoch));
        await h.TreeFrontier.Received(1).ObserveAsync(Origin, Frontier, Arg.Any<CancellationToken>());
    }

    private static readonly ReplicationAckedPositions Acked = new()
    {
        PhysicalTreeId = "tree-physical",
        Positions = [3, 0, 7],
    };

    [Test]
    public async Task Acked_positions_beside_a_valid_frontier_are_recorded_with_it()
    {
        // Issue #4684: the positions ride on the frontier the receiver records.
        var h = new Harness();

        await h.Service().Push(EmptyBox(), new CallContext(Origin, Frontier.ToText(), Acked.ToText()));

        await h.TreeFrontier.Received(1).ObserveAsync(
            Origin,
            Arg.Is<ReplicationSourceFrontier?>(f => f.HasValue && f.Value.AckedPositions != null
                && f.Value.AckedPositions.PhysicalTreeId == Acked.PhysicalTreeId
                && f.Value.AckedPositions.Positions.SequenceEqual(Acked.Positions)),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Malformed_acked_positions_vouch_none_but_keep_the_watermark()
    {
        var h = new Harness();

        await h.Service().Push(EmptyBox(), new CallContext(Origin, Frontier.ToText(), "1|not-base64!|3"));

        await h.TreeFrontier.Received(1).ObserveAsync(Origin, Frontier, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Acked_positions_without_a_valid_frontier_are_never_read()
    {
        var h = new Harness();

        await h.Service().Push(EmptyBox(), new CallContext(Origin, Frontier.ToText() + ".9", Acked.ToText()));
        await h.Service(peers: ["someone-else"]).Push(EmptyBox(), new CallContext(Origin, Frontier.ToText(), Acked.ToText()));

        await h.TreeFrontier.Received(2).ObserveAsync(Origin, null, Arg.Any<CancellationToken>());
    }

    [TestCase(null)]
    [TestCase("someone-else")]
    public async Task Acked_positions_on_a_call_whose_origin_is_not_authenticated_as_the_body_origin_are_never_read(string? stampedOrigin)
    {
        // Issue #4684: a peer cannot vouch positions for another origin's
        // shipper. The push is refused before any header is read.
        var h = new Harness();

        var refused = Assert.ThrowsAsync<RpcException>(
            async () => await h.Service().Push(EmptyBox(), new CallContext(stampedOrigin, Frontier.ToText(), Acked.ToText())));

        Assert.That(refused!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
        await h.TreeFrontier.DidNotReceiveWithAnyArgs().ObserveAsync(default!, default, default);
    }

    [Test]
    public async Task Oversized_acked_positions_vouch_none_but_keep_the_watermark()
    {
        var h = new Harness();
        var oversized = "1|dHJlZQ==|" + string.Join(',', Enumerable.Repeat("1", ReplicationAckedPositions.MaxPartitions + 1));

        await h.Service().Push(EmptyBox(), new CallContext(Origin, Frontier.ToText(), oversized));

        await h.TreeFrontier.Received(1).ObserveAsync(Origin, Frontier, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_frontier_from_a_peer_that_is_not_configured_is_ignored()
    {
        var h = new Harness();

        var ack = await h.Service(peers: ["someone-else"]).Push(EmptyBox(), new CallContext(Origin, Frontier.ToText()));

        Assert.That(ack.Value.ReceiverLineage, Is.EqualTo(Epoch));
        await h.TreeFrontier.Received(1).ObserveAsync(Origin, null, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Without_a_topology_no_frontier_is_accepted()
    {
        var h = new Harness();

        await h.Service(withTopology: false).Push(EmptyBox(), new CallContext(Origin, Frontier.ToText()));

        await h.TreeFrontier.Received(1).ObserveAsync(Origin, null, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_malformed_frontier_vouches_for_nothing()
    {
        var h = new Harness();

        await h.Service().Push(EmptyBox(), new CallContext(Origin, Frontier.ToText() + ".9"));

        await h.TreeFrontier.Received(1).ObserveAsync(Origin, null, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_frontier_on_a_call_whose_origin_is_not_authenticated_is_never_read()
    {
        var h = new Harness();

        var refused = Assert.ThrowsAsync<RpcException>(
            async () => await h.Service().Push(EmptyBox(), new CallContext(stampedOrigin: null, Frontier.ToText())));

        Assert.That(refused!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
        await h.TreeFrontier.DidNotReceiveWithAnyArgs().ObserveAsync(default!, default, default);
    }

    [Test]
    public async Task A_frontier_for_a_tree_not_enrolled_here_creates_no_frontier_and_reports_none()
    {
        var h = new Harness();

        var ack = await h.Service(enrolled: false).Push(EmptyBox(), new CallContext(Origin, Frontier.ToText()));

        Assert.That(ack.Value.ReceiverLineage, Is.Null);
        h.Factory.DidNotReceive().GetGrain<IReplicationTreeFrontierGrain>(Arg.Any<string>(), Arg.Any<string?>());
    }

    [Test]
    public async Task A_frontier_that_cannot_be_reached_reports_no_epoch_and_the_batch_still_applies()
    {
        var h = new Harness();
        h.TreeFrontier.ObserveAsync(Origin, Arg.Any<ReplicationSourceFrontier?>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("frontier down"));

        var ack = await h.Service().Push(EmptyBox(), new CallContext(Origin, Frontier.ToText()));

        Assert.Multiple(() =>
        {
            Assert.That(ack.Value.Accepted, Is.True);
            Assert.That(ack.Value.ReceiverLineage, Is.Null, "null is never read as a lineage change");
        });
    }

    [Test]
    public async Task A_deferred_ack_also_reports_the_epoch()
    {
        var h = new Harness();
        h.Applier.ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new ApplyResult { Deferred = true, HighWaterMark = HybridLogicalClock.Zero }));

        var ack = await h.Service().Push(EmptyBox(), new CallContext(Origin, Frontier.ToText()));

        Assert.Multiple(() =>
        {
            Assert.That(ack.Value.Accepted, Is.False);
            Assert.That(ack.Value.ReceiverLineage, Is.EqualTo(Epoch));
        });
    }

    private sealed class CallContext(string? stampedOrigin, string? frontier, string? ackedPositions = null) : ServerCallContext
    {
        protected override string MethodCore => "Push";
        protected override string HostCore => string.Empty;
        protected override string PeerCore => string.Empty;
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override global::Grpc.Core.Metadata RequestHeadersCore { get; } = Headers(stampedOrigin, frontier, ackedPositions);
        protected override CancellationToken CancellationTokenCore => CancellationToken.None;
        protected override global::Grpc.Core.Metadata ResponseTrailersCore { get; } = new();
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => new(string.Empty, new Dictionary<string, List<AuthProperty>>());
        protected override IDictionary<object, object> UserStateCore { get; } = new Dictionary<object, object>();
        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options)
            => throw new NotSupportedException();
        protected override Task WriteResponseHeadersAsyncCore(global::Grpc.Core.Metadata responseHeaders) => Task.CompletedTask;

        private static global::Grpc.Core.Metadata Headers(string? stampedOrigin, string? frontier, string? ackedPositions)
        {
            var headers = new global::Grpc.Core.Metadata();
            if (stampedOrigin is not null)
            {
                headers.Add(LatticeReplicationGrpcMetadataNames.OriginClusterIdHeader, stampedOrigin);
            }

            if (frontier is not null)
            {
                headers.Add(LatticeReplicationGrpcMetadataNames.SourceFrontierHeader, frontier);
            }

            if (ackedPositions is not null)
            {
                headers.Add(LatticeReplicationGrpcMetadataNames.AckedPositionsHeader, ackedPositions);
            }

            return headers;
        }
    }
}
