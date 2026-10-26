using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication.Grains;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// Regression coverage for the tenant gate on the content-answering replication
/// RPCs. <c>ProbeDigest</c>, <c>ProbeMerkleWalk</c> and
/// <c>ExchangeContentManifest</c> checked only enrollment, so an authenticated
/// peer that the tenancy add-on refuses to export a tenant tree to could still
/// probe that tree's range digests and entry counts - a bisectable key-existence
/// oracle over the tenant's data - although the snapshot export path denies it.
/// A tenant tree now requires the same authorized, authenticated peer the export
/// path requires; platform and legacy trees are unchanged.
/// </summary>
[TestFixture]
public class LatticeReplicationGrpcServiceTenantGateTests
{
    private const string TenantTree = "t/acme/orders";
    private const string LegacyTree = "orders";
    private const string Peer = "site-b";

    private sealed class HeaderServerCallContext(global::Grpc.Core.Metadata headers) : ServerCallContext
    {
        protected override string MethodCore => "Probe";
        protected override string HostCore => string.Empty;
        protected override string PeerCore => string.Empty;
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override global::Grpc.Core.Metadata RequestHeadersCore => headers;
        protected override CancellationToken CancellationTokenCore => CancellationToken.None;
        protected override global::Grpc.Core.Metadata ResponseTrailersCore => new();
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => null!;
        protected override IDictionary<object, object> UserStateCore { get; } = new Dictionary<object, object>();
        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) => null!;
        protected override Task WriteResponseHeadersAsyncCore(global::Grpc.Core.Metadata responseHeaders) => Task.CompletedTask;
    }

    private static ServerCallContext Context(string? authenticatedPeer, string? originHeader = null)
    {
        var headers = new global::Grpc.Core.Metadata();
        if (originHeader is not null)
        {
            headers.Add(LatticeReplicationGrpcMetadataNames.OriginClusterIdHeader, originHeader);
        }

        var context = new HeaderServerCallContext(headers);
        if (authenticatedPeer is not null)
        {
            context.UserState[LatticeReplicationGrpcAuthInterceptor.AuthenticatedPeerKey] = authenticatedPeer;
        }

        return context;
    }

    private static IReplicationTenantIsolationGate Gate(bool authorizes)
    {
        var gate = Substitute.For<IReplicationTenantIsolationGate>();
        gate.IsActive.Returns(true);
        gate.AuthorizeExportAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ValueTask.FromResult(authorizes));
        return gate;
    }

    private static LatticeReplicationGrpcService CreateService(
        IGrainFactory grainFactory,
        IReplicationTenantIsolationGate? gate)
    {
        var sp = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var method = new LatticeReplicationGrpcMethod(
            new PassthroughEncoder(sp.GetRequiredService<Serializer<ReplicationBatchEnvelope>>()),
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
            replicationContext: new EnrollAllReplicationContext(),
            tenantIsolationGate: gate);
    }

    private sealed class PassthroughEncoder(Serializer<ReplicationBatchEnvelope> serializer)
        : IReplicationBatchEncoder
    {
        public string ContentType => "test/binary";
        public int CurrentWireVersion => 1;
        public void Encode(ReplicationBatchEnvelope envelope, System.Buffers.IBufferWriter<byte> writer)
            => serializer.Serialize(envelope, writer);
        public ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload)
            => serializer.Deserialize(payload.Span);
    }

    private static IGrainFactory FactoryWithDigest(string treeName)
    {
        var factory = Substitute.For<IGrainFactory>();
        var lattice = Substitute.For<ILattice>();
        lattice.GetLeafProjectionDigestAsync(Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new LeafProjectionDigest
            {
                Hash = [1, 2, 3],
                EntryCount = 7,
                CheckpointOffset = 42,
                Version = 3,
            }));
        factory.GetGrain<ILattice>(treeName).Returns(lattice);
        return factory;
    }

    private static DigestProbeRequestBox DigestProbe(string tree) => new()
    {
        Value = new DigestProbeRequest { TreeName = tree, ShardIndex = 0 },
    };

    private static void AssertPermissionDenied(Func<Task> action) =>
        Assert.That(
            action,
            Throws.TypeOf<RpcException>()
                .With.Property("StatusCode").EqualTo(StatusCode.PermissionDenied));

    [Test]
    public void ProbeDigest_on_a_tenant_tree_is_refused_when_no_tenant_gate_is_wired()
    {
        var factory = FactoryWithDigest(TenantTree);
        var svc = CreateService(factory, gate: null);

        AssertPermissionDenied(async () => await svc.ProbeDigest(DigestProbe(TenantTree), Context(Peer)));
        factory.DidNotReceive().GetGrain<ILattice>(TenantTree);
    }

    [Test]
    public void ProbeDigest_on_a_tenant_tree_is_refused_without_an_authenticated_peer()
    {
        var gate = Gate(authorizes: true);
        var factory = FactoryWithDigest(TenantTree);
        var svc = CreateService(factory, gate);

        AssertPermissionDenied(async () => await svc.ProbeDigest(DigestProbe(TenantTree), Context(authenticatedPeer: null)));
        factory.DidNotReceive().GetGrain<ILattice>(TenantTree);
    }

    [Test]
    public void ProbeDigest_on_a_tenant_tree_is_refused_when_the_gate_denies_the_peer()
    {
        var factory = FactoryWithDigest(TenantTree);
        var svc = CreateService(factory, Gate(authorizes: false));

        AssertPermissionDenied(async () => await svc.ProbeDigest(DigestProbe(TenantTree), Context(Peer)));
        factory.DidNotReceive().GetGrain<ILattice>(TenantTree);
    }

    [Test]
    public async Task ProbeDigest_on_a_tenant_tree_answers_when_the_gate_authorizes_the_peer()
    {
        var gate = Gate(authorizes: true);
        var svc = CreateService(FactoryWithDigest(TenantTree), gate);

        var response = await svc.ProbeDigest(DigestProbe(TenantTree), Context(Peer));

        Assert.That(response.Value.DigestAvailable, Is.True);
        await gate.Received(1).AuthorizeExportAsync(TenantTree, Peer, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ProbeDigest_on_a_legacy_tree_does_not_consult_the_tenant_gate()
    {
        var gate = Gate(authorizes: false);
        var svc = CreateService(FactoryWithDigest(LegacyTree), gate);

        var response = await svc.ProbeDigest(DigestProbe(LegacyTree), Context(authenticatedPeer: null));

        Assert.That(response.Value.DigestAvailable, Is.True);
        await gate.DidNotReceiveWithAnyArgs().AuthorizeExportAsync(default!, default!, default);
    }

    [Test]
    public void ProbeMerkleWalk_on_a_tenant_tree_is_refused_when_the_gate_denies_the_peer()
    {
        var factory = Substitute.For<IGrainFactory>();
        var svc = CreateService(factory, Gate(authorizes: false));
        var box = new MerkleWalkProbeRequestBox
        {
            Value = new MerkleWalkProbeRequest
            {
                TreeName = TenantTree,
                ShardIndex = 0,
                RangeStartKey = "a",
                RangeEndKey = "z",
                Depth = 1,
            },
        };

        AssertPermissionDenied(async () => await svc.ProbeMerkleWalk(box, Context(Peer)));
        factory.DidNotReceive().GetGrain<ILattice>(TenantTree);
    }

    [Test]
    public void ExchangeContentManifest_on_a_tenant_tree_is_refused_when_the_gate_denies_the_peer()
    {
        var factory = Substitute.For<IGrainFactory>();
        var svc = CreateService(factory, Gate(authorizes: false));
        var box = new ContentManifestRequestBox
        {
            Value = new ContentManifestRequest
            {
                TreeName = TenantTree,
                OriginClusterId = Peer,
                Entries = [],
            },
        };

        AssertPermissionDenied(async () => await svc.ExchangeContentManifest(box, Context(Peer, originHeader: Peer)));
        factory.DidNotReceive().GetGrain<IReplicationHighWaterMarkGrain>(TenantTree);
    }
}
