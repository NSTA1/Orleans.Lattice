using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// Regression coverage for the two peer-facing authorization gates on the
/// replication gRPC surface.
/// <para>
/// <b>Tree-scope gate.</b> <c>ProbeDigest</c>, <c>ProbeMerkleWalk</c>,
/// <c>ExchangeContentManifest</c> and <c>GetPeerHighWaterMark</c> resolve the
/// wire-supplied <c>TreeName</c> to a local grain and read it inside a
/// deliberate <c>EnterSystemOrigin</c> access-gate bypass. That bypass is only
/// sound under the precondition <c>ReplicationSystemOriginDigestReader</c>
/// documents - that the tree id was resolved against local state and never
/// taken from the wire. Before this gate, none of these RPCs met it, so a peer
/// that cleared the shared-secret interceptor could aim them at any tree on the
/// silo (including the never-enrolled <c>sys-</c> trees) and use the returned
/// entry counts and range digests as a bisectable key-existence and
/// key-distribution oracle.
/// </para>
/// <para>
/// <b>Origin binding.</b> <c>ExchangeContentManifest</c> performs a durable
/// <c>TryAdvanceAsync</c> on the per-origin high-water mark keyed on a
/// wire-supplied <c>OriginClusterId</c> that was never bound to the caller, so
/// a peer could name a third cluster's origin and advance that stream's clock,
/// suppressing anti-entropy repair for it. The gate rejects a declared origin
/// that disagrees with the stamped origin header.
/// </para>
/// </summary>
[TestFixture]
public class LatticeReplicationGrpcServiceEnrollmentTests
{
    private const string EnrolledTree = "enrolled-tree";
    private const string UnenrolledTree = "sys-auth-policy";

    private sealed class MapReplicationContext(IReadOnlyDictionary<string, LatticeMergeMode> modes)
        : ILatticeReplicationContext
    {
        public bool IsReplicationEnabled => true;

        public string LocalReplicaId => "local";

        public LatticeMergeMode? ResolveMergeMode(string treeId) =>
            modes.TryGetValue(treeId, out var mode) ? mode : null;
    }

    private static ILatticeReplicationContext EnrolledOnly() =>
        new MapReplicationContext(new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal)
        {
            [EnrolledTree] = LatticeMergeMode.LwwRegister,
        });

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
        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) => null!;
        protected override Task WriteResponseHeadersAsyncCore(global::Grpc.Core.Metadata responseHeaders) => Task.CompletedTask;
    }

    private static ServerCallContext NoHeaders() => new HeaderServerCallContext(new global::Grpc.Core.Metadata());

    private static ServerCallContext WithOriginHeader(string origin)
    {
        var headers = new global::Grpc.Core.Metadata
        {
            { LatticeReplicationGrpcMetadataNames.OriginClusterIdHeader, origin },
        };
        return new HeaderServerCallContext(headers);
    }

    private static LatticeReplicationGrpcService CreateService(
        IGrainFactory grainFactory,
        ILatticeReplicationContext? replicationContext)
        => CreateService(grainFactory, replicationContext, Substitute.For<IReplicationApplier>());

    private static LatticeReplicationGrpcService CreateService(
        IGrainFactory grainFactory,
        ILatticeReplicationContext? replicationContext,
        IReplicationApplier applier)
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
            applier,
            new InMemoryWalCursorRegistry(),
            NoOpReceiverFlowControlPolicy.Instance,
            grainFactory,
            new ReceiverAppliedContentIndex(),
            NullLogger<LatticeReplicationGrpcService>.Instance,
            dictionaryProvider: null,
            replicationContext: replicationContext);
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

    private static void AssertPermissionDenied(Func<Task> action) =>
        Assert.That(
            action,
            Throws.TypeOf<RpcException>()
                .With.Property("StatusCode").EqualTo(StatusCode.PermissionDenied));

    [Test]
    public void ProbeDigest_refuses_a_tree_that_is_not_enrolled_for_replication()
    {
        var factory = FactoryWithDigest(UnenrolledTree);
        var svc = CreateService(factory, EnrolledOnly());
        var box = new DigestProbeRequestBox
        {
            Value = new DigestProbeRequest { TreeName = UnenrolledTree, ShardIndex = 0 },
        };

        AssertPermissionDenied(async () => await svc.ProbeDigest(box, NoHeaders()));
        factory.DidNotReceive().GetGrain<ILattice>(UnenrolledTree);
    }

    [Test]
    public async Task ProbeDigest_still_answers_for_an_enrolled_tree()
    {
        var svc = CreateService(FactoryWithDigest(EnrolledTree), EnrolledOnly());
        var box = new DigestProbeRequestBox
        {
            Value = new DigestProbeRequest { TreeName = EnrolledTree, ShardIndex = 0 },
        };

        var response = await svc.ProbeDigest(box, NoHeaders());

        Assert.That(response.Value.DigestAvailable, Is.True);
    }

    [Test]
    public void ProbeDigest_fails_closed_when_no_enrollment_source_is_available()
    {
        var svc = CreateService(FactoryWithDigest(EnrolledTree), replicationContext: null);
        var box = new DigestProbeRequestBox
        {
            Value = new DigestProbeRequest { TreeName = EnrolledTree, ShardIndex = 0 },
        };

        AssertPermissionDenied(async () => await svc.ProbeDigest(box, NoHeaders()));
    }

    [Test]
    public void ProbeMerkleWalk_refuses_a_tree_that_is_not_enrolled_for_replication()
    {
        var factory = Substitute.For<IGrainFactory>();
        var svc = CreateService(factory, EnrolledOnly());
        var box = new MerkleWalkProbeRequestBox
        {
            Value = new MerkleWalkProbeRequest
            {
                TreeName = UnenrolledTree,
                ShardIndex = 0,
                RangeStartKey = "a",
                RangeEndKey = "z",
                Depth = 1,
            },
        };

        AssertPermissionDenied(async () => await svc.ProbeMerkleWalk(box, NoHeaders()));
        factory.DidNotReceive().GetGrain<ILattice>(UnenrolledTree);
    }

    [Test]
    public void GetPeerHighWaterMark_refuses_a_tree_that_is_not_enrolled_for_replication()
    {
        var factory = Substitute.For<IGrainFactory>();
        var svc = CreateService(factory, EnrolledOnly());
        var box = new PeerHighWaterMarkRequestBox
        {
            Value = new PeerHighWaterMarkRequest
            {
                TreeName = UnenrolledTree,
                OriginClusterId = "site-a",
            },
        };

        AssertPermissionDenied(async () => await svc.GetPeerHighWaterMark(box, NoHeaders()));
        factory.DidNotReceive().GetGrain<IReplicationHighWaterMarkGrain>(UnenrolledTree);
    }

    [Test]
    public void ExchangeContentManifest_refuses_a_tree_that_is_not_enrolled_for_replication()
    {
        var factory = Substitute.For<IGrainFactory>();
        var svc = CreateService(factory, EnrolledOnly());
        var box = new ContentManifestRequestBox
        {
            Value = new ContentManifestRequest
            {
                TreeName = UnenrolledTree,
                OriginClusterId = "site-a",
                Entries = [],
            },
        };

        AssertPermissionDenied(async () => await svc.ExchangeContentManifest(box, NoHeaders()));
        factory.DidNotReceive().GetGrain<IReplicationHighWaterMarkGrain>(UnenrolledTree);
    }

    [Test]
    public void ExchangeContentManifest_refuses_an_origin_that_disagrees_with_the_stamped_header()
    {
        // The durable per-origin high-water mark is advanced under the body's
        // OriginClusterId. Letting a peer declare a third cluster's origin lets
        // it poison that stream's cursor and suppress anti-entropy repair.
        var factory = Substitute.For<IGrainFactory>();
        var hwmGrain = Substitute.For<IReplicationHighWaterMarkGrain>();
        factory.GetGrain<IReplicationHighWaterMarkGrain>(EnrolledTree).Returns(hwmGrain);
        var svc = CreateService(factory, EnrolledOnly());
        var box = new ContentManifestRequestBox
        {
            Value = new ContentManifestRequest
            {
                TreeName = EnrolledTree,
                OriginClusterId = "victim-site",
                Entries = [],
            },
        };

        AssertPermissionDenied(
            async () => await svc.ExchangeContentManifest(box, WithOriginHeader("attacker-site")));
    }

    [Test]
    public async Task ExchangeContentManifest_accepts_an_origin_that_matches_the_stamped_header()
    {
        var factory = Substitute.For<IGrainFactory>();
        var hwmGrain = Substitute.For<IReplicationHighWaterMarkGrain>();
        hwmGrain.GetAsync("site-a", Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(HybridLogicalClock.Zero));
        factory.GetGrain<IReplicationHighWaterMarkGrain>(EnrolledTree).Returns(hwmGrain);
        var svc = CreateService(factory, EnrolledOnly());
        var box = new ContentManifestRequestBox
        {
            Value = new ContentManifestRequest
            {
                TreeName = EnrolledTree,
                OriginClusterId = "site-a",
                Entries = [],
            },
        };

        var response = await svc.ExchangeContentManifest(box, WithOriginHeader("site-a"));

        Assert.That(response.Value.ExchangeSupported, Is.True);
    }

    [Test]
    public void ExchangeContentManifest_refuses_a_call_with_no_stamped_origin_header()
    {
        // Regression: the gate was absent-tolerant, so omitting the header
        // bypassed the origin binding entirely. Omitting a header is strictly
        // easier than forging one, so a tolerated absence was not a weaker
        // gate but no gate at all - a peer could enumerate a third cluster's
        // replication cursor simply by not stamping itself.
        var factory = Substitute.For<IGrainFactory>();
        var hwmGrain = Substitute.For<IReplicationHighWaterMarkGrain>();
        hwmGrain.GetAsync("site-a", Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(HybridLogicalClock.Zero));
        factory.GetGrain<IReplicationHighWaterMarkGrain>(EnrolledTree).Returns(hwmGrain);
        var svc = CreateService(factory, EnrolledOnly());
        var box = new ContentManifestRequestBox
        {
            Value = new ContentManifestRequest
            {
                TreeName = EnrolledTree,
                OriginClusterId = "site-a",
                Entries = [],
            },
        };

        var rpc = Assert.ThrowsAsync<RpcException>(async () =>
            await svc.ExchangeContentManifest(box, NoHeaders()));

        Assert.That(rpc!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public void Push_refuses_an_origin_that_disagrees_with_the_stamped_header()
    {
        // Push advances the same durable per-origin high-water mark that
        // ExchangeContentManifest does, but was the one verb of the family that
        // never bound its body-declared origin to the caller. A peer could push a
        // batch under a third cluster's origin and drive that stream's recorded
        // clock forward, which suppresses anti-entropy repair for it: the poisoned
        // value is served back as the re-replay cursor, so a maxed cursor makes
        // every WAL entry ineligible for re-replay.
        var svc = CreateService(Substitute.For<IGrainFactory>(), EnrolledOnly());
        var box = new ReplicationBatchEnvelopeBox
        {
            Value = new ReplicationBatchEnvelope
            {
                TreeName = EnrolledTree,
                OriginClusterId = "victim-site",
                Entries = [],
            },
        };

        AssertPermissionDenied(async () => await svc.Push(box, WithOriginHeader("attacker-site")));
    }

    [Test]
    public void Push_refusal_happens_before_the_batch_is_applied()
    {
        // The gate has to precede the applier, not merely accompany it: an apply
        // that has already run cannot be un-run by a later refusal.
        var applier = Substitute.For<IReplicationApplier>();
        var svc = CreateService(Substitute.For<IGrainFactory>(), EnrolledOnly(), applier);
        var box = new ReplicationBatchEnvelopeBox
        {
            Value = new ReplicationBatchEnvelope
            {
                TreeName = EnrolledTree,
                OriginClusterId = "victim-site",
                Entries = [],
            },
        };

        AssertPermissionDenied(async () => await svc.Push(box, WithOriginHeader("attacker-site")));

        applier.DidNotReceive().ApplyBatchAsync(
            Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void GetPeerHighWaterMark_refuses_an_origin_that_disagrees_with_the_stamped_header()
    {
        // The cursor is per-origin state; reading it back for an origin other than
        // the caller's own discloses a third cluster's replication position.
        var factory = Substitute.For<IGrainFactory>();
        var hwmGrain = Substitute.For<IReplicationHighWaterMarkGrain>();
        factory.GetGrain<IReplicationHighWaterMarkGrain>(EnrolledTree).Returns(hwmGrain);
        var svc = CreateService(factory, EnrolledOnly());
        var box = new PeerHighWaterMarkRequestBox
        {
            Value = new PeerHighWaterMarkRequest
            {
                TreeName = EnrolledTree,
                OriginClusterId = "victim-site",
            },
        };

        AssertPermissionDenied(
            async () => await svc.GetPeerHighWaterMark(box, WithOriginHeader("attacker-site")));
        hwmGrain.DidNotReceive().GetAsync("victim-site", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetPeerHighWaterMark_accepts_an_origin_that_matches_the_stamped_header()
    {
        var factory = Substitute.For<IGrainFactory>();
        var hwmGrain = Substitute.For<IReplicationHighWaterMarkGrain>();
        hwmGrain.GetAsync("site-a", Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(HybridLogicalClock.Zero));
        factory.GetGrain<IReplicationHighWaterMarkGrain>(EnrolledTree).Returns(hwmGrain);
        var svc = CreateService(factory, EnrolledOnly());
        var box = new PeerHighWaterMarkRequestBox
        {
            Value = new PeerHighWaterMarkRequest
            {
                TreeName = EnrolledTree,
                OriginClusterId = "site-a",
            },
        };

        var response = await svc.GetPeerHighWaterMark(box, WithOriginHeader("site-a"));

        Assert.That(response.Value.Clock, Is.EqualTo(HybridLogicalClock.Zero));
    }

    [Test]
    public void GetPeerHighWaterMark_refuses_a_call_with_no_stamped_origin_header()
    {
        // Same fail-closed posture as the manifest exchange: an unstamped call
        // carries no authenticated origin, so it may not read back a
        // per-origin replication cursor it merely names.
        var factory = Substitute.For<IGrainFactory>();
        var hwmGrain = Substitute.For<IReplicationHighWaterMarkGrain>();
        hwmGrain.GetAsync("site-a", Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(HybridLogicalClock.Zero));
        factory.GetGrain<IReplicationHighWaterMarkGrain>(EnrolledTree).Returns(hwmGrain);
        var svc = CreateService(factory, EnrolledOnly());
        var box = new PeerHighWaterMarkRequestBox
        {
            Value = new PeerHighWaterMarkRequest
            {
                TreeName = EnrolledTree,
                OriginClusterId = "site-a",
            },
        };

        var rpc = Assert.ThrowsAsync<RpcException>(async () =>
            await svc.GetPeerHighWaterMark(box, NoHeaders()));

        Assert.That(rpc!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }
}
