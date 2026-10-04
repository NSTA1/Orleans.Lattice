using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;
using Orleans.Serialization;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// #4585 on a real receiver tree: content-hash elision must elide only a write
/// the receiver provably still reflects. Equal bytes are not enough - a
/// last-writer-wins replica orders writes by version - so the same value at a
/// newer version, a value recorded from a merge that lost, and a value whose
/// leaf was lowered behind the applier's back (a purge and recreate) must all
/// ship. Each test drives the real <see cref="ReplicationApplier"/>, which
/// records into the applied-content index, and then the real exchange handler
/// against the same index and the same silo.
/// </summary>
[TestFixture]
[Category("Integration")]
public class ContentManifestElisionIntegrationTests
{
    private const string Receiver = "site-c";
    private const string OriginA = "site-a";
    private const string OriginB = "site-b";

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<ReceiverSiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    private sealed class ReceiverSiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(opts => opts.ClusterId = Receiver);
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, AllLwwResolver>();
        }
    }

    private sealed class AllLwwResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }

    private sealed class CallContext(string origin) : ServerCallContext
    {
        protected override string MethodCore => "ExchangeContentManifest";
        protected override string HostCore => string.Empty;
        protected override string PeerCore => string.Empty;
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override global::Grpc.Core.Metadata RequestHeadersCore =>
            new() { { LatticeReplicationGrpcMetadataNames.OriginClusterIdHeader, origin } };
        protected override CancellationToken CancellationTokenCore => CancellationToken.None;
        protected override global::Grpc.Core.Metadata ResponseTrailersCore => new();
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => null!;
        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) => null!;
        protected override Task WriteResponseHeadersAsyncCore(global::Grpc.Core.Metadata responseHeaders) => Task.CompletedTask;
    }

    private (ReplicationApplier Applier, LatticeReplicationGrpcService Service) CreateReceiver()
    {
        var options = new LatticeReplicationOptions { ClusterId = Receiver, ContentHashDedupEnabled = true };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);

        var index = new ReceiverAppliedContentIndex();
        var applier = new ReplicationApplier(
            _cluster.Client,
            monitor,
            appliedContentIndex: index,
            replicationContext: new EnrollAllReplicationContext());

        var sp = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var method = new LatticeReplicationGrpcMethod(
            Substitute.For<IReplicationBatchEncoder>(),
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
        var service = new LatticeReplicationGrpcService(
            method,
            applier,
            new InMemoryWalCursorRegistry(),
            NoOpReceiverFlowControlPolicy.Instance,
            _cluster.Client,
            index,
            NullLogger<LatticeReplicationGrpcService>.Instance,
            dictionaryProvider: null,
            replicationContext: new EnrollAllReplicationContext());
        return (applier, service);
    }

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    private static WalRecord Set(string tree, string key, byte[] value, long ticks, string origin) => new()
    {
        TreeId = tree,
        Op = MutationKind.Set,
        Key = key,
        Value = value,
        Timestamp = Hlc(ticks),
        OriginClusterId = origin,
        Mode = LatticeMergeMode.LwwRegister,
    };

    /// <summary>Offers <paramref name="offered"/> in a one-entry manifest from its origin; returns whether the receiver asked for it.</summary>
    private static async Task<bool> ReceiverAsksForAsync(LatticeReplicationGrpcService service, WalRecord offered)
    {
        var manifest = ContentManifestPlanner.BuildManifest([offered]);
        Assert.That(manifest, Has.Count.EqualTo(1), "The offered write is elision-eligible.");
        var response = await service.ExchangeContentManifest(
            new ContentManifestRequestBox
            {
                Value = new ContentManifestRequest
                {
                    TreeName = offered.TreeId,
                    OriginClusterId = offered.OriginClusterId!,
                    Entries = manifest,
                },
            },
            new CallContext(offered.OriginClusterId!));
        return response.Value.MissingEntryIndices.Contains(0);
    }

    [Test]
    public async Task A_value_recorded_from_a_losing_merge_does_not_elide_the_same_value_at_a_newer_version()
    {
        // Trace 1 of #4585.
        const string tree = "elide-losing-merge";
        var (applier, service) = CreateReceiver();
        var lattice = _cluster.Client.GetGrain<ILattice>(tree);
        byte[] v = [1, 1], w = [2, 2];

        await applier.ApplyAsync(Set(tree, "k", w, 9, OriginB));
        await applier.ApplyAsync(Set(tree, "k", v, 3, OriginA)); // loses at the leaf
        Assert.That((await lattice.GetWithVersionAsync("k")).Value, Is.EqualTo(w));

        var newer = Set(tree, "k", v, 10, OriginA);
        Assert.That(await ReceiverAsksForAsync(service, newer), Is.True,
            "V at 10 is a write the receiver does not hold; eliding it leaves W at 9 here for good.");

        await applier.ApplyAsync(newer);
        Assert.That((await lattice.GetWithVersionAsync("k")).Value, Is.EqualTo(v));
    }

    [Test]
    public async Task The_same_bytes_at_a_newer_version_from_another_origin_are_not_elided()
    {
        // Trace 2 of #4585.
        const string tree = "elide-newer-version";
        var (applier, service) = CreateReceiver();
        byte[] v = [3, 3];

        await applier.ApplyAsync(Set(tree, "k", v, 5, OriginB));

        Assert.That(await ReceiverAsksForAsync(service, Set(tree, "k", v, 7, OriginA)), Is.True,
            "V at 7 outranks a concurrent W at 6, so dropping it would let W win here alone.");
    }

    [Test]
    public async Task A_purge_and_recreate_behind_the_index_does_not_elide_a_recorded_write()
    {
        // #4585 condition (c): anything that lowers the leaf without a
        // tombstone must stop the index eliding the write it recorded.
        const string tree = "elide-after-purge";
        var (applier, service) = CreateReceiver();
        var lattice = _cluster.Client.GetGrain<ILattice>(tree);
        var write = Set(tree, "k", [4, 4], 5, OriginA);
        await applier.ApplyAsync(write);

        await lattice.DeleteTreeAsync();
        await lattice.PurgeTreeAsync();
        await applier.ApplyAsync(Set(tree, "other", [5], 6, OriginA)); // recreates the tree id
        Assert.That((await lattice.GetWithVersionAsync("k")).Value, Is.Null, "The purge removed k.");

        Assert.That(await ReceiverAsksForAsync(service, write), Is.True,
            "The receiver no longer holds k; eliding the re-offer would lose it.");
    }

    [Test]
    public async Task A_write_the_receiver_still_holds_is_elided()
    {
        const string tree = "elide-held";
        var (applier, service) = CreateReceiver();
        var write = Set(tree, "k", [6, 6], 5, OriginA);
        await applier.ApplyAsync(write);

        Assert.That(await ReceiverAsksForAsync(service, write), Is.False, "An exact re-send of a held write is elided.");
    }
}
