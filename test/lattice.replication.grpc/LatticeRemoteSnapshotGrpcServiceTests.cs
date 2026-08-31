using Grpc.Core;
using Orleans.Lattice.BPlusTree.Grains;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Tests;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// Unit tests for the server-side
/// <see cref="LatticeRemoteSnapshotGrpcService"/>. Validates the
/// request-validation surface that translates malformed receiver
/// requests into <see cref="StatusCode.InvalidArgument"/>, and
/// confirms the service threads payloads through to the underlying
/// <see cref="LatticeRemoteSnapshotService"/>.
/// </summary>
[TestFixture]
public class LatticeRemoteSnapshotGrpcServiceTests
{
    private const string Tree = "tree";
    private const string Source = "site-a";

    private static LatticeRemoteSnapshotGrpcMethods CreateMethods()
    {
        var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();
        services.AddSerializer();
        var sp = services.BuildServiceProvider();
        return new LatticeRemoteSnapshotGrpcMethods(
            sp.GetRequiredService<Serializer<RemoteSnapshotMetadataRequest>>(),
            sp.GetRequiredService<Serializer<RemoteSnapshotMetadata>>(),
            sp.GetRequiredService<Serializer<RemoteSnapshotStreamItem>>());
    }

    private static LatticeRemoteSnapshotGrpcService CreateService(StubSenderSnapshotProvider sender)
        => CreateService(sender, new AllEnrolledContext());

    /// <summary>
    /// Enrollment context reporting a merge mode for every tree, so the
    /// sender-side export gate admits the trees the pass-through tests use.
    /// </summary>
    private sealed class AllEnrolledContext : ILatticeReplicationContext
    {
        public bool IsReplicationEnabled => true;

        public string LocalReplicaId => Source;

        public LatticeMergeMode? ResolveMergeMode(string treeId) => LatticeMergeMode.LwwRegister;
    }

    private static LatticeRemoteSnapshotGrpcService CreateService(
        StubSenderSnapshotProvider sender,
        ILatticeReplicationContext replicationContext)
    {
        var inner = new LatticeRemoteSnapshotService(sender, replicationContext, NullLogger<LatticeRemoteSnapshotService>.Instance);
        return new LatticeRemoteSnapshotGrpcService(CreateMethods(), inner, NullLogger<LatticeRemoteSnapshotGrpcService>.Instance);
    }

    /// <summary>
    /// Enrollment context that reports a merge mode for no tree at all, so the
    /// sender-side export gate refuses every tree. Used to prove the refusal
    /// reaches the wire as <see cref="StatusCode.PermissionDenied"/> rather
    /// than being flattened into <see cref="StatusCode.Internal"/> by the
    /// generic exception arm.
    /// </summary>
    private sealed class NothingEnrolledContext : ILatticeReplicationContext
    {
        public bool IsReplicationEnabled => true;

        public string LocalReplicaId => Source;

        public LatticeMergeMode? ResolveMergeMode(string treeId) => null;
    }

    [Test]
    public void GetMetadata_maps_an_unenrolled_tree_to_permission_denied()
    {
        var sender = new StubSenderSnapshotProvider();
        var service = CreateService(sender, new NothingEnrolledContext());
        var context = new FakeServerCallContext();

        var rpc = Assert.ThrowsAsync<RpcException>(async () =>
            await service.GetMetadata(
                new RemoteSnapshotMetadataRequestBox { Value = new RemoteSnapshotMetadataRequest { TreeName = "sys-auth-policy", SourceClusterId = Source } },
                context));

        Assert.That(rpc!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public void RequestSnapshot_maps_an_unenrolled_tree_to_permission_denied()
    {
        var sender = new StubSenderSnapshotProvider();
        var service = CreateService(sender, new NothingEnrolledContext());
        var context = new FakeServerCallContext();
        var writer = new CapturingStreamWriter<RemoteSnapshotStreamItemBox>();

        var rpc = Assert.ThrowsAsync<RpcException>(async () =>
            await service.RequestSnapshot(
                new RemoteSnapshotMetadataRequestBox { Value = new RemoteSnapshotMetadataRequest { TreeName = "sys-auth-policy", SourceClusterId = Source } },
                writer,
                context));

        Assert.Multiple(() =>
        {
            Assert.That(rpc!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(writer.Written, Is.Empty);
        });
    }

    private sealed class CapturingStreamWriter<T> : IServerStreamWriter<T>
    {
        public List<T> Written { get; } = [];

        public WriteOptions? WriteOptions { get; set; }

        public Task WriteAsync(T message)
        {
            Written.Add(message);
            return Task.CompletedTask;
        }
    }

    private static async IAsyncEnumerable<SnapshotEntry> EmptyEntries()
    {
        await Task.CompletedTask;
        yield break;
    }

    private static async IAsyncEnumerable<SnapshotEntry> AsAsync(IEnumerable<SnapshotEntry> entries)
    {
        foreach (var e in entries)
        {
            yield return e;
        }
        await Task.CompletedTask;
    }

    [Test]
    public void GetMetadata_throws_invalid_argument_for_empty_tree_name()
    {
        var sender = new StubSenderSnapshotProvider();
        var service = CreateService(sender);
        var context = new FakeServerCallContext();

        var rpc = Assert.ThrowsAsync<RpcException>(async () =>
            await service.GetMetadata(
                new RemoteSnapshotMetadataRequestBox { Value = new RemoteSnapshotMetadataRequest { TreeName = "  ", SourceClusterId = Source } },
                context));

        Assert.That(rpc!.StatusCode, Is.EqualTo(StatusCode.InvalidArgument));
    }

    [Test]
    public void GetMetadata_throws_invalid_argument_for_empty_source_cluster_id()
    {
        var sender = new StubSenderSnapshotProvider();
        var service = CreateService(sender);
        var context = new FakeServerCallContext();

        var rpc = Assert.ThrowsAsync<RpcException>(async () =>
            await service.GetMetadata(
                new RemoteSnapshotMetadataRequestBox { Value = new RemoteSnapshotMetadataRequest { TreeName = Tree, SourceClusterId = " " } },
                context));

        Assert.That(rpc!.StatusCode, Is.EqualTo(StatusCode.InvalidArgument));
    }

    [Test]
    public async Task GetMetadata_returns_metadata_threaded_through_handler()
    {
        var sender = new StubSenderSnapshotProvider();
        var asOf = new HybridLogicalClock { WallClockTicks = 100, Counter = 0 };
        var frontier = new VersionVector();
        sender.Stage(Tree, new SnapshotStream(Tree, asOf, frontier, EmptyEntries()));

        var service = CreateService(sender);
        var response = await service.GetMetadata(
            new RemoteSnapshotMetadataRequestBox
            {
                Value = new RemoteSnapshotMetadataRequest
                {
                    TreeName = Tree,
                    SourceClusterId = Source,
                    FromAsOfHlc = HybridLogicalClock.Zero,
                },
            },
            new FakeServerCallContext());

        Assert.Multiple(() =>
        {
            Assert.That(response.Value.TreeName, Is.EqualTo(Tree));
            Assert.That(response.Value.SourceClusterId, Is.EqualTo(Source));
            Assert.That(response.Value.AsOfHlc, Is.EqualTo(asOf));
        });
    }

    [Test]
    public async Task RequestSnapshot_writes_each_entry_to_response_stream()
    {
        var sender = new StubSenderSnapshotProvider();
        var entries = new[]
        {
            new SnapshotEntry { Key = "a", Value = new byte[] { 1 }, Timestamp = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 } },
            new SnapshotEntry { Key = "b", Value = new byte[] { 2 }, Timestamp = new HybridLogicalClock { WallClockTicks = 2, Counter = 0 } },
        };
        sender.Stage(Tree, new SnapshotStream(Tree, new HybridLogicalClock { WallClockTicks = 2, Counter = 0 }, new VersionVector(), AsAsync(entries)));

        var service = CreateService(sender);
        var writer = new RecordingServerStreamWriter<RemoteSnapshotStreamItemBox>();

        await service.RequestSnapshot(
            new RemoteSnapshotMetadataRequestBox
            {
                Value = new RemoteSnapshotMetadataRequest
                {
                    TreeName = Tree,
                    SourceClusterId = Source,
                    FromAsOfHlc = HybridLogicalClock.Zero,
                },
            },
            writer,
            new FakeServerCallContext());

        Assert.Multiple(() =>
        {
            Assert.That(writer.Written.Count, Is.EqualTo(2));
            Assert.That(writer.Written[0].Value.Entry.Key, Is.EqualTo("a"));
            Assert.That(writer.Written[1].Value.Entry.Key, Is.EqualTo("b"));
        });
    }

    [Test]
    public void RequestSnapshot_throws_invalid_argument_for_empty_tree_name()
    {
        var sender = new StubSenderSnapshotProvider();
        var service = CreateService(sender);
        var writer = new RecordingServerStreamWriter<RemoteSnapshotStreamItemBox>();

        var rpc = Assert.ThrowsAsync<RpcException>(async () =>
            await service.RequestSnapshot(
                new RemoteSnapshotMetadataRequestBox { Value = new RemoteSnapshotMetadataRequest { TreeName = "  ", SourceClusterId = Source } },
                writer,
                new FakeServerCallContext()));

        Assert.That(rpc!.StatusCode, Is.EqualTo(StatusCode.InvalidArgument));
    }

    [Test]
    public void RequestSnapshot_throws_invalid_argument_for_empty_source_cluster_id()
    {
        var sender = new StubSenderSnapshotProvider();
        var service = CreateService(sender);
        var writer = new RecordingServerStreamWriter<RemoteSnapshotStreamItemBox>();

        var rpc = Assert.ThrowsAsync<RpcException>(async () =>
            await service.RequestSnapshot(
                new RemoteSnapshotMetadataRequestBox { Value = new RemoteSnapshotMetadataRequest { TreeName = Tree, SourceClusterId = "  " } },
                writer,
                new FakeServerCallContext()));

        Assert.That(rpc!.StatusCode, Is.EqualTo(StatusCode.InvalidArgument));
    }

    [Test]
    public void Constructor_throws_on_null_methods()
    {
        var sender = new StubSenderSnapshotProvider();
        var inner = new LatticeRemoteSnapshotService(sender, NullLogger<LatticeRemoteSnapshotService>.Instance);
        Assert.That(() => new LatticeRemoteSnapshotGrpcService(null!, inner, NullLogger<LatticeRemoteSnapshotGrpcService>.Instance),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_on_null_service()
    {
        Assert.That(() => new LatticeRemoteSnapshotGrpcService(CreateMethods(), null!, NullLogger<LatticeRemoteSnapshotGrpcService>.Instance),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_on_null_logger()
    {
        var sender = new StubSenderSnapshotProvider();
        var inner = new LatticeRemoteSnapshotService(sender, NullLogger<LatticeRemoteSnapshotService>.Instance);
        Assert.That(() => new LatticeRemoteSnapshotGrpcService(CreateMethods(), inner, null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void BindService_throws_when_binder_null()
    {
        Assert.That(
            () => LatticeRemoteSnapshotGrpcServiceBase.BindService(null!, serviceImpl: null),
            Throws.ArgumentNullException);
    }

    [Test]
    public void BindService_throws_when_methods_holder_not_initialised()
    {
        // Reset the holder to simulate a cold start before
        // AddLatticeReplicationGrpc / MapLatticeReplicationGrpc ran.
        var saved = LatticeRemoteSnapshotGrpcMethodsHolder.Current;
        LatticeRemoteSnapshotGrpcMethodsHolder.Current = null;
        try
        {
            Assert.That(
                () => LatticeRemoteSnapshotGrpcServiceBase.BindService(Substitute.For<ServiceBinderBase>(), serviceImpl: null),
                Throws.InvalidOperationException);
        }
        finally
        {
            LatticeRemoteSnapshotGrpcMethodsHolder.Current = saved;
        }
    }

    [Test]
    public void BindService_records_metadata_when_service_impl_null()
    {
        // Ensure the holder is populated.
        var saved = LatticeRemoteSnapshotGrpcMethodsHolder.Current;
        LatticeRemoteSnapshotGrpcMethodsHolder.Current = CreateMethods();
        try
        {
            Assert.That(
                () => LatticeRemoteSnapshotGrpcServiceBase.BindService(Substitute.For<ServiceBinderBase>(), serviceImpl: null),
                Throws.Nothing);
        }
        finally
        {
            LatticeRemoteSnapshotGrpcMethodsHolder.Current = saved;
        }
    }

    private sealed class RecordingServerStreamWriter<T> : IServerStreamWriter<T>
    {
        public List<T> Written { get; } = new();
        public WriteOptions? WriteOptions { get; set; }

        public Task WriteAsync(T message)
        {
            Written.Add(message);
            return Task.CompletedTask;
        }
    }

    private sealed class FakeServerCallContext : ServerCallContext
    {
        protected override string MethodCore => string.Empty;
        protected override string HostCore => string.Empty;
        protected override string PeerCore => string.Empty;
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override global::Grpc.Core.Metadata RequestHeadersCore { get; } = new();
        protected override CancellationToken CancellationTokenCore => CancellationToken.None;
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