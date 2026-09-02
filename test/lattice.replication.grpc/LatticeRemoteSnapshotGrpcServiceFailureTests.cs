using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// Pins the failure and cancellation arms of
/// <see cref="LatticeRemoteSnapshotGrpcService"/>, and the
/// <c>serviceImpl</c>-bound half of
/// <see cref="LatticeRemoteSnapshotGrpcServiceBase.BindService"/>.
/// <para>
/// The sibling fixture covers the happy path, the argument guards and the
/// enrollment refusal. What is pinned here is the translation contract the
/// receiver depends on: a caller-initiated cancellation must surface as a
/// plain <see cref="OperationCanceledException"/> rather than being
/// flattened into <see cref="StatusCode.Internal"/>, while an unexpected
/// sender-side fault must surface as <see cref="StatusCode.Internal"/> with
/// the underlying detail carried separately so the peer learns the call
/// failed without the server leaking its stack.
/// </para>
/// </summary>
[TestFixture]
public class LatticeRemoteSnapshotGrpcServiceFailureTests
{
    private const string Tree = "tree";
    private const string Source = "site-a";

    private static LatticeRemoteSnapshotGrpcMethods CreateMethods()
    {
        var sp = new ServiceCollection().AddSerializer().BuildServiceProvider();
        return new LatticeRemoteSnapshotGrpcMethods(
            sp.GetRequiredService<Serializer<RemoteSnapshotMetadataRequest>>(),
            sp.GetRequiredService<Serializer<RemoteSnapshotMetadata>>(),
            sp.GetRequiredService<Serializer<RemoteSnapshotStreamItem>>());
    }

    /// <summary>Admits every tree, so the enrollment gate never short-circuits.</summary>
    private sealed class AllEnrolledContext : ILatticeReplicationContext
    {
        public bool IsReplicationEnabled => true;

        public string LocalReplicaId => Source;

        public LatticeMergeMode? ResolveMergeMode(string treeId) => LatticeMergeMode.LwwRegister;
    }

    /// <summary>
    /// Snapshot provider whose export always faults with the supplied
    /// exception, so the service's catch arms can be driven without a live
    /// tree behind them.
    /// </summary>
    private sealed class ThrowingSnapshotProvider(Exception failure) : ISnapshotProvider
    {
        public Task<SnapshotStream> ExportAsync(
            string treeName,
            HybridLogicalClock asOfHlc,
            CancellationToken cancellationToken = default)
            => throw failure;
    }

    private static LatticeRemoteSnapshotGrpcService CreateService(ISnapshotProvider provider)
    {
        var inner = new LatticeRemoteSnapshotService(
            provider,
            new AllEnrolledContext(),
            NullLogger<LatticeRemoteSnapshotService>.Instance);
        return new LatticeRemoteSnapshotGrpcService(
            CreateMethods(),
            inner,
            NullLogger<LatticeRemoteSnapshotGrpcService>.Instance);
    }

    private static RemoteSnapshotMetadataRequestBox Request()
        => new()
        {
            Value = new RemoteSnapshotMetadataRequest
            {
                TreeName = Tree,
                SourceClusterId = Source,
                FromAsOfHlc = HybridLogicalClock.Zero,
            },
        };

    [Test]
    public void GetMetadata_rethrows_cancellation_rather_than_mapping_it_to_internal()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var service = CreateService(new ThrowingSnapshotProvider(new OperationCanceledException()));

        Assert.That(
            async () => await service.GetMetadata(Request(), new FakeServerCallContext(cts.Token)),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void GetMetadata_maps_an_unexpected_sender_fault_to_internal()
    {
        var service = CreateService(new ThrowingSnapshotProvider(new InvalidOperationException("tree store offline")));

        var rpc = Assert.ThrowsAsync<RpcException>(async () =>
            await service.GetMetadata(Request(), new FakeServerCallContext(CancellationToken.None)));

        Assert.Multiple(() =>
        {
            Assert.That(rpc!.StatusCode, Is.EqualTo(StatusCode.Internal));
            Assert.That(rpc.Status.Detail, Does.Contain(Tree));
            // The underlying message rides in the trailers, not in the status
            // detail, so a peer is told the call failed without the server
            // volunteering its internal failure text as the status.
            Assert.That(rpc.Status.Detail, Does.Not.Contain("tree store offline"));
        });
    }

    [Test]
    public void RequestSnapshot_rethrows_cancellation_rather_than_mapping_it_to_internal()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var service = CreateService(new ThrowingSnapshotProvider(new OperationCanceledException()));

        Assert.That(
            async () => await service.RequestSnapshot(
                Request(),
                new CollectingStreamWriter<RemoteSnapshotStreamItemBox>(),
                new FakeServerCallContext(cts.Token)),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void RequestSnapshot_maps_an_unexpected_sender_fault_to_internal()
    {
        var service = CreateService(new ThrowingSnapshotProvider(new InvalidOperationException("leaf chain torn")));
        var writer = new CollectingStreamWriter<RemoteSnapshotStreamItemBox>();

        var rpc = Assert.ThrowsAsync<RpcException>(async () =>
            await service.RequestSnapshot(Request(), writer, new FakeServerCallContext(CancellationToken.None)));

        Assert.Multiple(() =>
        {
            Assert.That(rpc!.StatusCode, Is.EqualTo(StatusCode.Internal));
            Assert.That(rpc.Status.Detail, Does.Contain(Tree));
            Assert.That(writer.Written, Is.Empty);
        });
    }

    [Test]
    public async Task BindService_binds_both_rpcs_to_the_supplied_service_instance()
    {
        var saved = LatticeRemoteSnapshotGrpcMethodsHolder.Current;
        LatticeRemoteSnapshotGrpcMethodsHolder.Current = CreateMethods();
        try
        {
            var service = CreateService(new ThrowingSnapshotProvider(new InvalidOperationException("boom")));
            var binder = new RecordingServiceBinder();

            LatticeRemoteSnapshotGrpcServiceBase.BindService(binder, service);

            Assert.Multiple(() =>
            {
                Assert.That(binder.UnaryHandlers, Has.Count.EqualTo(1));
                Assert.That(binder.ServerStreamingHandlers, Has.Count.EqualTo(1));
            });

            // Invoking the bound delegate proves it is wired to this instance
            // and not merely registered as metadata: the stub provider's
            // failure has to come back out through the bound handler.
            var handler = (UnaryServerMethod<RemoteSnapshotMetadataRequestBox, RemoteSnapshotMetadataBox>)binder.UnaryHandlers[0];
            var rpc = Assert.ThrowsAsync<RpcException>(async () =>
                await handler(Request(), new FakeServerCallContext(CancellationToken.None)));
            Assert.That(rpc!.StatusCode, Is.EqualTo(StatusCode.Internal));

            var stream = (ServerStreamingServerMethod<RemoteSnapshotMetadataRequestBox, RemoteSnapshotStreamItemBox>)binder.ServerStreamingHandlers[0];
            var streamRpc = Assert.ThrowsAsync<RpcException>(async () =>
                await stream(
                    Request(),
                    new CollectingStreamWriter<RemoteSnapshotStreamItemBox>(),
                    new FakeServerCallContext(CancellationToken.None)));
            Assert.That(streamRpc!.StatusCode, Is.EqualTo(StatusCode.Internal));

            await Task.CompletedTask;
        }
        finally
        {
            LatticeRemoteSnapshotGrpcMethodsHolder.Current = saved;
        }
    }

    /// <summary>
    /// Minimal <see cref="ServiceBinderBase"/> that records the handler
    /// delegates gRPC's binding callback registers, so a test can invoke them
    /// directly instead of standing up a server.
    /// </summary>
    private sealed class RecordingServiceBinder : ServiceBinderBase
    {
        public List<Delegate> UnaryHandlers { get; } = [];

        public List<Delegate> ServerStreamingHandlers { get; } = [];

        public override void AddMethod<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            UnaryServerMethod<TRequest, TResponse>? handler)
        {
            if (handler is not null)
            {
                UnaryHandlers.Add(handler);
            }
        }

        public override void AddMethod<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            ServerStreamingServerMethod<TRequest, TResponse>? handler)
        {
            if (handler is not null)
            {
                ServerStreamingHandlers.Add(handler);
            }
        }
    }

    private sealed class CollectingStreamWriter<T> : IServerStreamWriter<T>
    {
        public List<T> Written { get; } = [];

        public WriteOptions? WriteOptions { get; set; }

        public Task WriteAsync(T message)
        {
            Written.Add(message);
            return Task.CompletedTask;
        }
    }

    private sealed class FakeServerCallContext(CancellationToken cancellationToken) : ServerCallContext
    {
        protected override string MethodCore => string.Empty;
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
