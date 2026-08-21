using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;
using GrpcMetadata = Grpc.Core.Metadata;

namespace Orleans.Lattice.Api.State.Grpc.Tests;

/// <summary>
/// Unit coverage for <see cref="LatticeStateApiGrpcClient"/> driven over an
/// in-process fake <see cref="CallInvoker"/>. This exercises every client
/// method (including the dead-letter, history, and auth-scheme RPCs) and the
/// server-streaming enumerators without standing up a live gRPC server, so the
/// thin request/response plumbing is proven deterministically.
/// </summary>
[TestFixture]
public sealed class LatticeStateApiGrpcClientUnaryTests
{
    private RecordingCallInvoker _invoker = null!;
    private ServiceProvider _services = null!;
    private LatticeStateGrpcMethods _methods = null!;
    private LatticeStateApiGrpcClient _client = null!;

    [SetUp]
    public void SetUp()
    {
        _invoker = new RecordingCallInvoker();
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _methods = LatticeStateGrpcMethods.FromServiceProvider(_services);
        _client = new LatticeStateApiGrpcClient(_invoker, _methods);
    }

    [TearDown]
    public void TearDown() => _services.Dispose();

    [Test]
    public void Constructor_when_invoker_is_null_throws()
    {
        Assert.That(
            () => new LatticeStateApiGrpcClient(null!, _methods),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task ListTreesAsync_invokes_unary_call_and_returns_response()
    {
        var response = await _client.ListTreesAsync(new CatalogRequest { PageSize = 5 });

        Assert.That(response, Is.Not.Null);
        Assert.That(_invoker.LastMethodName, Is.EqualTo(LatticeStateGrpcMethods.ListTreesMethodName));
    }

    [Test]
    public async Task Every_unary_rpc_returns_a_response()
    {
        Assert.That(await _client.ListViewsAsync(new CatalogRequest()), Is.Not.Null);
        Assert.That(await _client.ListTagIndexesAsync(new CatalogRequest()), Is.Not.Null);
        Assert.That(await _client.ListTagValuesAsync(new CatalogRequest()), Is.Not.Null);
        Assert.That(await _client.ListCoveredTreesAsync(new CatalogRequest()), Is.Not.Null);
        Assert.That(await _client.ListIndexTagsAsync(new CatalogRequest()), Is.Not.Null);
        Assert.That(await _client.ScanTagMembersAsync(new TagMemberScanRequest { IndexName = "idx", Tag = "t" }), Is.Not.Null);
        Assert.That(await _client.GetTreeStructureAsync(new StructureRequest { TreeId = "tree" }), Is.Not.Null);
        Assert.That(await _client.ScanEntriesAsync(new EntryScanRequest { TreeId = "tree" }), Is.Not.Null);
        Assert.That(await _client.GetEntryAsync(new EntryGetRequest { TreeId = "tree", Key = "k" }), Is.Not.Null);
        Assert.That(await _client.GetEntryHistoryAsync(new EntryHistoryRequest { TreeId = "tree", Key = "k" }), Is.Not.Null);
        Assert.That(await _client.CancelScanAsync(new EntryScanCancelRequest { TreeId = "tree" }), Is.Not.Null);
        Assert.That(await _client.GetMetricsSnapshotAsync(new TreeMetricsRequest()), Is.Not.Null);
        Assert.That(await _client.GetClusterInfoAsync(new ClusterInfoRequest()), Is.Not.Null);
        Assert.That(await _client.GetAuthSchemeAsync(new AuthSchemeAdvertisementRequest()), Is.Not.Null);
        Assert.That(await _client.GetDeadLetterCountAsync(new DeadLetterCountRequest { TreeId = "tree" }), Is.Not.Null);
        Assert.That(await _client.ListDeadLettersAsync(new DeadLetterQueueRequest { TreeId = "tree" }), Is.Not.Null);
    }

    [Test]
    public void UnaryAsync_when_request_is_null_throws()
    {
        Assert.That(
            () => _client.GetEntryAsync(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ObserveChangesAsync_when_request_is_null_throws()
    {
        Assert.That(
            async () =>
            {
                await foreach (var _ in _client.ObserveChangesAsync(null!))
                {
                }
            },
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task ObserveChangesAsync_yields_every_streamed_notification()
    {
        _invoker.ServerStreamItemCount = 3;

        var count = 0;
        await foreach (var notification in _client.ObserveChangesAsync(new StateObserveRequest { TreeId = "tree" }))
        {
            Assert.That(notification, Is.Not.Null);
            count++;
        }

        Assert.That(count, Is.EqualTo(3));
    }

    [Test]
    public async Task ObserveMetricsAsync_yields_every_streamed_snapshot()
    {
        _invoker.ServerStreamItemCount = 2;

        var count = 0;
        await foreach (var snapshot in _client.ObserveMetricsAsync(new TreeMetricsRequest()))
        {
            Assert.That(snapshot, Is.Not.Null);
            count++;
        }

        Assert.That(count, Is.EqualTo(2));
    }

    private sealed class RecordingCallInvoker : CallInvoker
    {
        public string? LastMethodName { get; private set; }

        public int ServerStreamItemCount { get; set; } = 1;

        public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            string? host,
            CallOptions options,
            TRequest request)
        {
            LastMethodName = method.Name;
            var response = Activator.CreateInstance<TResponse>();
            return new AsyncUnaryCall<TResponse>(
                Task.FromResult(response),
                Task.FromResult(new GrpcMetadata()),
                () => Status.DefaultSuccess,
                () => new GrpcMetadata(),
                () => { });
        }

        public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            string? host,
            CallOptions options,
            TRequest request)
        {
            LastMethodName = method.Name;
            return new AsyncServerStreamingCall<TResponse>(
                new CannedStreamReader<TResponse>(ServerStreamItemCount),
                Task.FromResult(new GrpcMetadata()),
                () => Status.DefaultSuccess,
                () => new GrpcMetadata(),
                () => { });
        }

        public override TResponse BlockingUnaryCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            string? host,
            CallOptions options,
            TRequest request)
            => throw new NotSupportedException();

        public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            string? host,
            CallOptions options)
            => throw new NotSupportedException();

        public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            string? host,
            CallOptions options)
            => throw new NotSupportedException();
    }

    private sealed class CannedStreamReader<T> : IAsyncStreamReader<T>
        where T : class
    {
        private int _remaining;

        public CannedStreamReader(int count) => _remaining = count;

        public T Current { get; private set; } = null!;

        public Task<bool> MoveNext(CancellationToken cancellationToken)
        {
            if (_remaining <= 0)
            {
                return Task.FromResult(false);
            }

            _remaining--;
            Current = Activator.CreateInstance<T>();
            return Task.FromResult(true);
        }
    }
}
