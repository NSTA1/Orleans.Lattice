using Grpc.Core;
using Grpc.Net.Client;

namespace Orleans.Lattice.Api.State.Grpc.Tests;

/// <summary>
/// Integration coverage for the metrics-observation RPCs of the
/// <c>Orleans.Lattice.Api.State.Grpc</c> binding: the unary
/// <c>GetMetricsSnapshot</c> poll and the server-streaming
/// <c>ObserveMetrics</c> feed, driven over an in-process <c>TestServer</c>
/// backed by a real <see cref="ILatticeStateMetricsObserver"/>.
/// </summary>
[TestFixture]
[Category("Integration")]
public class LatticeStateGrpcMetricsIntegrationTests
{
    private GrpcStateClusterFixture _fixture = null!;
    private GrpcStateHost _host = null!;
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new GrpcStateClusterFixture();
        await _fixture.InitializeAsync();
        _host = await _fixture.CreateGrpcHostAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_host is not null)
        {
            await _host.DisposeAsync();
        }

        if (_fixture is not null)
        {
            await _fixture.DisposeAsync();
        }
    }

    private static async Task<TResponse> CallAsync<TRequest, TResponse>(
        GrpcChannel channel,
        Method<TRequest, TResponse> method,
        TRequest request)
        where TRequest : class
        where TResponse : class
    {
        var invoker = channel.CreateCallInvoker();
        using var call = invoker.AsyncUnaryCall(method, host: null, new CallOptions(), request);
        return await call.ResponseAsync.ConfigureAwait(false);
    }

    [Test]
    public async Task get_metrics_snapshot_matches_facade_over_grpc()
    {
        var treeId = $"grpc-metrics-{Guid.NewGuid():N}";
        await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: 9, shardCount: 2);

        var request = new TreeMetricsRequest { TreeIds = new[] { treeId } };
        var viaGrpc = await CallAsync(_host.Channel, _host.Methods.GetMetricsSnapshot, request);
        var viaFacade = await _fixture.Metrics.SampleAsync(request);

        Assert.That(viaGrpc.IsInitial, Is.True);
        var grpcMetrics = viaGrpc.Trees.Single(t => t.TreeId == treeId);
        var facadeMetrics = viaFacade.Trees.Single(t => t.TreeId == treeId);
        Assert.Multiple(() =>
        {
            Assert.That(grpcMetrics.LiveKeys, Is.EqualTo(facadeMetrics.LiveKeys));
            Assert.That(grpcMetrics.ShardCount, Is.EqualTo(facadeMetrics.ShardCount));
            Assert.That(grpcMetrics.MaxDepth, Is.EqualTo(facadeMetrics.MaxDepth));
        });
    }

    [Test]
    public async Task observe_metrics_streams_initial_snapshot_over_grpc()
    {
        var treeId = $"grpc-metrics-stream-{Guid.NewGuid():N}";
        await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: 6, shardCount: 1);

        using var cts = new CancellationTokenSource(Timeout);
        var invoker = _host.Channel.CreateCallInvoker();
        using var call = invoker.AsyncServerStreamingCall(
            _host.Methods.ObserveMetrics,
            host: null,
            new CallOptions(cancellationToken: cts.Token),
            new TreeMetricsRequest { TreeIds = new[] { treeId } });

        var moved = await call.ResponseStream.MoveNext(cts.Token);

        Assert.That(moved, Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(call.ResponseStream.Current.IsInitial, Is.True);
            Assert.That(call.ResponseStream.Current.Trees.Select(t => t.TreeId), Does.Contain(treeId));
        });
    }
}
