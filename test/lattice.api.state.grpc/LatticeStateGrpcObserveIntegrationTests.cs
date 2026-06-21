using System.Text;
using Grpc.Core;

namespace Orleans.Lattice.Api.State.Grpc.Tests;

/// <summary>
/// Integration coverage for the server-streaming <c>ObserveChanges</c> RPC of
/// the <c>Orleans.Lattice.Api.State.Grpc</c> binding. Drives the subscription
/// over an in-process <c>TestServer</c> backed by a real
/// <see cref="ILatticeStateObserver"/> and asserts live delivery, cursor
/// resume, and gRPC status-code mapping for the missing-tree and
/// expired-cursor paths.
/// </summary>
[TestFixture]
[Category("Integration")]
public class LatticeStateGrpcObserveIntegrationTests
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

    private AsyncServerStreamingCall<StateChangeNotification> Observe(
        StateObserveRequest request,
        CancellationToken cancellationToken)
        => _host.Channel.CreateCallInvoker().AsyncServerStreamingCall(
            _host.Methods.ObserveChanges,
            host: null,
            new CallOptions(cancellationToken: cancellationToken),
            request);

    private static async Task<IReadOnlyList<StateChangeNotification>> ReadAsync(
        AsyncServerStreamingCall<StateChangeNotification> call,
        int count,
        CancellationToken cancellationToken)
    {
        var collected = new List<StateChangeNotification>(count);
        while (collected.Count < count && await call.ResponseStream.MoveNext(cancellationToken).ConfigureAwait(false))
        {
            collected.Add(call.ResponseStream.Current);
        }

        return collected;
    }

    [Test]
    public async Task observe_streams_set_change_over_grpc()
    {
        var treeId = $"grpc-obs-set-{Guid.NewGuid():N}";
        var tree = await _fixture.RegisterTreeAsync(treeId, shardCount: 1);

        using var cts = new CancellationTokenSource(Timeout);
        using var call = Observe(new StateObserveRequest { TreeId = treeId }, cts.Token);

        // Let the server-side subscription seed its tail cursor, then mutate.
        await Task.Delay(300);
        await tree.SetAsync(GrpcStateClusterFixture.KeyAt(1), Encoding.UTF8.GetBytes("v1"));

        var changes = await ReadAsync(call, 1, cts.Token);

        Assert.That(changes, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(changes[0].Kind, Is.EqualTo(StateChangeKind.Set));
            Assert.That(changes[0].TreeId, Is.EqualTo(treeId));
            Assert.That(changes[0].Key, Is.EqualTo(GrpcStateClusterFixture.KeyAt(1)));
            Assert.That(changes[0].Position, Is.Not.Empty);
        });
    }

    [Test]
    public async Task observe_resumes_from_cursor_over_grpc()
    {
        var treeId = $"grpc-obs-resume-{Guid.NewGuid():N}";
        var tree = await _fixture.RegisterTreeAsync(treeId, shardCount: 1);

        string resumeToken;
        using (var firstCts = new CancellationTokenSource(Timeout))
        using (var firstCall = Observe(new StateObserveRequest { TreeId = treeId }, firstCts.Token))
        {
            await Task.Delay(300);
            await tree.SetAsync(GrpcStateClusterFixture.KeyAt(1), Encoding.UTF8.GetBytes("a"));
            var first = await ReadAsync(firstCall, 1, firstCts.Token);
            Assert.That(first, Has.Count.EqualTo(1));
            resumeToken = first[0].Position;
        }

        using var cts = new CancellationTokenSource(Timeout);
        using var call = Observe(
            new StateObserveRequest { TreeId = treeId, ContinuationToken = resumeToken }, cts.Token);

        await Task.Delay(300);
        await tree.SetAsync(GrpcStateClusterFixture.KeyAt(2), Encoding.UTF8.GetBytes("b"));
        var resumed = await ReadAsync(call, 1, cts.Token);

        Assert.That(resumed, Has.Count.EqualTo(1));
        Assert.That(resumed[0].Key, Is.EqualTo(GrpcStateClusterFixture.KeyAt(2)));
    }

    [Test]
    public void observe_maps_missing_tree_to_not_found_status_code()
    {
        var ex = Assert.ThrowsAsync<RpcException>(async () =>
        {
            using var cts = new CancellationTokenSource(Timeout);
            using var call = Observe(
                new StateObserveRequest { TreeId = $"missing-{Guid.NewGuid():N}" }, cts.Token);
            await ReadAsync(call, 1, cts.Token);
        });
        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.NotFound));
    }

    [Test]
    public async Task observe_maps_expired_cursor_to_failed_precondition_status_code()
    {
        var treeId = $"grpc-obs-topo-{Guid.NewGuid():N}";
        await _fixture.RegisterTreeAsync(treeId, shardCount: 1);

        // A token encoding two partitions cannot resume a single-partition tree.
        var token = Convert.ToBase64String(Encoding.ASCII.GetBytes("1|0|0"));

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
        {
            using var cts = new CancellationTokenSource(Timeout);
            using var call = Observe(
                new StateObserveRequest { TreeId = treeId, ContinuationToken = token }, cts.Token);
            await ReadAsync(call, 1, cts.Token);
        });
        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.FailedPrecondition));
    }
}
