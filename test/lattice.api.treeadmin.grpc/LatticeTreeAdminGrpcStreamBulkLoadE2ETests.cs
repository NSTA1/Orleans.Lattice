using Grpc.Core;
using Grpc.Core.Interceptors;
using Orleans.Lattice.Api.Data;

namespace Orleans.Lattice.Api.TreeAdmin.Grpc.Tests;

/// <summary>
/// End-to-end coverage of the duplex <c>StreamBulkLoad</c> RPC and
/// <see cref="LatticeTreeAdminApiGrpcClient.BulkLoadStreamAsync"/> over a live, co-hosted
/// gRPC server bound to a real cluster's facade (issue #5365): chunks stream in, each is
/// acknowledged in order, the half-close commits, and a stream broken mid-load resumes from
/// its first unacknowledged chunk with no entry lost or written twice.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeTreeAdminGrpcStreamBulkLoadE2ETests
{
    private GrpcTreeAdminClusterFixture _fixture = null!;
    private GrpcTreeAdminHost _host = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new GrpcTreeAdminClusterFixture();
        await _fixture.InitializeAsync();
        _host = await _fixture.CreateGrpcHostAsync(new AllowAllTreeAdminApiAuthorizer());
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

    [Test]
    public async Task stream_bulk_load_acknowledges_every_chunk_in_order_and_commits()
    {
        const string tree = "stream-bulk-e2e";
        await _host.Client.CreateTreeAsync(tree, shardCount: 2);
        var acks = new List<TreeBulkLoadChunkAck>();

        var result = await _host.Client.BulkLoadStreamAsync(
            tree,
            "stream-1",
            Keys(2_500),
            new ListProgress<TreeBulkLoadChunkAck>(acks),
            new TreeBulkLoadStreamOptions { ChunkSize = 1_000, MaxUnacknowledgedChunks = 2 });

        Assert.Multiple(() =>
        {
            Assert.That(result.TreeId, Is.EqualTo(tree));
            Assert.That(result.TotalLiveKeys, Is.EqualTo(2_500));
            Assert.That(acks.Select(ack => ack.ChunkIndex), Is.EqualTo(new long[] { 0, 1, 2 }));
            Assert.That(acks.Select(ack => ack.AcceptedEntryCount), Is.EqualTo(new[] { 1_000, 1_000, 500 }));
        });

        var lattice = _fixture.GrainFactory.GetGrain<ILattice>(tree);
        Assert.That(await lattice.GetAsync(Key(2_499)), Is.Not.Null);
    }

    [Test]
    public async Task stream_bulk_load_refuses_a_non_empty_tree_before_writing()
    {
        const string tree = "stream-bulk-nonempty-e2e";
        await _fixture.GrainFactory.GetGrain<ILattice>(tree).SetAsync("seed", [1]);

        var error = Assert.ThrowsAsync<RpcException>(async () =>
            await _host.Client.BulkLoadStreamAsync(tree, "stream-2", Keys(10)));
        var count = await _fixture.GrainFactory.GetGrain<ILattice>(tree).CountAsync();

        Assert.Multiple(() =>
        {
            Assert.That(error!.StatusCode, Is.EqualTo(StatusCode.FailedPrecondition));
            Assert.That(count, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task stream_bulk_load_stops_at_a_key_out_of_order_and_commits_nothing_after_it()
    {
        const string tree = "stream-bulk-order-e2e";
        await _host.Client.CreateTreeAsync(tree, shardCount: 2);

        var error = Assert.ThrowsAsync<BulkLoadOrderException>(async () =>
            await _host.Client.BulkLoadStreamAsync(tree, "stream-3", OutOfOrderAt(250), options: new TreeBulkLoadStreamOptions { ChunkSize = 100 }));

        Assert.That(error!.OffendingKey, Is.EqualTo(Key(0)));
        var count = await _fixture.GrainFactory.GetGrain<ILattice>(tree).CountAsync();
        Assert.That(count, Is.LessThanOrEqualTo(200), "no chunk at or after the offending key may be written");
    }

    [Test]
    public async Task a_stream_broken_mid_load_resumes_from_its_first_unacknowledged_chunk_without_loss_or_duplicates()
    {
        const string tree = "stream-bulk-resume-e2e";
        const int total = 1_234;
        await _host.Client.CreateTreeAsync(tree, shardCount: 2);
        var fault = new BreakFirstStreamAfterAcks(acks: 2);
        var client = LatticeTreeAdminApiGrpcClient.Create(_host.Channel.Intercept(fault), _host.Services);
        var acks = new List<TreeBulkLoadChunkAck>();

        var result = await client.BulkLoadStreamAsync(
            tree,
            "stream-4",
            Keys(total),
            new ListProgress<TreeBulkLoadChunkAck>(acks),
            new TreeBulkLoadStreamOptions { ChunkSize = 100, MaxUnacknowledgedChunks = 4, MaxResumeAttempts = 2 });

        var lattice = _fixture.GrainFactory.GetGrain<ILattice>(tree);
        Assert.Multiple(() =>
        {
            Assert.That(fault.Calls, Is.EqualTo(2), "the broken stream is reopened exactly once");
            Assert.That(result.TotalLiveKeys, Is.EqualTo(total));
            Assert.That(acks.Select(ack => ack.ChunkIndex).Distinct().Order(), Is.EqualTo(Enumerable.Range(0, 13).Select(i => (long)i)),
                "every chunk is acknowledged, those re-sent after the break included");
        });

        Assert.That(await lattice.CountAsync(), Is.EqualTo(total), "no entry is lost and none is written twice");
        foreach (var index in new[] { 0, 199, 200, 399, 400, 1_233 })
        {
            Assert.That(await lattice.GetAsync(Key(index)), Is.EqualTo(Value(index)), $"entry {index} must hold its own value");
        }
    }

    [Test]
    public void IsResumable_admits_only_transient_transport_faults()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TreeAdminBulkLoadStreamCall.IsResumable(new RpcException(new Status(StatusCode.Unavailable, "down"))), Is.True);
            Assert.That(TreeAdminBulkLoadStreamCall.IsResumable(new RpcException(new Status(StatusCode.Aborted, "reset"))), Is.True);
            Assert.That(TreeAdminBulkLoadStreamCall.IsResumable(new RpcException(new Status(StatusCode.Internal, "reset", new IOException()))), Is.True);
            Assert.That(TreeAdminBulkLoadStreamCall.IsResumable(new RpcException(new Status(StatusCode.Internal, "server fault"))), Is.False);
            Assert.That(TreeAdminBulkLoadStreamCall.IsResumable(new RpcException(new Status(StatusCode.PermissionDenied, "no"))), Is.False);
            Assert.That(TreeAdminBulkLoadStreamCall.IsResumable(new RpcException(new Status(StatusCode.Cancelled, "stop"))), Is.False);
        });
    }

    private static string Key(int index) => "k" + index.ToString("D6", System.Globalization.CultureInfo.InvariantCulture);

    private static byte[] Value(int index) => BitConverter.GetBytes(index);

    private static async IAsyncEnumerable<DataEntry> Keys(int count)
    {
        for (var i = 0; i < count; i++)
        {
            if (i % 100 == 0)
            {
                await Task.Yield();
            }

            yield return new DataEntry { Key = Key(i), Value = Value(i) };
        }
    }

    private static async IAsyncEnumerable<DataEntry> OutOfOrderAt(int count)
    {
        await foreach (var entry in Keys(count))
        {
            yield return entry;
        }

        yield return new DataEntry { Key = Key(0), Value = [0] };
    }

    /// <summary>
    /// Breaks the first duplex call the way a dropped connection does: after it has read
    /// <c>acks</c> acknowledgements, its next read fails with <c>Unavailable</c> and the call
    /// is cancelled, so chunks already sent may have landed without their acknowledgement.
    /// </summary>
    private sealed class BreakFirstStreamAfterAcks(int acks) : Interceptor
    {
        private int _calls;

        public int Calls => _calls;

        public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
            ClientInterceptorContext<TRequest, TResponse> context,
            AsyncDuplexStreamingCallContinuation<TRequest, TResponse> continuation)
        {
            var call = continuation(context);
            if (Interlocked.Increment(ref _calls) != 1)
            {
                return call;
            }

            var reader = new BreakingReader<TResponse>(call.ResponseStream, acks, call.Dispose);
            return new AsyncDuplexStreamingCall<TRequest, TResponse>(
                call.RequestStream,
                reader,
                call.ResponseHeadersAsync,
                call.GetStatus,
                call.GetTrailers,
                call.Dispose);
        }
    }

    private sealed class BreakingReader<T>(IAsyncStreamReader<T> inner, int acks, Action abort) : IAsyncStreamReader<T>
    {
        private int _acks;

        public T Current => inner.Current;

        public async Task<bool> MoveNext(CancellationToken cancellationToken)
        {
            if (_acks >= acks)
            {
                abort();
                throw new RpcException(new Status(StatusCode.Unavailable, "The connection was dropped by the test."));
            }

            var moved = await inner.MoveNext(cancellationToken);
            if (moved && inner.Current is TreeAdminBulkLoadStreamResponse { Ack: not null })
            {
                _acks++;
            }

            return moved;
        }
    }

    private sealed class ListProgress<T>(List<T> values) : IProgress<T>
    {
        public void Report(T value)
        {
            lock (values)
            {
                values.Add(value);
            }
        }
    }
}
