using System.Collections.Frozen;
using System.Text;
using Grpc.Core;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Region-routing coverage for the four <see cref="CallInvoker"/> overrides that
/// <see cref="RegionRoutingCallInvokerTests"/> does not reach:
/// <c>BlockingUnaryCall</c>, <c>AsyncServerStreamingCall</c>,
/// <c>AsyncClientStreamingCall</c> and <c>AsyncDuplexStreamingCall</c>.
/// </summary>
/// <remarks>
/// <para>
/// This matters for the same structural reason the gRPC auth interceptors'
/// streaming handlers did: every one of these five members must consult
/// <c>Selected()</c>. A member that reached for the default invoker instead -
/// a one-token difference, and the shape a copy-paste naturally produces - would
/// send that call kind to the <b>wrong region</b> while the unary path kept
/// routing correctly, so the existing unary-only fixture would stay green.
/// </para>
/// <para>
/// Each test therefore selects a non-default region and asserts the peer
/// invoker served the call, which fails if the override is rewired to
/// <c>_default</c>. Deterministic; no network, no timers.
/// </para>
/// </remarks>
[TestFixture]
public sealed class RegionRoutingCallInvokerCallKindTests
{
    private static readonly Marshaller<string> StringMarshaller =
        Marshallers.Create(static s => Encoding.UTF8.GetBytes(s), static b => Encoding.UTF8.GetString(b));

    private static Method<string, string> MethodOf(MethodType type) =>
        new(type, "svc", "m", StringMarshaller, StringMarshaller);

    private static RegionRoutingCallInvoker Build(RecordingCallInvoker @default, string region, RecordingCallInvoker peer)
    {
        var map = new Dictionary<string, CallInvoker>(StringComparer.Ordinal)
        {
            ["current"] = @default,
            [region] = peer,
        };
        return new RegionRoutingCallInvoker(@default, map.ToFrozenDictionary(StringComparer.Ordinal));
    }

    [Test]
    public void BlockingUnaryCall_routes_to_the_selected_region()
    {
        var @default = new RecordingCallInvoker("default");
        var peer = new RecordingCallInvoker("peer");
        var routing = Build(@default, "peer", peer);

        string result;
        using (LatticeApiMcpRegionScope.Enter("peer"))
        {
            result = routing.BlockingUnaryCall(MethodOf(MethodType.Unary), host: null, new CallOptions(), "req");
        }

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.EqualTo("peer"));
            Assert.That(peer.BlockingUnaryCalls, Is.EqualTo(1));
            Assert.That(@default.BlockingUnaryCalls, Is.Zero, "the default region must not have served the call");
        });
    }

    [Test]
    public void BlockingUnaryCall_with_no_selection_routes_to_the_default_region()
    {
        var @default = new RecordingCallInvoker("default");
        var peer = new RecordingCallInvoker("peer");
        var routing = Build(@default, "peer", peer);

        var result = routing.BlockingUnaryCall(MethodOf(MethodType.Unary), host: null, new CallOptions(), "req");

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.EqualTo("default"));
            Assert.That(peer.BlockingUnaryCalls, Is.Zero);
        });
    }

    [Test]
    public async Task AsyncServerStreamingCall_routes_to_the_selected_region()
    {
        var @default = new RecordingCallInvoker("default");
        var peer = new RecordingCallInvoker("peer");
        var routing = Build(@default, "peer", peer);

        AsyncServerStreamingCall<string> call;
        using (LatticeApiMcpRegionScope.Enter("peer"))
        {
            call = routing.AsyncServerStreamingCall(
                MethodOf(MethodType.ServerStreaming), host: null, new CallOptions(), "req");
        }

        using (call)
        {
            Assert.That(await call.ResponseStream.MoveNext(CancellationToken.None), Is.True);
            Assert.Multiple(() =>
            {
                Assert.That(call.ResponseStream.Current, Is.EqualTo("peer"));
                Assert.That(peer.ServerStreamingCalls, Is.EqualTo(1));
                Assert.That(@default.ServerStreamingCalls, Is.Zero);
            });
        }
    }

    [Test]
    public async Task AsyncClientStreamingCall_routes_to_the_selected_region()
    {
        var @default = new RecordingCallInvoker("default");
        var peer = new RecordingCallInvoker("peer");
        var routing = Build(@default, "peer", peer);

        AsyncClientStreamingCall<string, string> call;
        using (LatticeApiMcpRegionScope.Enter("peer"))
        {
            call = routing.AsyncClientStreamingCall(
                MethodOf(MethodType.ClientStreaming), host: null, new CallOptions());
        }

        using (call)
        {
            Assert.That(await call.ResponseAsync, Is.EqualTo("peer"));
            Assert.Multiple(() =>
            {
                Assert.That(peer.ClientStreamingCalls, Is.EqualTo(1));
                Assert.That(@default.ClientStreamingCalls, Is.Zero);
            });
        }
    }

    [Test]
    public async Task AsyncDuplexStreamingCall_routes_to_the_selected_region()
    {
        var @default = new RecordingCallInvoker("default");
        var peer = new RecordingCallInvoker("peer");
        var routing = Build(@default, "peer", peer);

        AsyncDuplexStreamingCall<string, string> call;
        using (LatticeApiMcpRegionScope.Enter("peer"))
        {
            call = routing.AsyncDuplexStreamingCall(
                MethodOf(MethodType.DuplexStreaming), host: null, new CallOptions());
        }

        using (call)
        {
            Assert.That(await call.ResponseStream.MoveNext(CancellationToken.None), Is.True);
            Assert.Multiple(() =>
            {
                Assert.That(call.ResponseStream.Current, Is.EqualTo("peer"));
                Assert.That(peer.DuplexStreamingCalls, Is.EqualTo(1));
                Assert.That(@default.DuplexStreamingCalls, Is.Zero);
            });
        }
    }

    [Test]
    public void Every_call_kind_falls_back_to_the_default_when_the_selection_is_unmapped()
    {
        var @default = new RecordingCallInvoker("default");
        var peer = new RecordingCallInvoker("peer");
        var routing = Build(@default, "peer", peer);

        using (LatticeApiMcpRegionScope.Enter("unmapped"))
        {
            routing.BlockingUnaryCall(MethodOf(MethodType.Unary), null, new CallOptions(), "req");
            routing.AsyncServerStreamingCall(MethodOf(MethodType.ServerStreaming), null, new CallOptions(), "req").Dispose();
            routing.AsyncClientStreamingCall(MethodOf(MethodType.ClientStreaming), null, new CallOptions()).Dispose();
            routing.AsyncDuplexStreamingCall(MethodOf(MethodType.DuplexStreaming), null, new CallOptions()).Dispose();
        }

        Assert.Multiple(() =>
        {
            Assert.That(@default.BlockingUnaryCalls, Is.EqualTo(1));
            Assert.That(@default.ServerStreamingCalls, Is.EqualTo(1));
            Assert.That(@default.ClientStreamingCalls, Is.EqualTo(1));
            Assert.That(@default.DuplexStreamingCalls, Is.EqualTo(1));
            Assert.That(
                peer.BlockingUnaryCalls + peer.ServerStreamingCalls + peer.ClientStreamingCalls + peer.DuplexStreamingCalls,
                Is.Zero,
                "an unmapped selection must fall back to the default rather than reach a peer");
        });
    }

    /// <summary>
    /// A <see cref="CallInvoker"/> that answers every call kind with its own name
    /// and counts the calls it served, so a routing test can prove which invoker
    /// the routing layer selected for each kind.
    /// </summary>
    private sealed class RecordingCallInvoker(string name) : CallInvoker
    {
        public int BlockingUnaryCalls { get; private set; }

        public int ServerStreamingCalls { get; private set; }

        public int ClientStreamingCalls { get; private set; }

        public int DuplexStreamingCalls { get; private set; }

        public override TResponse BlockingUnaryCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
        {
            BlockingUnaryCalls++;
            return (TResponse)(object)name;
        }

        public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
            => new(
                Task.FromResult((TResponse)(object)name),
                Task.FromResult(new Grpc.Core.Metadata()),
                static () => Status.DefaultSuccess,
                static () => new Grpc.Core.Metadata(),
                static () => { });

        public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
        {
            ServerStreamingCalls++;
            return new AsyncServerStreamingCall<TResponse>(
                new SingletonStreamReader<TResponse>((TResponse)(object)name),
                Task.FromResult(new Grpc.Core.Metadata()),
                static () => Status.DefaultSuccess,
                static () => new Grpc.Core.Metadata(),
                static () => { });
        }

        public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options)
        {
            ClientStreamingCalls++;
            return new AsyncClientStreamingCall<TRequest, TResponse>(
                new DiscardingStreamWriter<TRequest>(),
                Task.FromResult((TResponse)(object)name),
                Task.FromResult(new Grpc.Core.Metadata()),
                static () => Status.DefaultSuccess,
                static () => new Grpc.Core.Metadata(),
                static () => { });
        }

        public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options)
        {
            DuplexStreamingCalls++;
            return new AsyncDuplexStreamingCall<TRequest, TResponse>(
                new DiscardingStreamWriter<TRequest>(),
                new SingletonStreamReader<TResponse>((TResponse)(object)name),
                Task.FromResult(new Grpc.Core.Metadata()),
                static () => Status.DefaultSuccess,
                static () => new Grpc.Core.Metadata(),
                static () => { });
        }
    }

    /// <summary>A response stream that yields exactly one item, then completes.</summary>
    private sealed class SingletonStreamReader<T>(T item) : IAsyncStreamReader<T>
    {
        private bool _consumed;

        public T Current { get; private set; } = default!;

        public Task<bool> MoveNext(CancellationToken cancellationToken)
        {
            if (_consumed)
            {
                return Task.FromResult(false);
            }

            _consumed = true;
            Current = item;
            return Task.FromResult(true);
        }
    }

    /// <summary>A request stream that accepts and discards everything written to it.</summary>
    private sealed class DiscardingStreamWriter<T> : IClientStreamWriter<T>
    {
        public WriteOptions? WriteOptions { get; set; }

        public Task CompleteAsync() => Task.CompletedTask;

        public Task WriteAsync(T message) => Task.CompletedTask;
    }
}
