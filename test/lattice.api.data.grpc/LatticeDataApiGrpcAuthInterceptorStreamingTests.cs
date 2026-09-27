using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Api.Data.Grpc.Tests;

/// <summary>
/// Behavioural regression coverage proving <see cref="LatticeDataApiGrpcAuthInterceptor"/>
/// reaches an authorization decision on the <em>streaming</em> call shapes, not
/// only on unary calls.
/// <para>
/// <c>Grpc.Core.Interceptors.Interceptor</c> implements every handler as a
/// pass-through to the continuation, so a handler the interceptor does not
/// override admits the call with no authorization check whatsoever. An
/// interceptor that gated only <c>UnaryServerHandler</c> would therefore leave
/// the server-streaming, client-streaming, and duplex shapes wide open - a gap
/// that stays invisible until a streaming RPC is added to the already-gated
/// service, which is precisely the change least likely to prompt a review of the
/// interceptor. These tests fail if any streaming shape stops enforcing.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeDataApiGrpcAuthInterceptorStreamingTests
{
    private const string LatticeMethod = "/orleans.lattice.api.data/Set";
    private const string ForeignMethod = "/some.other.service/Ping";

    private static LatticeDataApiGrpcAuthInterceptor Create(
        ILatticeDataApiAuthorizer authorizer,
        bool requireAuthorization = true)
    {
        var options = new StaticOptionsMonitor(new LatticeDataApiGrpcOptions
        {
            RequireAuthorization = requireAuthorization,
        });
        return new LatticeDataApiGrpcAuthInterceptor(
            authorizer,
            options,
            NullLogger<LatticeDataApiGrpcAuthInterceptor>.Instance);
    }

    private static ILatticeDataApiAuthorizer Authorizer(bool allow)
    {
        var authorizer = Substitute.For<ILatticeDataApiAuthorizer>();
        authorizer
            .IsAuthorizedAsync(Arg.Any<LatticeDataApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .Returns(allow);
        return authorizer;
    }

    [Test]
    public void A_denied_client_streaming_call_is_rejected_with_permission_denied()
    {
        var interceptor = Create(Authorizer(allow: false));
        var reached = false;

        var ex = Assert.ThrowsAsync<RpcException>(() => interceptor.ClientStreamingServerHandler(
            new EmptyStreamReader<DataSetRequest>(),
            new StubServerCallContext(LatticeMethod),
            (_, _) =>
            {
                reached = true;
                return Task.FromResult(new DataSetResponse());
            }));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(reached, Is.False, "the continuation must not run for a denied call");
        });
    }

    [Test]
    public void A_denied_duplex_streaming_call_is_rejected_with_permission_denied()
    {
        var interceptor = Create(Authorizer(allow: false));
        var reached = false;

        var ex = Assert.ThrowsAsync<RpcException>(() => interceptor.DuplexStreamingServerHandler(
            new EmptyStreamReader<DataSetRequest>(),
            new DiscardingStreamWriter<DataSetResponse>(),
            new StubServerCallContext(LatticeMethod),
            (_, _, _) =>
            {
                reached = true;
                return Task.CompletedTask;
            }));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(reached, Is.False, "the continuation must not run for a denied call");
        });
    }

    [Test]
    public void A_denied_server_streaming_call_is_rejected_with_permission_denied()
    {
        var interceptor = Create(Authorizer(allow: false));
        var reached = false;

        var ex = Assert.ThrowsAsync<RpcException>(() => interceptor.ServerStreamingServerHandler(
            new DataSetRequest { TreeId = "t", Key = "k", Value = [1] },
            new DiscardingStreamWriter<DataSetResponse>(),
            new StubServerCallContext(LatticeMethod),
            (_, _, _) =>
            {
                reached = true;
                return Task.CompletedTask;
            }));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(reached, Is.False, "the continuation must not run for a denied call");
        });
    }

    [Test]
    public async Task An_authorized_client_streaming_call_reaches_the_continuation()
    {
        var interceptor = Create(Authorizer(allow: true));
        var expected = new DataSetResponse();

        var result = await interceptor.ClientStreamingServerHandler(
            new EmptyStreamReader<DataSetRequest>(),
            new StubServerCallContext(LatticeMethod),
            (_, _) => Task.FromResult(expected));

        Assert.That(result, Is.SameAs(expected));
    }

    [Test]
    public async Task An_authorized_duplex_streaming_call_reaches_the_continuation()
    {
        var interceptor = Create(Authorizer(allow: true));
        var reached = false;

        await interceptor.DuplexStreamingServerHandler(
            new EmptyStreamReader<DataSetRequest>(),
            new DiscardingStreamWriter<DataSetResponse>(),
            new StubServerCallContext(LatticeMethod),
            (_, _, _) =>
            {
                reached = true;
                return Task.CompletedTask;
            });

        Assert.That(reached, Is.True);
    }

    [Test]
    public async Task A_non_data_api_streaming_method_is_passed_through_without_an_auth_check()
    {
        var authorizer = Authorizer(allow: false);
        var interceptor = Create(authorizer);
        var expected = new DataSetResponse();

        // A denying authorizer proves the pass-through is real: a foreign method
        // must not be consulted at all, rather than being consulted and allowed.
        var result = await interceptor.ClientStreamingServerHandler(
            new EmptyStreamReader<DataSetRequest>(),
            new StubServerCallContext(ForeignMethod),
            (_, _) => Task.FromResult(expected));

        Assert.That(result, Is.SameAs(expected));
        await authorizer.DidNotReceive().IsAuthorizedAsync(
            Arg.Any<LatticeDataApiAuthorizationContext>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Streaming_enforcement_is_skipped_when_authorization_is_turned_off()
    {
        var authorizer = Authorizer(allow: false);
        var interceptor = Create(authorizer, requireAuthorization: false);
        var expected = new DataSetResponse();

        var result = await interceptor.ClientStreamingServerHandler(
            new EmptyStreamReader<DataSetRequest>(),
            new StubServerCallContext(LatticeMethod),
            (_, _) => Task.FromResult(expected));

        Assert.That(result, Is.SameAs(expected));
        await authorizer.DidNotReceive().IsAuthorizedAsync(
            Arg.Any<LatticeDataApiAuthorizationContext>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task An_authorized_server_streaming_call_reaches_the_continuation()
    {
        // The denial arm above proves the gate closes. Nothing proved it opens:
        // an interceptor that threw on every server-streaming call would pass
        // that test and fail every real request, and the shape most likely to
        // regress here is the one the service actually uses for its drains.
        var interceptor = Create(Authorizer(allow: true));
        var written = new List<DataSetResponse>();
        var expected = new DataSetResponse();

        await interceptor.ServerStreamingServerHandler(
            new DataSetRequest { TreeId = "t", Key = "k", Value = [1] },
            new RecordingStreamWriter<DataSetResponse>(written),
            new StubServerCallContext(LatticeMethod),
            async (_, stream, _) => await stream.WriteAsync(expected));

        Assert.That(written, Is.EqualTo(new[] { expected }).AsCollection);
    }

    [Test]
    public async Task A_non_data_api_server_streaming_method_is_passed_through_without_an_auth_check()
    {
        // Every streaming shape has its own pass-through branch, so proving it
        // on one shape says nothing about the others. A denying authorizer
        // makes the pass-through observable: were the branch missing, the call
        // would be refused rather than forwarded.
        var authorizer = Authorizer(allow: false);
        var interceptor = Create(authorizer);
        var reached = false;

        await interceptor.ServerStreamingServerHandler(
            new DataSetRequest { TreeId = "t", Key = "k", Value = [1] },
            new DiscardingStreamWriter<DataSetResponse>(),
            new StubServerCallContext(ForeignMethod),
            (_, _, _) =>
            {
                reached = true;
                return Task.CompletedTask;
            });

        Assert.That(reached, Is.True, "a foreign method must be forwarded, not refused");
        await authorizer.DidNotReceive().IsAuthorizedAsync(
            Arg.Any<LatticeDataApiAuthorizationContext>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_non_data_api_duplex_streaming_method_is_passed_through_without_an_auth_check()
    {
        var authorizer = Authorizer(allow: false);
        var interceptor = Create(authorizer);
        var reached = false;

        await interceptor.DuplexStreamingServerHandler(
            new EmptyStreamReader<DataSetRequest>(),
            new DiscardingStreamWriter<DataSetResponse>(),
            new StubServerCallContext(ForeignMethod),
            (_, _, _) =>
            {
                reached = true;
                return Task.CompletedTask;
            });

        Assert.That(reached, Is.True, "a foreign method must be forwarded, not refused");
        await authorizer.DidNotReceive().IsAuthorizedAsync(
            Arg.Any<LatticeDataApiAuthorizationContext>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_server_streaming_call_describes_itself_to_the_authorizer()
    {
        // The authorizer is the whole point of the interceptor, and a host
        // writes its policy against these three fields. Every existing test
        // matches the context with Arg.Any, so nothing asserted that the
        // interceptor populates it at all: an interceptor that handed over a
        // default context would satisfy all of them while making every
        // per-operation and per-tree policy decide on nothing.
        LatticeDataApiAuthorizationContext observed = default;
        var authorizer = Substitute.For<ILatticeDataApiAuthorizer>();
        authorizer
            .IsAuthorizedAsync(Arg.Any<LatticeDataApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                observed = call.Arg<LatticeDataApiAuthorizationContext>();
                return Task.FromResult(true);
            });

        var interceptor = Create(authorizer);
        var context = new StubServerCallContext(LatticeMethod);

        await interceptor.ServerStreamingServerHandler(
            new DataSetRequest { TreeId = "tree-7", Key = "k", Value = [1] },
            new DiscardingStreamWriter<DataSetResponse>(),
            context,
            (_, _, _) => Task.CompletedTask);

        Assert.Multiple(() =>
        {
            Assert.That(observed.Operation, Is.EqualTo(LatticeDataApiOperation.SetPoint));
            Assert.That(observed.TargetTreeId, Is.EqualTo("tree-7"));
            Assert.That(
                observed.Call,
                Is.SameAs(context),
                "the authorizer inspects headers and peer through the call context");
        });
    }

    [Test]
    public async Task A_duplex_streaming_call_describes_itself_without_a_single_request_message()
    {
        // A duplex call has no one request to describe, so the interceptor
        // passes `default!` and the description has to come from the method
        // name alone. The tree id is genuinely unknown here, and null is the
        // honest answer: a deny-by-default policy must be able to tell "no tree
        // was named" from "the root tree was named".
        LatticeDataApiAuthorizationContext observed = default;
        var authorizer = Substitute.For<ILatticeDataApiAuthorizer>();
        authorizer
            .IsAuthorizedAsync(Arg.Any<LatticeDataApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                observed = call.Arg<LatticeDataApiAuthorizationContext>();
                return Task.FromResult(true);
            });

        var interceptor = Create(authorizer);

        await interceptor.DuplexStreamingServerHandler(
            new EmptyStreamReader<DataSetRequest>(),
            new DiscardingStreamWriter<DataSetResponse>(),
            new StubServerCallContext(LatticeMethod),
            (_, _, _) => Task.CompletedTask);

        Assert.Multiple(() =>
        {
            Assert.That(observed.Operation, Is.EqualTo(LatticeDataApiOperation.SetPoint));
            Assert.That(observed.TargetTreeId, Is.Null, "no request message means no target tree to report");
        });
    }

    [Test]
    public void The_authorization_context_refuses_a_null_call()
    {
        // The context is a struct, so a caller can reach a default instance
        // without the constructor. The guard is what stops a half-built context
        // being handed to a policy that would then read a null call.
        Assert.Throws<ArgumentNullException>(() =>
            _ = new LatticeDataApiAuthorizationContext(null!, LatticeDataApiOperation.GetPoint, "t"));
    }

    [Test]
    public void The_authorization_context_carries_the_fields_a_policy_reads()
    {
        var call = new StubServerCallContext(LatticeMethod);
        var context = new LatticeDataApiAuthorizationContext(call, LatticeDataApiOperation.DeleteRange, "tree-3");

        Assert.Multiple(() =>
        {
            Assert.That(context.Call, Is.SameAs(call));
            Assert.That(context.Operation, Is.EqualTo(LatticeDataApiOperation.DeleteRange));
            Assert.That(context.TargetTreeId, Is.EqualTo("tree-3"));
        });
    }

    [Test]
    public void The_authorization_context_accepts_a_call_that_targets_no_single_tree()
    {
        var call = new StubServerCallContext(LatticeMethod);
        var context = new LatticeDataApiAuthorizationContext(
            call,
            LatticeDataApiOperation.SetManyAtomicCrossTree,
            targetTreeId: null);

        Assert.That(context.TargetTreeId, Is.Null);
    }

    /// <summary>
    /// An inbound request stream that is already complete. The authorization
    /// decision is taken before the first message is read, so the streaming
    /// handlers never need one.
    /// </summary>
    private sealed class EmptyStreamReader<T> : IAsyncStreamReader<T>
    {
        public T Current => throw new InvalidOperationException("The stub stream is empty.");

        public Task<bool> MoveNext(CancellationToken cancellationToken) => Task.FromResult(false);
    }

    /// <summary>An outbound response stream that discards everything written to it.</summary>
    private sealed class DiscardingStreamWriter<T> : IServerStreamWriter<T>
    {
        public WriteOptions? WriteOptions { get; set; }

        public Task WriteAsync(T message) => Task.CompletedTask;
    }

    /// <summary>
    /// An outbound response stream that records what the continuation wrote, so
    /// an authorized streaming call can be proven to have reached the service
    /// rather than merely to have not thrown.
    /// </summary>
    private sealed class RecordingStreamWriter<T>(List<T> written) : IServerStreamWriter<T>
    {
        public WriteOptions? WriteOptions { get; set; }

        public Task WriteAsync(T message)
        {
            written.Add(message);
            return Task.CompletedTask;
        }
    }

    /// <summary>
    /// A minimal <see cref="IOptionsMonitor{TOptions}"/> that always reports a fixed
    /// value; the interceptor reads only <see cref="IOptionsMonitor{TOptions}.CurrentValue"/>.
    /// </summary>
    private sealed class StaticOptionsMonitor(LatticeDataApiGrpcOptions value)
        : IOptionsMonitor<LatticeDataApiGrpcOptions>
    {
        public LatticeDataApiGrpcOptions CurrentValue { get; } = value;

        public LatticeDataApiGrpcOptions Get(string? name) => CurrentValue;

        public IDisposable? OnChange(Action<LatticeDataApiGrpcOptions, string?> listener) => null;
    }
}
