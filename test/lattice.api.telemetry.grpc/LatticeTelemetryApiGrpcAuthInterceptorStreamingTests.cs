using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Api.Telemetry.Grpc.Tests;

/// <summary>
/// Behavioural regression coverage proving <see cref="LatticeTelemetryApiGrpcAuthInterceptor"/>
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
public sealed class LatticeTelemetryApiGrpcAuthInterceptorStreamingTests
{
    private static readonly string LatticeMethod =
        TelemetryGrpcTestSupport.FullMethod(LatticeTelemetryGrpcMethods.QueryMethodName);

    private const string ForeignMethod = "/some.other.service/Ping";

    private static LatticeTelemetryApiGrpcAuthInterceptor Create(
        ILatticeTelemetryApiAuthorizer authorizer,
        bool requireAuthorization = true)
        => new(
            authorizer,
            new StaticOptionsMonitor(new LatticeTelemetryApiGrpcOptions
            {
                RequireAuthorization = requireAuthorization,
            }),
            NullLogger<LatticeTelemetryApiGrpcAuthInterceptor>.Instance);

    private static ILatticeTelemetryApiAuthorizer Authorizer(bool allow)
    {
        var authorizer = Substitute.For<ILatticeTelemetryApiAuthorizer>();
        authorizer
            .IsAuthorizedAsync(Arg.Any<LatticeTelemetryApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .Returns(allow);
        return authorizer;
    }

    private static TelemetryQueryRequest Request() => new() { QueryId = "lattice.ops.rate" };

    [Test]
    public void A_denied_server_streaming_call_is_rejected_with_permission_denied()
    {
        var interceptor = Create(Authorizer(allow: false));
        var reached = false;

        var exception = Assert.ThrowsAsync<RpcException>(() => interceptor.ServerStreamingServerHandler(
            Request(),
            new DiscardingStreamWriter<TelemetryQueryResponse>(),
            new FakeServerCallContext(LatticeMethod),
            (_, _, _) =>
            {
                reached = true;
                return Task.CompletedTask;
            }));

        Assert.Multiple(() =>
        {
            Assert.That(exception!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(reached, Is.False, "the continuation must not run for a denied call");
        });
    }

    [Test]
    public void A_denied_client_streaming_call_is_rejected_with_permission_denied()
    {
        var interceptor = Create(Authorizer(allow: false));
        var reached = false;

        var exception = Assert.ThrowsAsync<RpcException>(() => interceptor.ClientStreamingServerHandler(
            new EmptyStreamReader<TelemetryQueryRequest>(),
            new FakeServerCallContext(LatticeMethod),
            (_, _) =>
            {
                reached = true;
                return Task.FromResult(new TelemetryQueryResponse
                {
                    QueryId = "lattice.ops.rate",
                    Scope = TelemetryTenantScope.PinnedTo("t", TelemetryTenantVisibility.ActiveTenant),
                    Series = [],
                });
            }));

        Assert.Multiple(() =>
        {
            Assert.That(exception!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(reached, Is.False, "the continuation must not run for a denied call");
        });
    }

    [Test]
    public void A_denied_duplex_streaming_call_is_rejected_with_permission_denied()
    {
        var interceptor = Create(Authorizer(allow: false));
        var reached = false;

        var exception = Assert.ThrowsAsync<RpcException>(() => interceptor.DuplexStreamingServerHandler(
            new EmptyStreamReader<TelemetryQueryRequest>(),
            new DiscardingStreamWriter<TelemetryQueryResponse>(),
            new FakeServerCallContext(LatticeMethod),
            (_, _, _) =>
            {
                reached = true;
                return Task.CompletedTask;
            }));

        Assert.Multiple(() =>
        {
            Assert.That(exception!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(reached, Is.False, "the continuation must not run for a denied call");
        });
    }

    [Test]
    public async Task An_authorized_server_streaming_call_reaches_the_continuation()
    {
        var interceptor = Create(Authorizer(allow: true));
        var reached = false;

        await interceptor.ServerStreamingServerHandler(
            Request(),
            new DiscardingStreamWriter<TelemetryQueryResponse>(),
            new FakeServerCallContext(LatticeMethod),
            (_, _, _) =>
            {
                reached = true;
                return Task.CompletedTask;
            });

        Assert.That(reached, Is.True);
    }

    [Test]
    public async Task An_authorized_client_streaming_call_reaches_the_continuation()
    {
        var interceptor = Create(Authorizer(allow: true));
        var expected = new TelemetryQueryResponse
        {
            QueryId = "lattice.ops.rate",
            Scope = TelemetryTenantScope.PinnedTo("t", TelemetryTenantVisibility.ActiveTenant),
            Series = [],
        };

        var result = await interceptor.ClientStreamingServerHandler(
            new EmptyStreamReader<TelemetryQueryRequest>(),
            new FakeServerCallContext(LatticeMethod),
            (_, _) => Task.FromResult(expected));

        Assert.That(result, Is.SameAs(expected));
    }

    [Test]
    public async Task An_authorized_duplex_streaming_call_reaches_the_continuation()
    {
        var interceptor = Create(Authorizer(allow: true));
        var reached = false;

        await interceptor.DuplexStreamingServerHandler(
            new EmptyStreamReader<TelemetryQueryRequest>(),
            new DiscardingStreamWriter<TelemetryQueryResponse>(),
            new FakeServerCallContext(LatticeMethod),
            (_, _, _) =>
            {
                reached = true;
                return Task.CompletedTask;
            });

        Assert.That(reached, Is.True);
    }

    [Test]
    public async Task A_foreign_server_streaming_method_is_passed_through_without_an_auth_check()
    {
        var authorizer = Authorizer(allow: false);
        var interceptor = Create(authorizer);
        var reached = false;

        // A denying authorizer proves the pass-through is real: a foreign method
        // must not be consulted at all, rather than being consulted and allowed.
        await interceptor.ServerStreamingServerHandler(
            Request(),
            new DiscardingStreamWriter<TelemetryQueryResponse>(),
            new FakeServerCallContext(ForeignMethod),
            (_, _, _) =>
            {
                reached = true;
                return Task.CompletedTask;
            });

        Assert.That(reached, Is.True);
        await authorizer.DidNotReceive().IsAuthorizedAsync(
            Arg.Any<LatticeTelemetryApiAuthorizationContext>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_foreign_client_streaming_method_is_passed_through_without_an_auth_check()
    {
        var authorizer = Authorizer(allow: false);
        var interceptor = Create(authorizer);
        var expected = new TelemetryQueryResponse
        {
            QueryId = "lattice.ops.rate",
            Scope = TelemetryTenantScope.PinnedTo("t", TelemetryTenantVisibility.ActiveTenant),
            Series = [],
        };

        var result = await interceptor.ClientStreamingServerHandler(
            new EmptyStreamReader<TelemetryQueryRequest>(),
            new FakeServerCallContext(ForeignMethod),
            (_, _) => Task.FromResult(expected));

        Assert.That(result, Is.SameAs(expected));
        await authorizer.DidNotReceive().IsAuthorizedAsync(
            Arg.Any<LatticeTelemetryApiAuthorizationContext>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_foreign_duplex_streaming_method_is_passed_through_without_an_auth_check()
    {
        var authorizer = Authorizer(allow: false);
        var interceptor = Create(authorizer);
        var reached = false;

        await interceptor.DuplexStreamingServerHandler(
            new EmptyStreamReader<TelemetryQueryRequest>(),
            new DiscardingStreamWriter<TelemetryQueryResponse>(),
            new FakeServerCallContext(ForeignMethod),
            (_, _, _) =>
            {
                reached = true;
                return Task.CompletedTask;
            });

        Assert.That(reached, Is.True);
        await authorizer.DidNotReceive().IsAuthorizedAsync(
            Arg.Any<LatticeTelemetryApiAuthorizationContext>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Streaming_enforcement_is_skipped_when_authorization_is_turned_off()
    {
        var authorizer = Authorizer(allow: false);
        var interceptor = Create(authorizer, requireAuthorization: false);
        var reached = false;

        await interceptor.ServerStreamingServerHandler(
            Request(),
            new DiscardingStreamWriter<TelemetryQueryResponse>(),
            new FakeServerCallContext(LatticeMethod),
            (_, _, _) =>
            {
                reached = true;
                return Task.CompletedTask;
            });

        Assert.That(reached, Is.True);
        await authorizer.DidNotReceive().IsAuthorizedAsync(
            Arg.Any<LatticeTelemetryApiAuthorizationContext>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public void A_cancelled_authorizer_fails_a_streaming_call_with_cancelled()
    {
        var authorizer = Substitute.For<ILatticeTelemetryApiAuthorizer>();
        authorizer
            .IsAuthorizedAsync(Arg.Any<LatticeTelemetryApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .Returns<bool>(_ => throw new OperationCanceledException());
        var interceptor = Create(authorizer);
        var reached = false;

        var exception = Assert.ThrowsAsync<RpcException>(() => interceptor.DuplexStreamingServerHandler(
            new EmptyStreamReader<TelemetryQueryRequest>(),
            new DiscardingStreamWriter<TelemetryQueryResponse>(),
            new FakeServerCallContext(LatticeMethod),
            (_, _, _) =>
            {
                reached = true;
                return Task.CompletedTask;
            }));

        Assert.Multiple(() =>
        {
            Assert.That(exception!.StatusCode, Is.EqualTo(StatusCode.Cancelled));
            Assert.That(reached, Is.False, "a cancelled authorization must not admit the call");
        });
    }

    [Test]
    public void A_streaming_handler_rejects_a_null_argument()
    {
        var interceptor = Create(Authorizer(allow: true));
        var context = new FakeServerCallContext(LatticeMethod);

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await interceptor.ServerStreamingServerHandler<TelemetryQueryRequest, TelemetryQueryResponse>(
                    Request(), new DiscardingStreamWriter<TelemetryQueryResponse>(), context, null!),
                Throws.InstanceOf<ArgumentNullException>());
            Assert.That(
                async () => await interceptor.ClientStreamingServerHandler<TelemetryQueryRequest, TelemetryQueryResponse>(
                    null!, context, (_, _) => Task.FromResult<TelemetryQueryResponse>(null!)),
                Throws.InstanceOf<ArgumentNullException>());
            Assert.That(
                async () => await interceptor.DuplexStreamingServerHandler<TelemetryQueryRequest, TelemetryQueryResponse>(
                    new EmptyStreamReader<TelemetryQueryRequest>(), null!, context, (_, _, _) => Task.CompletedTask),
                Throws.InstanceOf<ArgumentNullException>());
        });
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
    /// A minimal <see cref="IOptionsMonitor{TOptions}"/> that always reports a fixed
    /// value; the interceptor reads only <see cref="IOptionsMonitor{TOptions}.CurrentValue"/>.
    /// </summary>
    private sealed class StaticOptionsMonitor(LatticeTelemetryApiGrpcOptions value)
        : IOptionsMonitor<LatticeTelemetryApiGrpcOptions>
    {
        public LatticeTelemetryApiGrpcOptions CurrentValue { get; } = value;

        public LatticeTelemetryApiGrpcOptions Get(string? name) => CurrentValue;

        public IDisposable? OnChange(Action<LatticeTelemetryApiGrpcOptions, string?> listener) => null;
    }
}
