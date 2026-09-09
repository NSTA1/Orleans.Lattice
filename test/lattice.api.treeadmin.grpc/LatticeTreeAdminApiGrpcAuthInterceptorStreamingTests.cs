using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Api.TreeAdmin.Grpc.Tests;

/// <summary>
/// Behavioural regression coverage proving <see cref="LatticeTreeAdminApiGrpcAuthInterceptor"/>
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
public sealed class LatticeTreeAdminApiGrpcAuthInterceptorStreamingTests
{
    private const string LatticeMethod =
        "/" + LatticeTreeAdminGrpcMethods.ServiceName + "/" + LatticeTreeAdminGrpcMethods.GetShardHotnessMethodName;

    private const string ForeignMethod = "/some.other.service/Ping";

    private static LatticeTreeAdminApiGrpcAuthInterceptor Create(
        ILatticeTreeAdminApiAuthorizer authorizer,
        bool requireAuthorization = true)
    {
        var options = Substitute.For<IOptionsMonitor<LatticeTreeAdminApiGrpcOptions>>();
        options.CurrentValue.Returns(new LatticeTreeAdminApiGrpcOptions
        {
            RequireAuthorization = requireAuthorization,
        });
        return new LatticeTreeAdminApiGrpcAuthInterceptor(
            authorizer,
            options,
            NullLogger<LatticeTreeAdminApiGrpcAuthInterceptor>.Instance);
    }

    private static ILatticeTreeAdminApiAuthorizer Authorizer(bool allow)
    {
        var authorizer = Substitute.For<ILatticeTreeAdminApiAuthorizer>();
        authorizer
            .IsAuthorizedAsync(Arg.Any<LatticeTreeAdminApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .Returns(allow);
        return authorizer;
    }

    [Test]
    public void A_denied_server_streaming_call_is_rejected_with_permission_denied()
    {
        var interceptor = Create(Authorizer(allow: false));
        var reached = false;

        var exception = Assert.ThrowsAsync<RpcException>(() => interceptor.ServerStreamingServerHandler<object, string>(
            new object(),
            new DiscardingStreamWriter<string>(),
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

        var exception = Assert.ThrowsAsync<RpcException>(() => interceptor.ClientStreamingServerHandler<object, string>(
            new EmptyStreamReader<object>(),
            new FakeServerCallContext(LatticeMethod),
            (_, _) =>
            {
                reached = true;
                return Task.FromResult("ok");
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

        var exception = Assert.ThrowsAsync<RpcException>(() => interceptor.DuplexStreamingServerHandler<object, string>(
            new EmptyStreamReader<object>(),
            new DiscardingStreamWriter<string>(),
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

        await interceptor.ServerStreamingServerHandler<object, string>(
            new object(),
            new DiscardingStreamWriter<string>(),
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

        var result = await interceptor.ClientStreamingServerHandler<object, string>(
            new EmptyStreamReader<object>(),
            new FakeServerCallContext(LatticeMethod),
            (_, _) => Task.FromResult("ok"));

        Assert.That(result, Is.EqualTo("ok"));
    }

    [Test]
    public async Task An_authorized_duplex_streaming_call_reaches_the_continuation()
    {
        var interceptor = Create(Authorizer(allow: true));
        var reached = false;

        await interceptor.DuplexStreamingServerHandler<object, string>(
            new EmptyStreamReader<object>(),
            new DiscardingStreamWriter<string>(),
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
        await interceptor.ServerStreamingServerHandler<object, string>(
            new object(),
            new DiscardingStreamWriter<string>(),
            new FakeServerCallContext(ForeignMethod),
            (_, _, _) =>
            {
                reached = true;
                return Task.CompletedTask;
            });

        Assert.That(reached, Is.True);
        await authorizer.DidNotReceive().IsAuthorizedAsync(
            Arg.Any<LatticeTreeAdminApiAuthorizationContext>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_foreign_client_streaming_method_is_passed_through_without_an_auth_check()
    {
        var authorizer = Authorizer(allow: false);
        var interceptor = Create(authorizer);

        var result = await interceptor.ClientStreamingServerHandler<object, string>(
            new EmptyStreamReader<object>(),
            new FakeServerCallContext(ForeignMethod),
            (_, _) => Task.FromResult("ok"));

        Assert.That(result, Is.EqualTo("ok"));
        await authorizer.DidNotReceive().IsAuthorizedAsync(
            Arg.Any<LatticeTreeAdminApiAuthorizationContext>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_foreign_duplex_streaming_method_is_passed_through_without_an_auth_check()
    {
        var authorizer = Authorizer(allow: false);
        var interceptor = Create(authorizer);
        var reached = false;

        await interceptor.DuplexStreamingServerHandler<object, string>(
            new EmptyStreamReader<object>(),
            new DiscardingStreamWriter<string>(),
            new FakeServerCallContext(ForeignMethod),
            (_, _, _) =>
            {
                reached = true;
                return Task.CompletedTask;
            });

        Assert.That(reached, Is.True);
        await authorizer.DidNotReceive().IsAuthorizedAsync(
            Arg.Any<LatticeTreeAdminApiAuthorizationContext>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Streaming_enforcement_is_skipped_when_authorization_is_turned_off()
    {
        var authorizer = Authorizer(allow: false);
        var interceptor = Create(authorizer, requireAuthorization: false);
        var reached = false;

        await interceptor.ServerStreamingServerHandler<object, string>(
            new object(),
            new DiscardingStreamWriter<string>(),
            new FakeServerCallContext(LatticeMethod),
            (_, _, _) =>
            {
                reached = true;
                return Task.CompletedTask;
            });

        Assert.That(reached, Is.True);
        await authorizer.DidNotReceive().IsAuthorizedAsync(
            Arg.Any<LatticeTreeAdminApiAuthorizationContext>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public void A_cancelled_authorizer_fails_a_streaming_call_with_cancelled()
    {
        var authorizer = Substitute.For<ILatticeTreeAdminApiAuthorizer>();
        authorizer
            .IsAuthorizedAsync(Arg.Any<LatticeTreeAdminApiAuthorizationContext>(), Arg.Any<CancellationToken>())
            .Returns<bool>(_ => throw new OperationCanceledException());
        var interceptor = Create(authorizer);
        var reached = false;

        var exception = Assert.ThrowsAsync<RpcException>(() => interceptor.DuplexStreamingServerHandler<object, string>(
            new EmptyStreamReader<object>(),
            new DiscardingStreamWriter<string>(),
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
                async () => await interceptor.ServerStreamingServerHandler<object, string>(
                    new object(), new DiscardingStreamWriter<string>(), context, null!),
                Throws.InstanceOf<ArgumentNullException>());
            Assert.That(
                async () => await interceptor.ClientStreamingServerHandler<object, string>(
                    null!, context, (_, _) => Task.FromResult("ok")),
                Throws.InstanceOf<ArgumentNullException>());
            Assert.That(
                async () => await interceptor.DuplexStreamingServerHandler<object, string>(
                    new EmptyStreamReader<object>(), null!, context, (_, _, _) => Task.CompletedTask),
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
}
