using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Replication.Grpc.Tests.Security;

/// <summary>
/// Behavioural regression coverage proving <see cref="LatticeReplicationGrpcAuthInterceptor"/>
/// enforces the shared-secret credential on the <em>streaming</em> call shapes,
/// not only on unary calls.
/// <para>
/// <c>Grpc.Core.Interceptors.Interceptor</c> implements every handler as a
/// pass-through to the continuation, so a handler the interceptor does not
/// override admits the call with no credential check whatsoever. An interceptor
/// that gated only <c>UnaryServerHandler</c> would therefore leave the
/// server-streaming, client-streaming, and duplex shapes wide open on the
/// replication receive path - a gap that stays invisible until a streaming RPC
/// is added to one of the already-gated replication services. These tests fail
/// if any streaming shape stops enforcing.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeReplicationGrpcAuthInterceptorStreamingTests
{
    private const string ReplicationMethod =
        "/" + LatticeReplicationGrpcMethod.ServiceName + "/Push";

    private const string ForeignMethod = "/some.other.Service/Stream";

    private const string GoodSecret = "accepted-secret";

    private static LatticeReplicationGrpcAuthInterceptor Create(
        IReplicationSecretProvider secrets,
        bool requireAuthentication = true)
    {
        var options = Substitute.For<IOptionsMonitor<LatticeReplicationSecurityOptions>>();
        options.CurrentValue.Returns(new LatticeReplicationSecurityOptions
        {
            RequireAuthentication = requireAuthentication,
        });
        return new LatticeReplicationGrpcAuthInterceptor(
            secrets,
            options,
            NullLogger<LatticeReplicationGrpcAuthInterceptor>.Instance);
    }

    private static IReplicationSecretProvider Secrets(bool accept)
    {
        var secrets = Substitute.For<IReplicationSecretProvider>();
        secrets.IsAcceptedAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(accept);
        return secrets;
    }

    private static StubServerCallContext Context(string method, string? secret = GoodSecret)
    {
        var headers = new global::Grpc.Core.Metadata();
        if (secret is not null)
        {
            headers.Add(LatticeReplicationGrpcMetadataNames.SecretHeader, secret);
        }
        return new StubServerCallContext(method, headers);
    }

    [Test]
    public void A_client_streaming_call_with_no_credential_is_rejected_as_unauthenticated()
    {
        var interceptor = Create(Secrets(accept: true));
        var reached = false;

        var exception = Assert.ThrowsAsync<RpcException>(() => interceptor.ClientStreamingServerHandler<object, string>(
            new EmptyStreamReader<object>(),
            Context(ReplicationMethod, secret: null),
            (_, _) =>
            {
                reached = true;
                return Task.FromResult("ok");
            }));

        Assert.Multiple(() =>
        {
            Assert.That(exception!.StatusCode, Is.EqualTo(StatusCode.Unauthenticated));
            Assert.That(reached, Is.False, "the continuation must not run for an uncredentialed call");
        });
    }

    [Test]
    public void A_duplex_streaming_call_with_no_credential_is_rejected_as_unauthenticated()
    {
        var interceptor = Create(Secrets(accept: true));
        var reached = false;

        var exception = Assert.ThrowsAsync<RpcException>(() => interceptor.DuplexStreamingServerHandler<object, string>(
            new EmptyStreamReader<object>(),
            new NullStreamWriter<string>(),
            Context(ReplicationMethod, secret: null),
            (_, _, _) =>
            {
                reached = true;
                return Task.CompletedTask;
            }));

        Assert.Multiple(() =>
        {
            Assert.That(exception!.StatusCode, Is.EqualTo(StatusCode.Unauthenticated));
            Assert.That(reached, Is.False, "the continuation must not run for an uncredentialed call");
        });
    }

    [Test]
    public void A_server_streaming_call_with_no_credential_is_rejected_as_unauthenticated()
    {
        var interceptor = Create(Secrets(accept: true));
        var reached = false;

        var exception = Assert.ThrowsAsync<RpcException>(() => interceptor.ServerStreamingServerHandler<object, string>(
            new object(),
            new NullStreamWriter<string>(),
            Context(ReplicationMethod, secret: null),
            (_, _, _) =>
            {
                reached = true;
                return Task.CompletedTask;
            }));

        Assert.Multiple(() =>
        {
            Assert.That(exception!.StatusCode, Is.EqualTo(StatusCode.Unauthenticated));
            Assert.That(reached, Is.False, "the continuation must not run for an uncredentialed call");
        });
    }

    [Test]
    public void A_client_streaming_call_with_an_unaccepted_credential_is_rejected_with_permission_denied()
    {
        var interceptor = Create(Secrets(accept: false));
        var reached = false;

        var exception = Assert.ThrowsAsync<RpcException>(() => interceptor.ClientStreamingServerHandler<object, string>(
            new EmptyStreamReader<object>(),
            Context(ReplicationMethod, secret: "wrong"),
            (_, _) =>
            {
                reached = true;
                return Task.FromResult("ok");
            }));

        Assert.Multiple(() =>
        {
            Assert.That(exception!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(reached, Is.False, "the continuation must not run for a rejected credential");
        });
    }

    [Test]
    public void A_duplex_streaming_call_with_an_unaccepted_credential_is_rejected_with_permission_denied()
    {
        var interceptor = Create(Secrets(accept: false));
        var reached = false;

        var exception = Assert.ThrowsAsync<RpcException>(() => interceptor.DuplexStreamingServerHandler<object, string>(
            new EmptyStreamReader<object>(),
            new NullStreamWriter<string>(),
            Context(ReplicationMethod, secret: "wrong"),
            (_, _, _) =>
            {
                reached = true;
                return Task.CompletedTask;
            }));

        Assert.Multiple(() =>
        {
            Assert.That(exception!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(reached, Is.False, "the continuation must not run for a rejected credential");
        });
    }

    [Test]
    public async Task An_accepted_client_streaming_call_reaches_the_continuation()
    {
        var secrets = Secrets(accept: true);
        var interceptor = Create(secrets);

        var result = await interceptor.ClientStreamingServerHandler<object, string>(
            new EmptyStreamReader<object>(),
            Context(ReplicationMethod),
            (_, _) => Task.FromResult("ok"));

        Assert.That(result, Is.EqualTo("ok"));
        await secrets.Received(1).IsAcceptedAsync(GoodSecret, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task An_accepted_duplex_streaming_call_reaches_the_continuation()
    {
        var secrets = Secrets(accept: true);
        var interceptor = Create(secrets);
        var reached = false;

        await interceptor.DuplexStreamingServerHandler<object, string>(
            new EmptyStreamReader<object>(),
            new NullStreamWriter<string>(),
            Context(ReplicationMethod),
            (_, _, _) =>
            {
                reached = true;
                return Task.CompletedTask;
            });

        Assert.That(reached, Is.True);
        await secrets.Received(1).IsAcceptedAsync(GoodSecret, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task An_accepted_server_streaming_call_reaches_the_continuation()
    {
        var secrets = Secrets(accept: true);
        var interceptor = Create(secrets);
        var reached = false;

        await interceptor.ServerStreamingServerHandler<object, string>(
            new object(),
            new NullStreamWriter<string>(),
            Context(ReplicationMethod),
            (_, _, _) =>
            {
                reached = true;
                return Task.CompletedTask;
            });

        Assert.That(reached, Is.True);
        await secrets.Received(1).IsAcceptedAsync(GoodSecret, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_foreign_client_streaming_method_is_passed_through_without_consulting_secrets()
    {
        // A rejecting secret provider proves the pass-through is real: a foreign
        // method must not be consulted at all, rather than consulted and allowed.
        var secrets = Secrets(accept: false);
        var interceptor = Create(secrets);

        var result = await interceptor.ClientStreamingServerHandler<object, string>(
            new EmptyStreamReader<object>(),
            Context(ForeignMethod, secret: null),
            (_, _) => Task.FromResult("ok"));

        Assert.That(result, Is.EqualTo("ok"));
        _ = secrets.DidNotReceiveWithAnyArgs().IsAcceptedAsync(default!, default);
    }

    [Test]
    public async Task A_foreign_duplex_streaming_method_is_passed_through_without_consulting_secrets()
    {
        var secrets = Secrets(accept: false);
        var interceptor = Create(secrets);
        var reached = false;

        await interceptor.DuplexStreamingServerHandler<object, string>(
            new EmptyStreamReader<object>(),
            new NullStreamWriter<string>(),
            Context(ForeignMethod, secret: null),
            (_, _, _) =>
            {
                reached = true;
                return Task.CompletedTask;
            });

        Assert.That(reached, Is.True);
        _ = secrets.DidNotReceiveWithAnyArgs().IsAcceptedAsync(default!, default);
    }

    [Test]
    public async Task Streaming_enforcement_is_skipped_when_authentication_is_turned_off()
    {
        var secrets = Secrets(accept: false);
        var interceptor = Create(secrets, requireAuthentication: false);

        var result = await interceptor.ClientStreamingServerHandler<object, string>(
            new EmptyStreamReader<object>(),
            Context(ReplicationMethod, secret: null),
            (_, _) => Task.FromResult("ok"));

        Assert.That(result, Is.EqualTo("ok"));
        _ = secrets.DidNotReceiveWithAnyArgs().IsAcceptedAsync(default!, default);
    }

    [Test]
    public void The_streaming_handlers_reject_a_null_argument()
    {
        var interceptor = Create(Secrets(accept: true));
        var context = Context(ReplicationMethod);

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await interceptor.ClientStreamingServerHandler<object, string>(
                    null!, context, (_, _) => Task.FromResult("ok")),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await interceptor.ClientStreamingServerHandler<object, string>(
                    new EmptyStreamReader<object>(), null!, (_, _) => Task.FromResult("ok")),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await interceptor.ClientStreamingServerHandler<object, string>(
                    new EmptyStreamReader<object>(), context, null!),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await interceptor.DuplexStreamingServerHandler<object, string>(
                    null!, new NullStreamWriter<string>(), context, (_, _, _) => Task.CompletedTask),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await interceptor.DuplexStreamingServerHandler<object, string>(
                    new EmptyStreamReader<object>(), null!, context, (_, _, _) => Task.CompletedTask),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await interceptor.DuplexStreamingServerHandler<object, string>(
                    new EmptyStreamReader<object>(), new NullStreamWriter<string>(), null!, (_, _, _) => Task.CompletedTask),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await interceptor.DuplexStreamingServerHandler<object, string>(
                    new EmptyStreamReader<object>(), new NullStreamWriter<string>(), context, null!),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await interceptor.ServerStreamingServerHandler<object, string>(
                    new object(), new NullStreamWriter<string>(), context, null!),
                Throws.ArgumentNullException);
        });
    }

    /// <summary>
    /// An inbound request stream that is already complete. The credential
    /// decision is taken before the first message is read, so the streaming
    /// handlers never need one.
    /// </summary>
    private sealed class EmptyStreamReader<T> : IAsyncStreamReader<T>
    {
        public T Current => throw new InvalidOperationException("The stub stream is empty.");

        public Task<bool> MoveNext(CancellationToken cancellationToken) => Task.FromResult(false);
    }

    /// <summary>An outbound response stream that discards everything written to it.</summary>
    private sealed class NullStreamWriter<T> : IServerStreamWriter<T>
    {
        public WriteOptions? WriteOptions { get; set; }

        public Task WriteAsync(T message) => Task.CompletedTask;
    }

    /// <summary>
    /// A minimal <see cref="ServerCallContext"/> carrying the method name and the
    /// inbound request headers the interceptor reads the credential from.
    /// </summary>
    private sealed class StubServerCallContext(string method, global::Grpc.Core.Metadata requestHeaders)
        : ServerCallContext
    {
        protected override string MethodCore { get; } = method;

        protected override string HostCore => string.Empty;

        protected override string PeerCore => "ipv4:127.0.0.1:0";

        protected override DateTime DeadlineCore => DateTime.MaxValue;

        protected override global::Grpc.Core.Metadata RequestHeadersCore { get; } = requestHeaders;

        protected override CancellationToken CancellationTokenCore => CancellationToken.None;

        protected override global::Grpc.Core.Metadata ResponseTrailersCore { get; } = new();

        protected override Status StatusCore { get; set; }

        protected override WriteOptions? WriteOptionsCore { get; set; }

        protected override AuthContext AuthContextCore =>
            new(string.Empty, new Dictionary<string, List<AuthProperty>>());

        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) => null!;

        protected override Task WriteResponseHeadersAsyncCore(global::Grpc.Core.Metadata responseHeaders) => Task.CompletedTask;
    }
}
