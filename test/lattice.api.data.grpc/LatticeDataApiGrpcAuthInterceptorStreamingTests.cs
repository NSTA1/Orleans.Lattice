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
/// <see cref="Grpc.Core.Interceptors.Interceptor"/> implements every handler as a
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
    private sealed class StaticOptionsMonitor(LatticeDataApiGrpcOptions value)
        : IOptionsMonitor<LatticeDataApiGrpcOptions>
    {
        public LatticeDataApiGrpcOptions CurrentValue { get; } = value;

        public LatticeDataApiGrpcOptions Get(string? name) => CurrentValue;

        public IDisposable? OnChange(Action<LatticeDataApiGrpcOptions, string?> listener) => null;
    }
}
