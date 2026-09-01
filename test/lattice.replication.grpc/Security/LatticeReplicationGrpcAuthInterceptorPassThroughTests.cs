using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;

namespace Orleans.Lattice.Replication.Grpc.Tests.Security;

/// <summary>
/// Unit-level coverage for the two interceptor pass-through paths:
/// (a) the call targets a non-LatticeReplication method, and (b) the
/// host has opted out of authentication. Both must invoke the
/// continuation without consulting the secret provider.
/// </summary>
[TestFixture]
public class LatticeReplicationGrpcAuthInterceptorPassThroughTests
{
    private static IOptionsMonitor<LatticeReplicationSecurityOptions> OptionsFor(LatticeReplicationSecurityOptions o)
    {
        var m = Substitute.For<IOptionsMonitor<LatticeReplicationSecurityOptions>>();
        m.CurrentValue.Returns(o);
        return m;
    }

    private sealed class StubServerCallContext : ServerCallContext
    {
        private readonly string _method;
        public StubServerCallContext(string method) { _method = method; }
        protected override string MethodCore => _method;
        protected override string HostCore => string.Empty;
        protected override string PeerCore => "ipv4:127.0.0.1:0";
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override global::Grpc.Core.Metadata RequestHeadersCore { get; } = new();
        protected override CancellationToken CancellationTokenCore => CancellationToken.None;
        protected override global::Grpc.Core.Metadata ResponseTrailersCore { get; } = new();
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => new(string.Empty, new Dictionary<string, List<AuthProperty>>());
        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) => null!;
        protected override Task WriteResponseHeadersAsyncCore(global::Grpc.Core.Metadata responseHeaders) => Task.CompletedTask;
    }

    [Test]
    public async Task UnaryServerHandler_passes_through_for_non_replication_method_without_consulting_secrets()
    {
        var secrets = Substitute.For<IReplicationSecretProvider>();
        var interceptor = new LatticeReplicationGrpcAuthInterceptor(
            secrets,
            OptionsFor(new LatticeReplicationSecurityOptions { RequireAuthentication = true }),
            NullLogger<LatticeReplicationGrpcAuthInterceptor>.Instance);

        var ctx = new StubServerCallContext("/some.other.Service/Method");
        var called = false;
        var result = await interceptor.UnaryServerHandler<object, string>(
            request: new object(),
            context: ctx,
            continuation: (_, _) => { called = true; return Task.FromResult("ok"); });

        Assert.That(called, Is.True);
        Assert.That(result, Is.EqualTo("ok"));
        _ = secrets.DidNotReceiveWithAnyArgs().IsAcceptedAsync(default!, default);
    }

    [Test]
    public async Task UnaryServerHandler_passes_through_when_RequireAuthentication_is_false()
    {
        var secrets = Substitute.For<IReplicationSecretProvider>();
        var interceptor = new LatticeReplicationGrpcAuthInterceptor(
            secrets,
            OptionsFor(new LatticeReplicationSecurityOptions { RequireAuthentication = false }),
            NullLogger<LatticeReplicationGrpcAuthInterceptor>.Instance);

        var ctx = new StubServerCallContext("/orleans.lattice.replication.LatticeReplication/Push");
        var called = false;
        var result = await interceptor.UnaryServerHandler<object, string>(
            request: new object(),
            context: ctx,
            continuation: (_, _) => { called = true; return Task.FromResult("ok"); });

        Assert.That(called, Is.True);
        Assert.That(result, Is.EqualTo("ok"));
        _ = secrets.DidNotReceiveWithAnyArgs().IsAcceptedAsync(default!, default);
    }

    [Test]
    public async Task ServerStreamingServerHandler_passes_through_for_non_replication_method_without_consulting_secrets()
    {
        var secrets = Substitute.For<IReplicationSecretProvider>();
        var interceptor = new LatticeReplicationGrpcAuthInterceptor(
            secrets,
            OptionsFor(new LatticeReplicationSecurityOptions { RequireAuthentication = true }),
            NullLogger<LatticeReplicationGrpcAuthInterceptor>.Instance);

        var ctx = new StubServerCallContext("/some.other.Service/Stream");
        var writer = new NullStreamWriter<string>();
        var called = false;

        await interceptor.ServerStreamingServerHandler<object, string>(
            request: new object(),
            responseStream: writer,
            context: ctx,
            continuation: (_, _, _) => { called = true; return Task.CompletedTask; });

        Assert.That(called, Is.True);
        _ = secrets.DidNotReceiveWithAnyArgs().IsAcceptedAsync(default!, default);
    }

    private sealed class NullStreamWriter<T> : IServerStreamWriter<T>
    {
        public WriteOptions? WriteOptions { get; set; }

        public Task WriteAsync(T message) => Task.CompletedTask;
    }

    [Test]
    public void UnaryServerHandler_throws_when_request_is_null()
    {
        var interceptor = new LatticeReplicationGrpcAuthInterceptor(
            Substitute.For<IReplicationSecretProvider>(),
            OptionsFor(new LatticeReplicationSecurityOptions()),
            NullLogger<LatticeReplicationGrpcAuthInterceptor>.Instance);

        Assert.That(
            async () => await interceptor.UnaryServerHandler<object, string>(
                request: null!,
                context: new StubServerCallContext("/orleans.lattice.replication.LatticeReplication/Push"),
                continuation: (_, _) => Task.FromResult("ok")),
            Throws.ArgumentNullException);
    }

    [Test]
    public void UnaryServerHandler_throws_when_context_is_null()
    {
        var interceptor = new LatticeReplicationGrpcAuthInterceptor(
            Substitute.For<IReplicationSecretProvider>(),
            OptionsFor(new LatticeReplicationSecurityOptions()),
            NullLogger<LatticeReplicationGrpcAuthInterceptor>.Instance);

        Assert.That(
            async () => await interceptor.UnaryServerHandler<object, string>(
                request: new object(),
                context: null!,
                continuation: (_, _) => Task.FromResult("ok")),
            Throws.ArgumentNullException);
    }

    [Test]
    public void UnaryServerHandler_throws_when_continuation_is_null()
    {
        var interceptor = new LatticeReplicationGrpcAuthInterceptor(
            Substitute.For<IReplicationSecretProvider>(),
            OptionsFor(new LatticeReplicationSecurityOptions()),
            NullLogger<LatticeReplicationGrpcAuthInterceptor>.Instance);

        Assert.That(
            async () => await interceptor.UnaryServerHandler<object, string>(
                request: new object(),
                context: new StubServerCallContext("/orleans.lattice.replication.LatticeReplication/Push"),
                continuation: null!),
            Throws.ArgumentNullException);
    }
}
