using Grpc.Core;

namespace Orleans.Lattice.Api.Backup.Grpc.Tests;

/// <summary>
/// Minimal in-process <see cref="ServerCallContext"/> test double for driving the
/// server-side gRPC service and the auth interceptor directly - no live server.
/// Carries a method name, request headers, and a cancellation token; the
/// remaining members return inert values sufficient for the code under test.
/// </summary>
internal sealed class FakeServerCallContext : ServerCallContext
{
    private readonly string _method;
    private readonly global::Grpc.Core.Metadata _requestHeaders;
    private readonly CancellationToken _cancellationToken;

    public FakeServerCallContext(
        string method,
        global::Grpc.Core.Metadata? requestHeaders = null,
        CancellationToken cancellationToken = default)
    {
        _method = method;
        _requestHeaders = requestHeaders ?? new global::Grpc.Core.Metadata();
        _cancellationToken = cancellationToken;
    }

    protected override string MethodCore => _method;

    protected override string HostCore => "localhost";

    protected override string PeerCore => "ipv4:127.0.0.1:0";

    protected override DateTime DeadlineCore => DateTime.MaxValue;

    protected override global::Grpc.Core.Metadata RequestHeadersCore => _requestHeaders;

    protected override CancellationToken CancellationTokenCore => _cancellationToken;

    protected override global::Grpc.Core.Metadata ResponseTrailersCore { get; } = new global::Grpc.Core.Metadata();

    protected override Status StatusCore { get; set; }

    protected override WriteOptions? WriteOptionsCore { get; set; }

    protected override AuthContext AuthContextCore =>
        new(null, new Dictionary<string, List<AuthProperty>>());

    protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) =>
        throw new NotSupportedException();

    protected override Task WriteResponseHeadersAsyncCore(global::Grpc.Core.Metadata responseHeaders) =>
        Task.CompletedTask;
}
