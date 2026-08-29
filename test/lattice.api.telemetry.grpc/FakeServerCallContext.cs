using Grpc.Core;

namespace Orleans.Lattice.Api.Telemetry.Grpc.Tests;

/// <summary>
/// Minimal in-memory <see cref="ServerCallContext"/> test double. Grpc.Core.Api
/// ships no public test context, so unit tests that drive the interceptor or the
/// service directly (no network, no host) build one of these to carry the gRPC
/// method name, request headers, and cancellation token an inbound call would
/// present. Every other member is a benign default.
/// </summary>
internal sealed class FakeServerCallContext : ServerCallContext
{
    private readonly string _method;
    private readonly global::Grpc.Core.Metadata _requestHeaders;
    private readonly CancellationToken _cancellationToken;

    /// <summary>Builds a fake call context for <paramref name="method"/>.</summary>
    /// <param name="method">The full gRPC method name (for example <c>/svc/Rpc</c>).</param>
    /// <param name="requestHeaders">The inbound request headers, or <c>null</c> for none.</param>
    /// <param name="cancellationToken">The call cancellation token.</param>
    public FakeServerCallContext(
        string method,
        global::Grpc.Core.Metadata? requestHeaders = null,
        CancellationToken cancellationToken = default)
    {
        _method = method;
        _requestHeaders = requestHeaders ?? new global::Grpc.Core.Metadata();
        _cancellationToken = cancellationToken;
    }

    /// <inheritdoc />
    protected override string MethodCore => _method;

    /// <inheritdoc />
    protected override string HostCore => "localhost";

    /// <inheritdoc />
    protected override string PeerCore => "ipv4:127.0.0.1:0";

    /// <inheritdoc />
    protected override DateTime DeadlineCore => DateTime.MaxValue;

    /// <inheritdoc />
    protected override global::Grpc.Core.Metadata RequestHeadersCore => _requestHeaders;

    /// <inheritdoc />
    protected override CancellationToken CancellationTokenCore => _cancellationToken;

    /// <inheritdoc />
    protected override global::Grpc.Core.Metadata ResponseTrailersCore { get; } = new global::Grpc.Core.Metadata();

    /// <inheritdoc />
    protected override Status StatusCore { get; set; }

    /// <inheritdoc />
    protected override WriteOptions? WriteOptionsCore { get; set; }

    /// <inheritdoc />
    protected override AuthContext AuthContextCore { get; } =
        new AuthContext(null, new Dictionary<string, List<AuthProperty>>());

    /// <inheritdoc />
    protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) =>
        throw new NotSupportedException();

    /// <inheritdoc />
    protected override Task WriteResponseHeadersAsyncCore(global::Grpc.Core.Metadata responseHeaders) => Task.CompletedTask;
}
