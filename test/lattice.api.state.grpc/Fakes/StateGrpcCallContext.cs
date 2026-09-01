using Grpc.Core;

namespace Orleans.Lattice.Api.State.Grpc.Tests;

/// <summary>
/// Minimal in-process <see cref="ServerCallContext"/> test double that carries a
/// caller-supplied method name, request headers, and cancellation token, so the
/// server-side service and the auth interceptor can be driven directly without a
/// live gRPC server. The remaining members return inert values.
/// </summary>
internal sealed class StateGrpcCallContext : ServerCallContext
{
    private readonly string _method;
    private readonly global::Grpc.Core.Metadata _requestHeaders;
    private readonly CancellationToken _cancellationToken;

    public StateGrpcCallContext(
        string method,
        global::Grpc.Core.Metadata? requestHeaders = null,
        CancellationToken cancellationToken = default)
    {
        _method = method;
        _requestHeaders = requestHeaders ?? new global::Grpc.Core.Metadata();
        _cancellationToken = cancellationToken;
    }

    /// <summary>Builds a context addressing <paramref name="methodName"/> on the state-API service.</summary>
    public static StateGrpcCallContext ForMethod(
        string methodName,
        global::Grpc.Core.Metadata? requestHeaders = null,
        CancellationToken cancellationToken = default) =>
        new($"/{LatticeStateGrpcMethods.ServiceName}/{methodName}", requestHeaders, cancellationToken);

    protected override string MethodCore => _method;

    protected override string HostCore => "localhost";

    protected override string PeerCore => "ipv4:127.0.0.1:0";

    protected override DateTime DeadlineCore => DateTime.MaxValue;

    protected override global::Grpc.Core.Metadata RequestHeadersCore => _requestHeaders;

    protected override CancellationToken CancellationTokenCore => _cancellationToken;

    protected override global::Grpc.Core.Metadata ResponseTrailersCore { get; } = new global::Grpc.Core.Metadata();

    protected override Status StatusCore { get; set; } = Status.DefaultSuccess;

    protected override WriteOptions? WriteOptionsCore { get; set; }

    protected override AuthContext AuthContextCore =>
        new(null, new Dictionary<string, List<AuthProperty>>());

    protected override IDictionary<object, object> UserStateCore { get; } = new Dictionary<object, object>();

    protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) =>
        throw new NotSupportedException();

    protected override Task WriteResponseHeadersAsyncCore(global::Grpc.Core.Metadata responseHeaders) =>
        Task.CompletedTask;
}
