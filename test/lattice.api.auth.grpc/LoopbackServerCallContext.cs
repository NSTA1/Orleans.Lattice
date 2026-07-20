using Grpc.Core;

namespace Orleans.Lattice.Api.Auth.Grpc.Tests;

/// <summary>
/// A minimal <see cref="ServerCallContext"/> test double carrying a configurable
/// method name and cancellation token, with inert defaults for every other
/// member. Shared by the loopback round-trip (which needs a context to pass into
/// the service overrides) and the interceptor admit / deny unit tests (which key
/// their gating decision off <see cref="ServerCallContext.Method"/>).
/// </summary>
internal sealed class LoopbackServerCallContext(string method, CancellationToken cancellationToken = default)
    : ServerCallContext
{
    protected override string MethodCore { get; } = method;

    protected override string HostCore => "localhost";

    protected override string PeerCore => "ipv4:127.0.0.1:0";

    protected override DateTime DeadlineCore => DateTime.MaxValue;

    protected override global::Grpc.Core.Metadata RequestHeadersCore { get; } = new();

    protected override CancellationToken CancellationTokenCore { get; } = cancellationToken;

    protected override global::Grpc.Core.Metadata ResponseTrailersCore { get; } = new();

    protected override Status StatusCore { get; set; } = Status.DefaultSuccess;

    protected override WriteOptions? WriteOptionsCore { get; set; }

    protected override AuthContext AuthContextCore { get; } =
        new(null, new Dictionary<string, List<AuthProperty>>());

    protected override IDictionary<object, object> UserStateCore { get; } = new Dictionary<object, object>();

    protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) =>
        throw new NotSupportedException();

    protected override Task WriteResponseHeadersAsyncCore(global::Grpc.Core.Metadata responseHeaders) => Task.CompletedTask;
}
