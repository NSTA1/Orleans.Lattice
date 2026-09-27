using Grpc.Core;
using GrpcMetadata = Grpc.Core.Metadata;

namespace Orleans.Lattice.Api.Apps.Grpc.Tests;

internal sealed class TestCallContext(string method, GrpcMetadata? headers = null) : ServerCallContext
{
    protected override string MethodCore => method;
    protected override string HostCore => "localhost";
    protected override string PeerCore => "test";
    protected override DateTime DeadlineCore => DateTime.MaxValue;
    protected override GrpcMetadata RequestHeadersCore => headers ?? new GrpcMetadata();
    protected override CancellationToken CancellationTokenCore => CancellationToken.None;
    protected override GrpcMetadata ResponseTrailersCore { get; } = new();
    protected override Status StatusCore { get; set; }
    protected override WriteOptions? WriteOptionsCore { get; set; }
    protected override AuthContext AuthContextCore => new("test", new Dictionary<string, List<AuthProperty>>());
    protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options)
        => throw new NotSupportedException();
    protected override Task WriteResponseHeadersAsyncCore(GrpcMetadata responseHeaders) => Task.CompletedTask;
}
