using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;

namespace Orleans.Lattice.Api.Schema.Grpc.Tests;

/// <summary>
/// In-process test doubles for the schema control-API gRPC binding that let the
/// hand-written service and client be driven directly - without standing up an
/// Orleans cluster or an ASP.NET Core gRPC server - so the unit tests stay in the
/// Tier 2 fast loop. None of these carry an integration-bearing field (no
/// <c>TestCluster</c>, <c>TestServer</c>, <c>IHost</c>, or <c>GrpcChannel</c>), so
/// fixtures that use them are pure unit tests.
/// </summary>
internal static class SchemaGrpcTestDoubles
{
    /// <summary>The fully-qualified method name for a schema control-API RPC.</summary>
    public static string FullMethod(string methodName) =>
        "/" + LatticeSchemaGrpcMethods.ServiceName + "/" + methodName;
}

/// <summary>
/// Minimal <see cref="ServerCallContext"/> double: carries a configurable method
/// name, request headers, and cancellation token, and no-ops the response-header
/// write. Enough to drive the server-side service methods directly.
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

    protected override Status StatusCore { get; set; } = Status.DefaultSuccess;

    protected override WriteOptions? WriteOptionsCore { get; set; }

    protected override AuthContext AuthContextCore { get; } =
        new AuthContext(null, new Dictionary<string, List<AuthProperty>>());

    protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) =>
        throw new NotSupportedException();

    protected override Task WriteResponseHeadersAsyncCore(global::Grpc.Core.Metadata responseHeaders) => Task.CompletedTask;
}

/// <summary>
/// Collecting <see cref="IServerStreamWriter{T}"/> double that records every
/// written entry in order, so the server-streaming RPC can be drained
/// synchronously and asserted.
/// </summary>
internal sealed class CollectingServerStreamWriter<T> : IServerStreamWriter<T>
{
    public List<T> Written { get; } = new();

    public WriteOptions? WriteOptions { get; set; }

    public Task WriteAsync(T message)
    {
        Written.Add(message);
        return Task.CompletedTask;
    }
}

/// <summary>
/// In-memory <see cref="IAsyncStreamReader{T}"/> over a fixed sequence, so a
/// fake call invoker can hand a client a canned server-streaming response.
/// </summary>
internal sealed class FakeAsyncStreamReader<T> : IAsyncStreamReader<T>
{
    private readonly IReadOnlyList<T> _items;
    private int _index = -1;

    public FakeAsyncStreamReader(IReadOnlyList<T> items) => _items = items;

    public T Current => _items[_index];

    public Task<bool> MoveNext(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _index++;
        return Task.FromResult(_index < _items.Count);
    }
}

/// <summary>
/// <see cref="CallInvoker"/> double that returns pre-canned unary and
/// server-streaming responses without any transport or marshalling, and records
/// the last request and method name it was handed. Only the unary and
/// server-streaming paths the client uses are implemented; the others throw.
/// </summary>
internal sealed class FakeCallInvoker : CallInvoker
{
    private readonly object? _unaryResponse;
    private readonly System.Collections.IEnumerable? _streamItems;

    /// <summary>The most recent request object passed to the invoker.</summary>
    public object? LastRequest { get; private set; }

    /// <summary>The most recent gRPC method name passed to the invoker.</summary>
    public string? LastMethodName { get; private set; }

    private FakeCallInvoker(object? unaryResponse, System.Collections.IEnumerable? streamItems)
    {
        _unaryResponse = unaryResponse;
        _streamItems = streamItems;
    }

    /// <summary>Builds an invoker that answers unary calls with <paramref name="response"/>.</summary>
    public static FakeCallInvoker ForUnary(object response) => new(response, null);

    /// <summary>Builds an invoker that answers server-streaming calls with <paramref name="items"/>.</summary>
    public static FakeCallInvoker ForStream<T>(IReadOnlyList<T> items) => new(null, items);

    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
    {
        LastRequest = request;
        LastMethodName = method.Name;
        return new AsyncUnaryCall<TResponse>(
            Task.FromResult((TResponse)_unaryResponse!),
            Task.FromResult(new global::Grpc.Core.Metadata()),
            () => Status.DefaultSuccess,
            () => new global::Grpc.Core.Metadata(),
            () => { });
    }

    public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
    {
        LastRequest = request;
        LastMethodName = method.Name;
        var items = _streamItems is null
            ? new List<TResponse>()
            : new List<TResponse>(System.Linq.Enumerable.Cast<TResponse>(_streamItems));
        return new AsyncServerStreamingCall<TResponse>(
            new FakeAsyncStreamReader<TResponse>(items),
            Task.FromResult(new global::Grpc.Core.Metadata()),
            () => Status.DefaultSuccess,
            () => new global::Grpc.Core.Metadata(),
            () => { });
    }

    public override TResponse BlockingUnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
        throw new NotSupportedException();

    public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options) =>
        throw new NotSupportedException();

    public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options) =>
        throw new NotSupportedException();
}
