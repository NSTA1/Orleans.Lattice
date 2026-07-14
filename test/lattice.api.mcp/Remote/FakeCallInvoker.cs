using System.Collections;
using Grpc.Core;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Deterministic fake <see cref="CallInvoker"/> for the remote-host adapter
/// tests. It bypasses the wire entirely: a unary call runs a supplied responder
/// against the request and returns the produced response (or faults with a
/// supplied exception), while a server-streaming call replays a supplied
/// sequence. Every call's outbound gRPC metadata and request object are
/// captured so a test can assert the credential header the interceptor stamped.
/// No network, no timers, no threads.
/// </summary>
internal sealed class FakeCallInvoker : CallInvoker
{
    private readonly Func<object, object> _unaryResponder;
    private readonly Func<object, IEnumerable>? _streamResponder;

    /// <summary>
    /// Creates a fake invoker. <paramref name="unaryResponder"/> maps a request
    /// object to its response object; returning (or throwing) an
    /// <see cref="Exception"/> faults the call. <paramref name="streamResponder"/>
    /// maps a request to the sequence a server-streaming call replays.
    /// </summary>
    public FakeCallInvoker(
        Func<object, object> unaryResponder,
        Func<object, IEnumerable>? streamResponder = null)
    {
        _unaryResponder = unaryResponder ?? throw new ArgumentNullException(nameof(unaryResponder));
        _streamResponder = streamResponder;
    }

    /// <summary>The headers carried on the most recent call (the interceptor's stamped metadata).</summary>
    public Grpc.Core.Metadata? LastHeaders { get; private set; }

    /// <summary>The request object of the most recent call.</summary>
    public object? LastRequest { get; private set; }

    /// <summary>The number of calls the invoker has served.</summary>
    public int CallCount { get; private set; }

    /// <inheritdoc />
    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        string? host,
        CallOptions options,
        TRequest request)
    {
        Capture(options, request);

        Task<TResponse> responseAsync;
        if (options.CancellationToken.IsCancellationRequested)
        {
            responseAsync = Task.FromCanceled<TResponse>(options.CancellationToken);
        }
        else
        {
            try
            {
                var produced = _unaryResponder(request!);
                responseAsync = produced is Exception ex
                    ? Task.FromException<TResponse>(ex)
                    : Task.FromResult((TResponse)produced);
            }
            catch (Exception ex)
            {
                responseAsync = Task.FromException<TResponse>(ex);
            }
        }

        return new AsyncUnaryCall<TResponse>(
            responseAsync,
            Task.FromResult(new Grpc.Core.Metadata()),
            static () => Status.DefaultSuccess,
            static () => new Grpc.Core.Metadata(),
            static () => { });
    }

    /// <inheritdoc />
    public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        string? host,
        CallOptions options,
        TRequest request)
    {
        Capture(options, request);

        if (_streamResponder is null)
        {
            throw new InvalidOperationException("No stream responder configured on the fake invoker.");
        }

        var items = new List<TResponse>();
        foreach (var item in _streamResponder(request!))
        {
            items.Add((TResponse)item);
        }

        return new AsyncServerStreamingCall<TResponse>(
            new FakeStreamReader<TResponse>(items),
            Task.FromResult(new Grpc.Core.Metadata()),
            static () => Status.DefaultSuccess,
            static () => new Grpc.Core.Metadata(),
            static () => { });
    }

    /// <inheritdoc />
    public override TResponse BlockingUnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
        => throw new NotSupportedException("Blocking calls are not used by the remote-host adapters.");

    /// <inheritdoc />
    public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options)
        => throw new NotSupportedException("Client-streaming calls are not used by the remote-host adapters.");

    /// <inheritdoc />
    public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options)
        => throw new NotSupportedException("Duplex-streaming calls are not used by the remote-host adapters.");

    private void Capture<TRequest>(CallOptions options, TRequest request)
    {
        CallCount++;
        LastHeaders = options.Headers;
        LastRequest = request;
    }

    private sealed class FakeStreamReader<T>(IReadOnlyList<T> items) : IAsyncStreamReader<T>
    {
        private int _index = -1;

        public T Current => items[_index];

        public Task<bool> MoveNext(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _index++;
            return Task.FromResult(_index < items.Count);
        }
    }
}

