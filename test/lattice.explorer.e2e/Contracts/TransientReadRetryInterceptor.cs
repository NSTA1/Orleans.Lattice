using Grpc.Core;
using Grpc.Core.Interceptors;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;

/// <summary>
/// Retries a contract's facade <em>reads</em> that fail transiently. Late in a story the one-host
/// CI estate is heavily loaded (two west silos, an east silo, a line feed, replication, views
/// and background maintenance), and a read can outlast the silo's 30-second grain call
/// timeout, which the facade reports as <see cref="StatusCode.Internal"/> ("... request
/// failed."). A read changes nothing, so reading it again is safe; a mutation is never
/// retried here (a contract re-checks state instead).
/// </summary>
internal sealed class TransientReadRetryInterceptor : Interceptor
{
    private static readonly string[] ReadPrefixes =
        ["Get", "List", "Describe", "Check", "Count", "Read", "Range", "Explain", "Probe", "Resolve", "Inspect", "Audit"];

    private static readonly TimeSpan[] Backoff =
        [TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(20)];

    /// <summary>The shared instance.</summary>
    public static TransientReadRetryInterceptor Instance { get; } = new();

    /// <inheritdoc />
    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        TRequest request,
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
    {
        if (!IsRead(context.Method.Name))
        {
            return continuation(request, context);
        }

        var response = RetryAsync(request, context, continuation);
        return new AsyncUnaryCall<TResponse>(
            response,
            Task.FromResult(new Grpc.Core.Metadata()),
            () => response.IsCompletedSuccessfully ? Status.DefaultSuccess : new Status(StatusCode.Unknown, "The call has not completed successfully."),
            () => [],
            () => { });
    }

    private static async Task<TResponse> RetryAsync<TRequest, TResponse>(
        TRequest request,
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
        where TRequest : class
        where TResponse : class
    {
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                using var call = continuation(request, context);
                return await call.ResponseAsync.ConfigureAwait(false);
            }
            catch (RpcException ex) when (attempt < Backoff.Length && IsTransient(ex))
            {
                await Task.Delay(Backoff[attempt]).ConfigureAwait(false);
            }
        }
    }

    private static bool IsRead(string method) =>
        ReadPrefixes.Any(prefix => method.StartsWith(prefix, StringComparison.Ordinal));

    private static bool IsTransient(RpcException exception) =>
        exception.StatusCode is StatusCode.Unavailable or StatusCode.DeadlineExceeded or StatusCode.ResourceExhausted
        || (exception.StatusCode == StatusCode.Internal && exception.Status.Detail.EndsWith("request failed.", StringComparison.Ordinal));
}
