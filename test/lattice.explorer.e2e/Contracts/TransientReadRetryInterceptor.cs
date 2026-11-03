using Grpc.Core;
using Grpc.Core.Interceptors;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;

/// <summary>
/// Retries a contract's facade <em>reads</em> that fail transiently. Late in a story the one-host
/// CI estate is heavily loaded (two west silos, an east silo, a line feed, replication, views
/// and background maintenance), and a read can outlast the silo's grain call timeout, meet a
/// silo that is briefly unavailable, or lose its remote enumeration to a grain deactivation.
/// Every facade reports those as <see cref="StatusCode.DeadlineExceeded"/>
/// and <see cref="StatusCode.Unavailable"/> (#5742), and storage back-pressure as
/// <see cref="StatusCode.ResourceExhausted"/> (#5728).
/// </summary>
/// <remarks>
/// Every method the read prefixes match is side-effect-free and therefore idempotent, so reading
/// it again is safe even when a <see cref="StatusCode.DeadlineExceeded"/> left its outcome unknown.
/// A mutation is never retried here: a write that reports <see cref="StatusCode.DeadlineExceeded"/>
/// may already have applied, so the contract re-checks state instead. <see cref="StatusCode.Internal"/>
/// is a server fault, not a transient one, and is never retried.
/// </remarks>
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
            var clock = System.Diagnostics.Stopwatch.StartNew();
            try
            {
                using var call = continuation(request, context);
                return await call.ResponseAsync.ConfigureAwait(false);
            }
            catch (RpcException ex) when (attempt < Backoff.Length && ShouldRetry(context.Method.Name, ex.StatusCode))
            {
                ContractRetries.Record(context.Method.FullName, clock.Elapsed, $"{ex.StatusCode}: {ex.Status.Detail}");
                await Task.Delay(Backoff[attempt]).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Whether a failed call to <paramref name="method"/> with <paramref name="code"/> is retried.</summary>
    /// <param name="method">The gRPC method name.</param>
    /// <param name="code">The status the call failed with.</param>
    internal static bool ShouldRetry(string method, StatusCode code) => IsRead(method) && IsTransient(code);

    private static bool IsRead(string method) =>
        ReadPrefixes.Any(prefix => method.StartsWith(prefix, StringComparison.Ordinal));

    private static bool IsTransient(StatusCode code) =>
        code is StatusCode.Unavailable or StatusCode.DeadlineExceeded or StatusCode.ResourceExhausted;
}
