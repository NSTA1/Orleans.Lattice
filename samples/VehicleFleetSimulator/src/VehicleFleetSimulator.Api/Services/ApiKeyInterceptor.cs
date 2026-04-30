using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace VehicleFleetSimulator.Api.Services;

/// <summary>
/// Production-shaped placeholder authn: requires <c>x-api-key</c> in call metadata to match the
/// configured <c>Auth:ApiKey</c> value. When no key is configured (typical in dev), all calls are allowed.
/// </summary>
public sealed class ApiKeyInterceptor : Interceptor
{
    private const string HeaderName = "x-api-key";
    private readonly string? _expectedKey;
    private readonly ILogger<ApiKeyInterceptor> _logger;

    public ApiKeyInterceptor(IConfiguration configuration, ILogger<ApiKeyInterceptor> logger)
    {
        _expectedKey = configuration["Auth:ApiKey"];
        _logger = logger;
    }

    public override Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request, ServerCallContext context, UnaryServerMethod<TRequest, TResponse> continuation)
    {
        Authorize(context);
        return continuation(request, context);
    }

    public override Task<TResponse> ClientStreamingServerHandler<TRequest, TResponse>(
        IAsyncStreamReader<TRequest> requestStream, ServerCallContext context,
        ClientStreamingServerMethod<TRequest, TResponse> continuation)
    {
        Authorize(context);
        return continuation(requestStream, context);
    }

    public override Task ServerStreamingServerHandler<TRequest, TResponse>(
        TRequest request, IServerStreamWriter<TResponse> responseStream, ServerCallContext context,
        ServerStreamingServerMethod<TRequest, TResponse> continuation)
    {
        Authorize(context);
        return continuation(request, responseStream, context);
    }

    public override Task DuplexStreamingServerHandler<TRequest, TResponse>(
        IAsyncStreamReader<TRequest> requestStream, IServerStreamWriter<TResponse> responseStream,
        ServerCallContext context, DuplexStreamingServerMethod<TRequest, TResponse> continuation)
    {
        Authorize(context);
        return continuation(requestStream, responseStream, context);
    }

    private void Authorize(ServerCallContext context)
    {
        if (string.IsNullOrEmpty(_expectedKey)) return; // disabled when unconfigured

        var entry = context.RequestHeaders.FirstOrDefault(h =>
            string.Equals(h.Key, HeaderName, StringComparison.OrdinalIgnoreCase));
        if (entry is null || !string.Equals(entry.Value, _expectedKey, StringComparison.Ordinal))
        {
            _logger.LogWarning("Rejecting gRPC call to {Method} due to missing/invalid API key.", context.Method);
            throw new RpcException(new Status(StatusCode.Unauthenticated, "Missing or invalid API key."));
        }
    }
}
