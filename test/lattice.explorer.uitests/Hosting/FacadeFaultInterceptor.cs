using Grpc.Core;
using Grpc.Core.Interceptors;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// The world's global gRPC server interceptor behind <see cref="FacadeFaults"/>: a
/// call to an armed facade service fails with <c>Internal</c> before it reaches the
/// facade, exactly as a call that met a lost grain activation does.
/// </summary>
/// <param name="faults">The world's switch.</param>
internal sealed class FacadeFaultInterceptor(FacadeFaults faults) : Interceptor
{
    /// <summary>The detail every injected fault carries.</summary>
    public const string Detail = "Injected: the grain activation was on a silo that left the cluster.";

    /// <inheritdoc />
    public override Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
    {
        ThrowIfArmed(context);
        return continuation(request, context);
    }

    /// <inheritdoc />
    public override Task ServerStreamingServerHandler<TRequest, TResponse>(
        TRequest request,
        IServerStreamWriter<TResponse> responseStream,
        ServerCallContext context,
        ServerStreamingServerMethod<TRequest, TResponse> continuation)
    {
        ThrowIfArmed(context);
        return continuation(request, responseStream, context);
    }

    /// <inheritdoc />
    public override Task<TResponse> ClientStreamingServerHandler<TRequest, TResponse>(
        IAsyncStreamReader<TRequest> requestStream,
        ServerCallContext context,
        ClientStreamingServerMethod<TRequest, TResponse> continuation)
    {
        ThrowIfArmed(context);
        return continuation(requestStream, context);
    }

    /// <inheritdoc />
    public override Task DuplexStreamingServerHandler<TRequest, TResponse>(
        IAsyncStreamReader<TRequest> requestStream,
        IServerStreamWriter<TResponse> responseStream,
        ServerCallContext context,
        DuplexStreamingServerMethod<TRequest, TResponse> continuation)
    {
        ThrowIfArmed(context);
        return continuation(requestStream, responseStream, context);
    }

    private void ThrowIfArmed(ServerCallContext context)
    {
        if (faults.Fails(context.Method))
        {
            throw new RpcException(new Status(StatusCode.Internal, Detail));
        }
    }
}
