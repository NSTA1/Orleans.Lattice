using Grpc.AspNetCore.Server;
using Grpc.Core;
using Microsoft.AspNetCore.Http;

namespace Orleans.Lattice.Api.Schema.Grpc;

/// <summary>
/// Resolves the gRPC method a server call was routed to, so a method-keyed
/// security decision never rests on the raw request path.
/// </summary>
/// <remarks>
/// ASP.NET Core endpoint routing matches a request path case-insensitively, so a
/// call sent to a re-cased path still reaches the canonical handler while
/// <see cref="ServerCallContext.Method"/> reports the path exactly as the caller
/// sent it. An interceptor that recognises its own service by an ordinal test of
/// that raw path therefore never sees such a call, and it runs with no
/// authentication or authorization at all. The canonical name is read from the
/// routed endpoint's <see cref="GrpcMethodMetadata"/> instead. Allocation-free on
/// the routed path: ASP.NET Core caches the endpoint metadata, and the canonical
/// name is the method definition's own string.
/// </remarks>
internal static class GrpcRoutedMethod
{
    /// <summary>
    /// The canonical <c>/{service}/{method}</c> name of the method
    /// <paramref name="context"/> was routed to, or
    /// <see cref="ServerCallContext.Method"/> when the call was not dispatched by
    /// ASP.NET Core endpoint routing (an in-process call context, whose method no
    /// router matched case-insensitively).
    /// </summary>
    /// <param name="context">The server call context.</param>
    /// <returns>The canonical full method name.</returns>
    public static string Resolve(ServerCallContext context)
    {
        HttpContext httpContext;
        try
        {
            httpContext = context.GetHttpContext();
        }
        catch (InvalidOperationException)
        {
            return context.Method;
        }

        return httpContext.GetEndpoint()?.Metadata.GetMetadata<GrpcMethodMetadata>()?.Method.FullName
            ?? context.Method;
    }
}