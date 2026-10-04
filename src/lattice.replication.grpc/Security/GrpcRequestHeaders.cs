using Grpc.Core;

namespace Orleans.Lattice.Replication.Grpc;

/// <summary>
/// Reads request metadata on the receiver side of the replication gRPC
/// services and their authentication interceptor, so every receiver-side
/// header lookup (the shared-secret credential and the stamped origin
/// cluster id) matches header names the same way.
/// </summary>
internal static class GrpcRequestHeaders
{
    /// <summary>
    /// Returns the value of the first request header named
    /// <paramref name="key"/>, compared ordinal-ignore-case because HTTP/2
    /// header names are lower-cased on the wire, or <see langword="null"/>
    /// when the request carries no such header.
    /// </summary>
    /// <param name="context">The server call context whose request headers are read.</param>
    /// <param name="key">The header name.</param>
    /// <returns>The header value, or <see langword="null"/> when absent.</returns>
    public static string? Read(ServerCallContext context, string key)
    {
        foreach (var entry in context.RequestHeaders)
        {
            if (string.Equals(entry.Key, key, StringComparison.OrdinalIgnoreCase))
            {
                return entry.Value;
            }
        }

        return null;
    }
}
