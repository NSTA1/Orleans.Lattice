using Grpc.Core;

namespace Orleans.Lattice.Api.State.Grpc.Tests;

/// <summary>
/// An <see cref="IServerStreamWriter{T}"/> that records everything the service
/// writes, so a server-streaming RPC can be driven in process and its emitted
/// sequence asserted without a live gRPC server.
/// </summary>
internal sealed class RecordingServerStreamWriter<T> : IServerStreamWriter<T>
{
    /// <summary>Every message the service wrote, in order.</summary>
    public List<T> Written { get; } = [];

    public WriteOptions? WriteOptions { get; set; }

    public Task WriteAsync(T message)
    {
        Written.Add(message);
        return Task.CompletedTask;
    }
}
