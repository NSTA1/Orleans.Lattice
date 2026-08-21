using Grpc.Core;

namespace Orleans.Lattice.Api.Backup.Grpc.Tests;

/// <summary>
/// In-memory <see cref="IServerStreamWriter{T}"/> that records every message the
/// service writes, so a server-streaming RPC can be driven directly against a
/// <see cref="FakeServerCallContext"/> and its output asserted without a live
/// transport.
/// </summary>
internal sealed class RecordingServerStreamWriter<T> : IServerStreamWriter<T>
{
    /// <summary>The messages written, in order.</summary>
    public List<T> Written { get; } = new();

    /// <inheritdoc />
    public WriteOptions? WriteOptions { get; set; }

    /// <inheritdoc />
    public Task WriteAsync(T message)
    {
        Written.Add(message);
        return Task.CompletedTask;
    }
}
