using System.Collections.Concurrent;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// In-memory <see cref="IReplicationTransport"/> used by the two-site test
/// fixture. Records every payload routed through it so tests can assert on
/// shipped content without standing up a real HTTP/gRPC server.
/// </summary>
internal sealed class LoopbackTransport : IReplicationTransport
{
    private readonly ConcurrentQueue<LoopbackEnvelope> _sent = new();

    /// <summary>Envelopes recorded by <see cref="SendAsync"/>, in arrival order.</summary>
    public IReadOnlyCollection<LoopbackEnvelope> Sent => _sent.ToArray();

    /// <inheritdoc />
    public Task SendAsync(string targetClusterId, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(targetClusterId);
        _sent.Enqueue(new LoopbackEnvelope(targetClusterId, payload.ToArray()));
        return Task.CompletedTask;
    }
}

/// <summary>Recorded payload from a <see cref="LoopbackTransport"/> send.</summary>
internal sealed record LoopbackEnvelope(string TargetClusterId, byte[] Payload);
