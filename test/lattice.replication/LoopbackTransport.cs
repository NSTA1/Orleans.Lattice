using System.Collections.Concurrent;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// In-memory <see cref="IReplicationTransport"/> used by the two-site test
/// fixture. Records every <see cref="ReplicationBatch"/> routed through it
/// so tests can assert on shipped content without standing up a real
/// HTTP/gRPC server. The acknowledgement returned to the sender can be
/// customised via <see cref="AckFactory"/>; the default treats every send
/// as accepted with the entry's <c>HighestAppliedHlc</c> defaulted to
/// <see cref="HybridLogicalClock.Zero"/>.
/// </summary>
internal sealed class LoopbackTransport : IReplicationTransport
{
    private readonly ConcurrentQueue<ReplicationBatch> _sent = new();

    /// <summary>Batches recorded by <see cref="SendAsync"/>, in arrival order.</summary>
    public IReadOnlyCollection<ReplicationBatch> Sent => _sent.ToArray();

    /// <summary>
    /// Optional factory used to synthesise the receiver-side
    /// <see cref="ReplicationAck"/> returned to the caller. When
    /// <see langword="null"/> the transport returns
    /// <c>new ReplicationAck { Accepted = true, HighestAppliedHlc = default }</c>.
    /// </summary>
    public Func<ReplicationBatch, ReplicationAck>? AckFactory { get; set; }

    /// <inheritdoc />
    public Task<ReplicationAck> SendAsync(ReplicationBatch batch, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(batch.TargetClusterId))
        {
            throw new ArgumentException(
                "ReplicationBatch.TargetClusterId must be non-empty.",
                nameof(batch));
        }

        if (string.IsNullOrEmpty(batch.TreeName))
        {
            throw new ArgumentException(
                "ReplicationBatch.TreeName must be non-empty.",
                nameof(batch));
        }

        if (string.IsNullOrEmpty(batch.OriginClusterId))
        {
            throw new ArgumentException(
                "ReplicationBatch.OriginClusterId must be non-empty.",
                nameof(batch));
        }

        _sent.Enqueue(batch);

        var ack = AckFactory?.Invoke(batch)
            ?? new ReplicationAck { Accepted = true, HighestAppliedHlc = default };
        return Task.FromResult(ack);
    }
}
