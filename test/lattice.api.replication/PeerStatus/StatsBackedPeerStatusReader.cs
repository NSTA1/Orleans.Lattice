using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Api.Replication.Tests.PeerStatus;

/// <summary>
/// An <see cref="IReplicationPeerStatusReader"/> backed by a real, manually
/// clocked <see cref="ReplicationPeerStats"/>, so facade tests exercise the
/// production ordering, filtering, cursor and limit semantics of the read path
/// with deterministic elapsed times and no silo. Records every read it serves.
/// </summary>
internal sealed class StatsBackedPeerStatusReader : IReplicationPeerStatusReader
{
    /// <summary>The telemetry state reads are served from.</summary>
    public ClockedPeerStats Stats { get; } = new();

    /// <summary>Every read served, in order.</summary>
    public List<ReplicationPeerStatusReadRequest> Reads { get; } = [];

    /// <inheritdoc />
    public Task<IReadOnlyList<ReplicationPeerStatusRow>> ReadAsync(
        ReplicationPeerStatusReadRequest request,
        CancellationToken cancellationToken)
    {
        Reads.Add(request);
        return Task.FromResult<IReadOnlyList<ReplicationPeerStatusRow>>(Stats.ReadStatusPage(request));
    }

    /// <summary>A <see cref="ReplicationPeerStats"/> whose clock is a settable instant.</summary>
    internal sealed class ClockedPeerStats : ReplicationPeerStats
    {
        /// <summary>The instant the stats read as "now".</summary>
        public DateTimeOffset Now { get; set; } = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

        /// <summary>Moves the clock forward.</summary>
        /// <param name="by">The amount to advance.</param>
        public void Advance(TimeSpan by) => Now += by;

        /// <inheritdoc />
        protected override DateTimeOffset GetTimestamp() => Now;
    }
}
