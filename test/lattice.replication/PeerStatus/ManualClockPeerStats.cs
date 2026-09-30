namespace Orleans.Lattice.Replication.Tests.PeerStatus;

/// <summary>
/// A <see cref="ReplicationPeerStats"/> whose clock is a settable instant, so
/// elapsed-time fields are asserted exactly rather than against the wall clock.
/// </summary>
internal sealed class ManualClockPeerStats : ReplicationPeerStats
{
    /// <summary>The instant <see cref="GetTimestamp"/> reports.</summary>
    public DateTimeOffset Now { get; set; } = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>Moves the clock forward.</summary>
    /// <param name="by">The amount to advance.</param>
    public void Advance(TimeSpan by) => Now += by;

    /// <inheritdoc />
    protected override DateTimeOffset GetTimestamp() => Now;
}
