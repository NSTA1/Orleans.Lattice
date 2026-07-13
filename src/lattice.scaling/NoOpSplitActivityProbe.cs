namespace Orleans.Lattice.Scaling;

/// <summary>
/// Default <see cref="ISplitActivityProbe"/>: reports no shard splits in flight.
/// With this probe the scale-in gate is never suppressed on split grounds, which
/// is the correct behaviour for a deployment that does not surface a
/// split-activity source. A host that wants split-aware scale-in gating replaces
/// this registration with a real probe.
/// </summary>
internal sealed class NoOpSplitActivityProbe : ISplitActivityProbe
{
    /// <inheritdoc />
    public bool AnySplitInFlight() => false;
}
