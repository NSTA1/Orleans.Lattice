namespace Orleans.Lattice.Scaling;

/// <summary>
/// Tracks the asymmetric scale-in window: scale-in is only permitted once the
/// cluster has been continuously eligible (all compute dimensions low, WAL
/// healthy, and no shard split in flight) for at least a configured window. Any
/// break in eligibility resets the window, so a brief dip in load cannot trigger
/// a premature scale-in. Deterministic and driven by explicit timestamps; not
/// thread-safe (updated from a single sampling timer).
/// </summary>
internal sealed class ScaleInGate
{
    private DateTimeOffset? _eligibleSince;

    /// <summary>
    /// The instant the cluster most recently <em>became</em> continuously
    /// eligible for scale-in, or <see langword="null"/> when not currently
    /// eligible. Exposed for assertions and diagnostics.
    /// </summary>
    internal DateTimeOffset? EligibleSince => _eligibleSince;

    /// <summary>
    /// Records this tick's eligibility and returns whether scale-in is permitted:
    /// <see langword="true"/> only when the cluster has been continuously eligible
    /// for at least <paramref name="window"/>. When <paramref name="eligible"/> is
    /// <see langword="false"/> the window is reset and the method returns
    /// <see langword="false"/>.
    /// </summary>
    /// <param name="eligible">Whether every scale-in precondition holds this tick.</param>
    /// <param name="now">This tick's timestamp.</param>
    /// <param name="window">How long eligibility must persist before scale-in is allowed.</param>
    /// <returns>Whether scale-in is permitted this tick.</returns>
    internal bool Evaluate(bool eligible, DateTimeOffset now, TimeSpan window)
    {
        if (!eligible)
        {
            _eligibleSince = null;
            return false;
        }

        _eligibleSince ??= now;
        return now - _eligibleSince.Value >= window;
    }
}
