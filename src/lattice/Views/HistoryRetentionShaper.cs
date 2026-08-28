namespace Orleans.Lattice.Views;

/// <summary>
/// Applies a source tree's live durable-history retention policy to a revision
/// row at drain time. The history projection is a pure function of a single
/// mutation and cannot read the runtime-tunable policy, so it emits the maximal
/// row (full LWW value, full CRDT delta) and the maintainer calls
/// <see cref="Shape"/> to stamp the age-bound expiry and strip LWW value bytes to
/// metadata per the active <see cref="HistoryRetentionMode"/>.
/// <para>
/// CRDT delta rows are never stripped (the delta is the compact history), and
/// delete / range-tombstone markers carry no value to strip, so for those kinds
/// shaping only stamps the expiry and records the mode in effect.
/// </para>
/// </summary>
internal static class HistoryRetentionShaper
{
    /// <summary>
    /// Shapes <paramref name="row"/> for storage under <paramref name="policy"/>,
    /// returning the reshaped row and the absolute UTC tick at which the view
    /// entry should expire (<c>0</c> when the policy has no age bound).
    /// </summary>
    /// <param name="row">The maximal revision row emitted by the projection.</param>
    /// <param name="policy">The resolved retention policy for the source tree.</param>
    /// <param name="drainNowTicks">
    /// <see cref="DateTime.UtcNow"/> ticks captured once for the drain pass, used
    /// both as the expiry base and as the apply-time clock for the hybrid window.
    /// </param>
    public static (HistoryRow Row, long ExpiresAtTicks) Shape(
        HistoryRow row,
        HistoryRetentionPolicy policy,
        long drainNowTicks)
    {
        var expiresAtTicks = policy.Window > TimeSpan.Zero
            ? drainNowTicks + policy.Window.Ticks
            : 0L;

        // Only an LWW Set row carries value bytes that the mode can strip. CRDT
        // deltas, deletes and range-tombstone markers keep their (delta / empty)
        // payload verbatim and merely record the mode that was in effect.
        if (row.Kind != HistoryRowKind.Set)
        {
            return (row with { RetentionShape = policy.Mode }, expiresAtTicks);
        }

        var keepBytes = KeepsValueBytes(row.Kind, row.Timestamp.WallClockTicks, policy, drainNowTicks);

        var shaped = keepBytes
            ? row with { RetentionShape = policy.Mode }
            : row with { RetentionShape = policy.Mode, Value = null };

        return (shaped, expiresAtTicks);
    }

    /// <summary>
    /// The single definition of whether a revision keeps its LWW value bytes under
    /// <paramref name="policy"/>. Shared by the drain-time view shaping above and
    /// by the write-ahead-log history fallback, so a tree with no history view
    /// honours exactly the same retention rule as one with a view rather than
    /// serving the full plaintext regardless of the configured mode.
    /// </summary>
    /// <param name="kind">The revision's row kind.</param>
    /// <param name="revisionWallClockTicks">
    /// The revision's wall-clock tick, used only by the hybrid window.
    /// </param>
    /// <param name="policy">The resolved retention policy for the source tree.</param>
    /// <param name="nowTicks"><see cref="DateTime.UtcNow"/> ticks captured once for the pass.</param>
    /// <returns><see langword="true"/> when the value bytes are retained.</returns>
    public static bool KeepsValueBytes(
        HistoryRowKind kind,
        long revisionWallClockTicks,
        HistoryRetentionPolicy policy,
        long nowTicks)
    {
        // Only an LWW Set carries value bytes the mode can strip; every other kind
        // is already metadata (or a CRDT delta, which is the compact history).
        if (kind != HistoryRowKind.Set)
        {
            return true;
        }

        return policy.Mode switch
        {
            HistoryRetentionMode.FullValue => true,
            HistoryRetentionMode.Hybrid => IsRecent(revisionWallClockTicks, policy, nowTicks),
            _ => false, // MetadataOnly (the default).
        };
    }

    // A hybrid revision keeps its full bytes while its apply-time age is within
    // the configured full-value window; an older revision (drained from a backlog
    // or a catch-up replay) is shaped to metadata. A non-positive window degrades
    // hybrid to metadata-only.
    private static bool IsRecent(long revisionWallClockTicks, HistoryRetentionPolicy policy, long nowTicks)
    {
        if (policy.HybridFullValueWindow <= TimeSpan.Zero)
        {
            return false;
        }

        var age = nowTicks - revisionWallClockTicks;
        return age <= policy.HybridFullValueWindow.Ticks;
    }
}
