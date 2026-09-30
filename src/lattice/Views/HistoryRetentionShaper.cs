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
    /// Shapes <paramref name="row"/> without reporting whether shaping altered
    /// it. Retained as the plain form the shaping contract is specified and
    /// tested against, and as the oracle the change-detecting overload's parity
    /// tests compare against; do not delete it as redundant.
    /// </summary>
    public static (HistoryRow Row, long ExpiresAtTicks) Shape(
        HistoryRow row,
        HistoryRetentionPolicy policy,
        long drainNowTicks) =>
        Shape(row, policy, drainNowTicks, out _);

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
    /// <param name="changed">
    /// Whether shaping altered the row at all. The projection is a pure function
    /// and never stamps <see cref="HistoryRow.RetentionShape"/>, so every row it
    /// emits arrives carrying the enum default,
    /// <see cref="HistoryRetentionMode.MetadataOnly"/> - which is also the default
    /// policy. Under that policy a delete, a range-tombstone marker and a CRDT
    /// delta are therefore already in their stored shape and reshaping them is a
    /// no-op, yet the maintainer still re-serialised each one. The flag lets it
    /// keep the bytes it already holds. The expiry is stamped on the
    /// <em>view entry</em>, not inside the row, so it never forces a re-encode.
    /// </param>
    public static (HistoryRow Row, long ExpiresAtTicks) Shape(
        HistoryRow row,
        HistoryRetentionPolicy policy,
        long drainNowTicks,
        out bool changed)
    {
        // Saturate rather than overflow: a window so large that the absolute
        // expiry would exceed DateTime.MaxValue is stamped at the maximum
        // representable tick (effectively never expiring), preserving the
        // retain-nearly-forever intent instead of wrapping to a negative tick
        // that the next TTL sweep would treat as already-expired and drop.
        // drainNowTicks and Window.Ticks are both non-negative here.
        long expiresAtTicks = 0L;
        if (policy.Window > TimeSpan.Zero)
        {
            var windowTicks = policy.Window.Ticks;
            expiresAtTicks = windowTicks > DateTime.MaxValue.Ticks - drainNowTicks
                ? DateTime.MaxValue.Ticks
                : drainNowTicks + windowTicks;
        }

        // Only an LWW Set row carries value bytes that the mode can strip. CRDT
        // deltas, deletes and range-tombstone markers keep their (delta / empty)
        // payload verbatim and merely record the mode that was in effect.
        if (row.Kind != HistoryRowKind.Set)
        {
            changed = row.RetentionShape != policy.Mode;
            return (row with { RetentionShape = policy.Mode }, expiresAtTicks);
        }

        var keepBytes = KeepsValueBytes(row.Kind, row.Timestamp.WallClockTicks, policy, drainNowTicks);

        var shaped = keepBytes
            ? row with { RetentionShape = policy.Mode }
            : row with { RetentionShape = policy.Mode, Value = null };

        changed = row.RetentionShape != policy.Mode || (!keepBytes && row.Value is not null);
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
