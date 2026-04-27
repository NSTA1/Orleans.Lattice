using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Internal helper for encoding and decoding the per-entry causal-plus
/// frontier carried on <see cref="ReplogEntry.VectorClock"/>. The codec
/// supports both an <em>absolute</em> form (the full frontier as a
/// stand-alone <see cref="VersionVector"/>) and a <em>delta</em> form
/// (only the entries whose clock strictly advances against a named
/// predecessor frontier).
/// </summary>
/// <remarks>
/// <para>
/// Replication transports that ship dense per-shard offsets can save
/// wire bytes by delta-encoding each entry's frontier against its
/// predecessor on the same shard, then restoring the absolute frontier
/// on the receiving side. To keep the trim-from-the-head invariant
/// safe, callers must encode an <em>absolute</em> frontier on every
/// batch boundary and on any entry whose predecessor was trimmed by
/// log GC: <see cref="DecodeDelta(VersionVector, VersionVector)"/>
/// against a missing or empty predecessor returns the delta verbatim,
/// which collapses to the empty frontier when the delta itself is empty
/// - so a delta-only entry whose predecessor has vanished would be
/// silently demoted to "no causal dependencies".
/// </para>
/// <para>
/// The codec is purely a pair of pointwise-max / sparse-difference
/// computations over <see cref="VersionVector"/>. It does not allocate
/// per call beyond the result vector and never mutates either input.
/// </para>
/// </remarks>
internal static class VectorClockCodec
{
    /// <summary>
    /// Returns an absolute snapshot of <paramref name="current"/>. A
    /// <see langword="null"/> input is treated as the empty frontier
    /// and produces an empty (non-<see langword="null"/>) vector so
    /// callers can write it into <see cref="ReplogEntry.VectorClock"/>
    /// without a null guard. The returned vector is a fresh instance
    /// independent of <paramref name="current"/>; subsequent advances
    /// to the input do not leak into the encoded snapshot.
    /// </summary>
    /// <param name="current">
    /// The frontier to snapshot, or <see langword="null"/> for the
    /// empty frontier.
    /// </param>
    public static VersionVector EncodeAbsolute(VersionVector? current)
    {
        var result = new VersionVector();
        if (current is null)
        {
            return result;
        }

        foreach (var (id, clock) in current.Entries)
        {
            result.Entries[id] = clock;
        }

        return result;
    }

    /// <summary>
    /// Returns the sparse difference of <paramref name="current"/>
    /// against <paramref name="predecessor"/>: every origin whose clock
    /// strictly advances (or is absent from the predecessor) appears
    /// with its <em>current</em> clock; origins whose clock is
    /// unchanged or has regressed are omitted. Treats either argument
    /// being <see langword="null"/> as the empty frontier - a
    /// <see langword="null"/> predecessor produces a delta identical
    /// to <see cref="EncodeAbsolute(VersionVector?)"/>.
    /// </summary>
    /// <remarks>
    /// The shape is symmetric with
    /// <see cref="DecodeDelta(VersionVector, VersionVector)"/>: for any
    /// pair <c>(current, predecessor)</c>,
    /// <c>DecodeDelta(EncodeDelta(current, predecessor), predecessor)</c>
    /// reproduces the absolute clock on every origin
    /// <paramref name="current"/> did not regress past the predecessor.
    /// </remarks>
    /// <param name="current">The frontier to difference against the predecessor.</param>
    /// <param name="predecessor">The reference frontier from which to compute the diff.</param>
    public static VersionVector EncodeDelta(VersionVector? current, VersionVector? predecessor)
    {
        var delta = new VersionVector();
        if (current is null)
        {
            return delta;
        }

        foreach (var (id, clock) in current.Entries)
        {
            var prior = predecessor is null ? HybridLogicalClock.Zero : predecessor.GetClock(id);
            if (clock > prior)
            {
                delta.Entries[id] = clock;
            }
        }

        return delta;
    }

    /// <summary>
    /// Reconstructs an absolute frontier by pointwise-max-merging
    /// <paramref name="delta"/> into <paramref name="predecessor"/>.
    /// Either argument may be <see langword="null"/>: a
    /// <see langword="null"/> predecessor reduces the call to
    /// <see cref="EncodeAbsolute(VersionVector?)"/> over the delta, and
    /// a <see langword="null"/> delta reproduces the predecessor
    /// verbatim. Always returns a fresh instance; neither input is
    /// mutated.
    /// </summary>
    /// <param name="delta">
    /// The sparse advance to merge in, typically the output of an
    /// earlier <see cref="EncodeDelta(VersionVector?, VersionVector?)"/>
    /// call.
    /// </param>
    /// <param name="predecessor">
    /// The reference frontier the delta was computed against, or
    /// <see langword="null"/> when no predecessor is available - for
    /// example because the predecessor entry has been trimmed by log
    /// GC. In that case the delta must itself be an absolute frontier
    /// (i.e. the producer must have emitted an absolute encoding for
    /// this entry, per the codec contract).
    /// </param>
    public static VersionVector DecodeDelta(VersionVector? delta, VersionVector? predecessor)
    {
        var result = EncodeAbsolute(predecessor);
        if (delta is null)
        {
            return result;
        }

        foreach (var (id, clock) in delta.Entries)
        {
            if (result.Entries.TryGetValue(id, out var existing))
            {
                if (clock > existing)
                {
                    result.Entries[id] = clock;
                }
            }
            else
            {
                result.Entries[id] = clock;
            }
        }

        return result;
    }
}
