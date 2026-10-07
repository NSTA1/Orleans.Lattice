using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// The bootstrap drop floor a receiver tree holds (issue #4549). A full
/// bootstrap's export carries, per origin <c>o</c>, the source's applied low
/// watermark <c>S(o)</c> and the writes of <c>o</c> the source held without
/// applying (<c>H(o)</c>) when the export opened. Every write of <c>o</c>
/// stamped below <c>S(o)</c> and not in <c>H(o)</c> was applied at the source
/// before the export opened, so the export already reflects it: carried, or
/// superseded by a later write or a delete the export carries. A later
/// delivery of such a write is dropped (acknowledged without being merged), so
/// a write still in flight from a third cluster cannot resurrect a key the
/// source deleted and reaped.
/// <para>
/// The floor belongs to the receiver's lineage of the tree: it is cleared
/// whenever the tree's applied identities are reset, which the tree frontier
/// does on every replacement of the tree's contents. A later bootstrap replaces
/// it. Rows the bootstrap drain itself applies are never subject to it.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ReplicationBootstrapFloor)]
internal sealed class ReplicationBootstrapFloor
{
    /// <summary>Most writes of one origin the floor exempts as held at the source.</summary>
    internal const int MaxHeldPerOrigin = 16_384;

    /// <summary>Most origins one floor covers.</summary>
    internal const int MaxOrigins = 1_024;

    /// <summary>Per origin, the source's applied low watermark at export open.</summary>
    [Id(0)] public Dictionary<string, HybridLogicalClock> LowWatermarks { get; set; } = new(StringComparer.Ordinal);

    /// <summary>Per origin, the writes below its low watermark the source held without applying.</summary>
    [Id(1)] public Dictionary<string, HashSet<HybridLogicalClock>> Held { get; set; } = new(StringComparer.Ordinal);

    /// <summary>
    /// Whether the import that installed the floor has yet to close against a
    /// stable source. While it has not, a delivery below the floor is deferred,
    /// not dropped: the import may turn out unstable, and a dropped delivery is
    /// acknowledged to its sender and never re-sent.
    /// </summary>
    [Id(2)] public bool Provisional { get; set; } = true;

    /// <summary>
    /// Builds a floor from an export's per-origin low watermarks and held
    /// writes, or returns <see langword="null"/> - no floor, which drops
    /// nothing - when the export carries no watermark or exceeds a bound.
    /// </summary>
    internal static ReplicationBootstrapFloor? From(
        IReadOnlyDictionary<string, HybridLogicalClock> lowWatermarks,
        IReadOnlyDictionary<string, HybridLogicalClock[]> held)
    {
        ArgumentNullException.ThrowIfNull(lowWatermarks);
        ArgumentNullException.ThrowIfNull(held);
        if (lowWatermarks.Count > MaxOrigins)
        {
            return null;
        }

        var floor = new ReplicationBootstrapFloor();
        foreach (var (origin, lowWatermark) in lowWatermarks)
        {
            if (string.IsNullOrEmpty(origin) || lowWatermark == HybridLogicalClock.Zero)
            {
                continue;
            }

            if (held.TryGetValue(origin, out var identities) && identities.Length > 0)
            {
                if (identities.Length > MaxHeldPerOrigin)
                {
                    return null;
                }

                floor.Held[origin] = new HashSet<HybridLogicalClock>(identities);
            }

            floor.LowWatermarks[origin] = lowWatermark;
        }

        return floor.LowWatermarks.Count == 0 ? null : floor;
    }

    /// <summary>The admission read for <paramref name="originClusterId"/>.</summary>
    internal (HybridLogicalClock LowWatermark, HybridLogicalClock[] Held) For(string originClusterId)
    {
        if (!LowWatermarks.TryGetValue(originClusterId, out var lowWatermark))
        {
            return (HybridLogicalClock.Zero, Array.Empty<HybridLogicalClock>());
        }

        return (lowWatermark, Held.TryGetValue(originClusterId, out var held) && held.Count > 0
            ? [.. held]
            : Array.Empty<HybridLogicalClock>());
    }
}
