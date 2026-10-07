using System.Globalization;

namespace Orleans.Lattice.Replication;

/// <summary>
/// The sender's applied low watermark for one tree of one receiver (issue #4586):
/// every write the sender authored to the batch's tree and stamped strictly below
/// <see cref="TreeLowWatermark"/> was acknowledged by the receiver while the
/// receiver's tree was in lineage <see cref="ReceiverLineage"/>, and every write it
/// authored to any replicated tree and stamped strictly below
/// <see cref="OriginLowWatermark"/> was acknowledged by that receiver. A zero
/// watermark vouches for nothing. Travels out of band beside a batch (the gRPC
/// transport sends it as a call header), so the batch framing is unchanged.
/// </summary>
[GenerateSerializer]
[Immutable]
[Alias(ReplicationTypeAliases.ReplicationSourceFrontier)]
internal readonly record struct ReplicationSourceFrontier
{
    /// <summary>The longest text form <see cref="TryParse"/> accepts.</summary>
    public const int MaxTextLength = 128;

    private const string FormatVersion = "1";

    /// <summary>
    /// The receiver's tree lineage the covering acknowledgements were taken under,
    /// as echoed in <see cref="ReplicationAck.ReceiverLineage"/>. Never empty.
    /// </summary>
    [Id(0)] public Guid ReceiverLineage { get; init; }

    /// <summary>The low watermark over the batch's tree.</summary>
    [Id(1)] public HybridLogicalClock TreeLowWatermark { get; init; }

    /// <summary>
    /// The low watermark over every tree the sender replicates to the receiver:
    /// never above <see cref="TreeLowWatermark"/>, and zero while any of those
    /// trees has none.
    /// </summary>
    [Id(2)] public HybridLogicalClock OriginLowWatermark { get; init; }

    /// <summary>
    /// The sender's per-receiver aggregate generation: raised whenever the
    /// sender observes a lineage change of any of the receiver's trees, or a
    /// tree joins the replicated set. <see cref="OriginLowWatermark"/> is
    /// comparable only within one generation. Batches of different trees
    /// arrive in any order, so a receiver ignores an origin watermark from an
    /// older generation than one it has already seen: that watermark may have
    /// been computed over a tree's coverage from before its lineage changed.
    /// </summary>
    [Id(3)] public long OriginGeneration { get; init; }

    /// <summary>
    /// The shipper's acknowledged read positions (issue #4684), present only
    /// while it vouches the watermark. Not part of <see cref="ToText"/>: the gRPC
    /// transport ships it as its own header.
    /// </summary>
    [Id(4)] public ReplicationAckedPositions? AckedPositions { get; init; }

    /// <summary>Renders the canonical text form <see cref="TryParse"/> reads.</summary>
    public string ToText() => string.Join(
        '.',
        FormatVersion,
        ReceiverLineage.ToString("N", CultureInfo.InvariantCulture),
        TreeLowWatermark.WallClockTicks.ToString(CultureInfo.InvariantCulture),
        TreeLowWatermark.Counter.ToString(CultureInfo.InvariantCulture),
        OriginLowWatermark.WallClockTicks.ToString(CultureInfo.InvariantCulture),
        OriginLowWatermark.Counter.ToString(CultureInfo.InvariantCulture),
        OriginGeneration.ToString(CultureInfo.InvariantCulture));

    /// <summary>
    /// Parses the text form strictly. The text is wire input from a peer, so
    /// anything other than the exact canonical shape - an unknown version, an empty
    /// lineage, a negative component, an origin watermark above the tree's, a
    /// wrong field count, or over-long text - is refused, and the receiver treats the batch as carrying
    /// no watermark at all.
    /// </summary>
    public static bool TryParse(string? text, out ReplicationSourceFrontier frontier)
    {
        frontier = default;
        if (string.IsNullOrEmpty(text) || text.Length > MaxTextLength)
        {
            return false;
        }

        var parts = text.Split('.');
        if (parts.Length != 7
            || !string.Equals(parts[0], FormatVersion, StringComparison.Ordinal)
            || !Guid.TryParseExact(parts[1], "N", out var lineage)
            || lineage == Guid.Empty
            || !TryParseClock(parts[2], parts[3], out var tree)
            || !TryParseClock(parts[4], parts[5], out var origin)
            || origin.CompareTo(tree) > 0
            || !long.TryParse(parts[6], NumberStyles.None, CultureInfo.InvariantCulture, out var generation))
        {
            return false;
        }

        frontier = new ReplicationSourceFrontier
        {
            ReceiverLineage = lineage,
            TreeLowWatermark = tree,
            OriginLowWatermark = origin,
            OriginGeneration = generation,
        };
        return true;
    }

    private static bool TryParseClock(string wall, string counter, out HybridLogicalClock clock)
    {
        clock = default;
        if (!long.TryParse(wall, NumberStyles.None, CultureInfo.InvariantCulture, out var ticks)
            || !int.TryParse(counter, NumberStyles.None, CultureInfo.InvariantCulture, out var count))
        {
            return false;
        }

        clock = new HybridLogicalClock { WallClockTicks = ticks, Counter = count };
        return true;
    }
}
