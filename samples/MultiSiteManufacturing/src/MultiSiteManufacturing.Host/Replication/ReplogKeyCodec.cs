using System.Globalization;
using System.Text;
using Orleans.Lattice.Primitives;

namespace MultiSiteManufacturing.Host.Replication;

/// <summary>
/// Encodes / decodes replog entry keys. The canonical form is
/// <c>{wallTicks:D20}{counter:D10}|{clusterId}|{op}|{key}</c> —
/// fixed-width zero-padded HLC components first so a forward
/// lexicographic scan is HLC-ascending; cluster id as the tiebreaker
/// when two clusters mint identical HLCs; op letter (<c>S</c> for
/// set, <c>D</c> for delete) purely for audit; original key
/// appended last.
/// </summary>
/// <remarks>
/// <b>FUTURE seam.</b> The replog is a discovery mechanism that
/// disappears once the library ships a native change feed. This
/// codec is deliberately isolated so it can be deleted in one hop.
/// </remarks>
internal static class ReplogKeyCodec
{
    private const int WallWidth = 20;
    private const int CounterWidth = 10;
    private const char Sep = '|';

    /// <summary>Op marker letter for <see cref="ReplicationOp.Set"/>.</summary>
    public const char SetMarker = 'S';

    /// <summary>Op marker letter for <see cref="ReplicationOp.Delete"/>.</summary>
    public const char DeleteMarker = 'D';

    /// <summary>
    /// Encodes a replog key. <paramref name="clusterId"/> must not
    /// contain the separator character; <paramref name="originalKey"/>
    /// may contain anything — it is appended verbatim as the trailing
    /// segment.
    /// </summary>
    public static string Encode(HybridLogicalClock hlc, string clusterId, ReplicationOp op, string originalKey)
    {
        ArgumentNullException.ThrowIfNull(clusterId);
        ArgumentNullException.ThrowIfNull(originalKey);
        if (clusterId.IndexOf(Sep) >= 0)
        {
            throw new ArgumentException($"Cluster id must not contain '{Sep}'.", nameof(clusterId));
        }

        var marker = op switch
        {
            ReplicationOp.Set => SetMarker,
            ReplicationOp.Delete => DeleteMarker,
            _ => throw new ArgumentOutOfRangeException(nameof(op)),
        };

        var sb = new StringBuilder(WallWidth + CounterWidth + clusterId.Length + originalKey.Length + 5);
        sb.Append(hlc.WallClockTicks.ToString($"D{WallWidth}", CultureInfo.InvariantCulture));
        sb.Append(hlc.Counter.ToString($"D{CounterWidth}", CultureInfo.InvariantCulture));
        sb.Append(Sep);
        sb.Append(clusterId);
        sb.Append(Sep);
        sb.Append(marker);
        sb.Append(Sep);
        sb.Append(originalKey);
        return sb.ToString();
    }

    /// <summary>
    /// Attempts to parse a replog key. Returns <c>false</c> for any
    /// malformed input; out parameters are meaningful only on
    /// <c>true</c>.
    /// </summary>
    public static bool TryDecode(
        string replogKey,
        out HybridLogicalClock hlc,
        out string clusterId,
        out ReplicationOp op,
        out string originalKey)
    {
        hlc = default;
        clusterId = string.Empty;
        op = default;
        originalKey = string.Empty;

        if (string.IsNullOrEmpty(replogKey) || replogKey.Length <= WallWidth + CounterWidth + 5)
        {
            return false;
        }

        var wallSpan = replogKey.AsSpan(0, WallWidth);
        var counterSpan = replogKey.AsSpan(WallWidth, CounterWidth);
        if (!long.TryParse(wallSpan, NumberStyles.None, CultureInfo.InvariantCulture, out var wall)
            || !int.TryParse(counterSpan, NumberStyles.None, CultureInfo.InvariantCulture, out var counter))
        {
            return false;
        }

        var rest = replogKey.AsSpan(WallWidth + CounterWidth);
        if (rest.IsEmpty || rest[0] != Sep)
        {
            return false;
        }
        rest = rest[1..];

        var clusterEnd = rest.IndexOf(Sep);
        if (clusterEnd <= 0)
        {
            return false;
        }
        var clusterSpan = rest[..clusterEnd];
        rest = rest[(clusterEnd + 1)..];

        if (rest.Length < 2 || rest[1] != Sep)
        {
            return false;
        }
        if (rest[0] != SetMarker && rest[0] != DeleteMarker)
        {
            return false;
        }
        op = rest[0] == SetMarker ? ReplicationOp.Set : ReplicationOp.Delete;

        hlc = new HybridLogicalClock { WallClockTicks = wall, Counter = counter };
        clusterId = clusterSpan.ToString();
        originalKey = rest[2..].ToString();
        return true;
    }

    /// <summary>
    /// Returns the inclusive lower bound for a forward range scan
    /// that starts immediately <b>after</b> <paramref name="cursor"/>.
    /// Callers pass <c>(StartAfter(cursor, clusterId), ReplogEndBound)</c>
    /// to <c>ScanEntriesAsync</c>.
    /// </summary>
    /// <remarks>
    /// The <paramref name="clusterId"/> argument is only used to
    /// satisfy the encoder's separator-prefix constraint — the
    /// trailing segment is an empty original key, so any encoded
    /// replog entry strictly greater than the cursor sorts above the
    /// returned lower bound regardless of which cluster produced it.
    /// </remarks>
    public static string StartAfter(HybridLogicalClock cursor, string clusterId)
    {
        // Strictly above the cursor: bump the counter by one (or the
        // wall clock when the counter saturates).
        var next = cursor.Counter == int.MaxValue
            ? new HybridLogicalClock { WallClockTicks = cursor.WallClockTicks + 1, Counter = 0 }
            : new HybridLogicalClock { WallClockTicks = cursor.WallClockTicks, Counter = cursor.Counter + 1 };
        return Encode(next, clusterId, ReplicationOp.Set, string.Empty);
    }

    /// <summary>
    /// Exclusive upper bound for a full forward scan — a string that
    /// lex-sorts strictly above every valid replog key (fixed-width
    /// digits can only be '0'..'9' = 0x30..0x39, and we append a
    /// character above the separator to exceed every valid trailing
    /// segment).
    /// </summary>
    public static string ReplogEndBound { get; } =
        new string('9', WallWidth + CounterWidth) + ((char)(Sep + 1));

    /// <summary>
    /// Inclusive lower bound for scanning the entire replog from the
    /// beginning (used by the anti-entropy sweep on first run).
    /// </summary>
    public static string ReplogStartBound { get; } =
        new string('0', WallWidth + CounterWidth) + ((char)Sep);
}
