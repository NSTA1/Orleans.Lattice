using System.Globalization;
using Orleans.Lattice.Api.Replication;

namespace Orleans.Lattice.Explorer.UI.Areas.Replication;

/// <summary>
/// The Replication area's text for counts, sizes, durations, merge modes and
/// enrolment sources. Invariant culture throughout: the Explorer is not localised.
/// </summary>
internal static class ReplicationFormat
{
    private static readonly string[] ByteUnits = ["B", "KB", "MB", "GB", "TB", "PB"];

    /// <summary>A count with group separators, such as <c>1,204</c>.</summary>
    /// <param name="value">The count.</param>
    public static string Count(long value) => value.ToString("N0", CultureInfo.InvariantCulture);

    /// <summary>A count and its noun, singular or plural: <c>1 tree</c>, <c>3 trees</c>.</summary>
    /// <param name="value">The count.</param>
    /// <param name="singular">The singular noun.</param>
    /// <param name="plural">The plural noun.</param>
    public static string Count(long value, string singular, string plural) =>
        Count(value) + " " + (value == 1 ? singular : plural);

    /// <summary>A byte size in binary multiples, such as <c>3.2 MB</c>.</summary>
    /// <param name="bytes">The size in bytes; a negative size reads as zero.</param>
    public static string Bytes(long bytes)
    {
        if (bytes < 1024)
        {
            return Math.Max(0, bytes).ToString(CultureInfo.InvariantCulture) + " B";
        }

        double value = bytes;
        var unit = 0;

        // The unit is chosen on the figure as written, so a size that rounds up to
        // 1024 moves to the next unit rather than reading "1024 KB" (#4355).
        while (Math.Round(value, value >= 100 ? 0 : 1, MidpointRounding.AwayFromZero) >= 1024 && unit < ByteUnits.Length - 1)
        {
            value /= 1024;
            unit++;
        }

        return value.ToString(value >= 100 ? "0" : "0.0", CultureInfo.InvariantCulture) + " " + ByteUnits[unit];
    }

    /// <summary>The backlog of a link or an edge: <c>Caught up</c>, or entries and bytes behind.</summary>
    /// <param name="entries">Entries behind.</param>
    /// <param name="bytes">Bytes behind.</param>
    public static string Backlog(long entries, long bytes) =>
        entries <= 0 && bytes <= 0
            ? "Caught up"
            : Count(entries, "entry", "entries") + ", " + Bytes(bytes) + " behind";

    /// <summary>The time since a link last made contact: <c>Never</c>, <c>12 s ago</c>, <c>4 min ago</c>.</summary>
    /// <param name="elapsed">The elapsed time, or <see langword="null"/> when the link never made contact.</param>
    public static string Contact(TimeSpan? elapsed)
    {
        if (elapsed is not { } span)
        {
            return "Never";
        }

        if (span < TimeSpan.Zero)
        {
            span = TimeSpan.Zero;
        }

        if (span < TimeSpan.FromMinutes(1))
        {
            return ((long)span.TotalSeconds).ToString(CultureInfo.InvariantCulture) + " s ago";
        }

        if (span < TimeSpan.FromHours(1))
        {
            return ((long)span.TotalMinutes).ToString(CultureInfo.InvariantCulture) + " min ago";
        }

        if (span < TimeSpan.FromDays(1))
        {
            return ((long)span.TotalHours).ToString(CultureInfo.InvariantCulture) + " h "
                + span.Minutes.ToString(CultureInfo.InvariantCulture) + " min ago";
        }

        return ((long)span.TotalDays).ToString(CultureInfo.InvariantCulture) + " d ago";
    }

    /// <summary>The name of a merge mode as an operator reads it.</summary>
    /// <param name="mode">The merge mode.</param>
    public static string MergeMode(LatticeMergeMode mode) => mode switch
    {
        LatticeMergeMode.LwwRegister => "LWW register",
        LatticeMergeMode.OrSet => "OR-set",
        LatticeMergeMode.PnCounter => "PN-counter",
        LatticeMergeMode.VersionVector => "Version vector",
        LatticeMergeMode.MvRegister => "MV register",
        LatticeMergeMode.OrMap => "OR-map",
        LatticeMergeMode.Sequence => "Sequence",
        LatticeMergeMode.OrFlag => "OR-flag",
        LatticeMergeMode.RwFlag => "RW-flag",
        LatticeMergeMode.GCounter => "G-counter",
        LatticeMergeMode.GSet => "G-set",
        LatticeMergeMode.RwSet => "RW-set",
        LatticeMergeMode.MaxRegister => "Max register",
        LatticeMergeMode.MinRegister => "Min register",
        _ => mode.ToString(),
    };

    /// <summary>The merge mode of an enrolment, or a dash when none is in force.</summary>
    /// <param name="mode">The merge mode, or <see langword="null"/>.</param>
    public static string MergeMode(LatticeMergeMode? mode) => mode is { } value ? MergeMode(value) : "None";

    /// <summary>Where an enrolment came from.</summary>
    /// <param name="source">The enrolment source.</param>
    public static string Source(ReplicationEnrollmentSource source) => source switch
    {
        ReplicationEnrollmentSource.Runtime => "Runtime",
        ReplicationEnrollmentSource.Static => "Static",
        ReplicationEnrollmentSource.RuntimeAndStatic => "Runtime and static",
        _ => source.ToString(),
    };

    /// <summary>A link's direction, from this region's point of view.</summary>
    /// <param name="direction">The direction.</param>
    public static string Direction(ReplicationLinkDirection direction) =>
        direction == ReplicationLinkDirection.Inbound ? "Inbound" : "Outbound";
}
