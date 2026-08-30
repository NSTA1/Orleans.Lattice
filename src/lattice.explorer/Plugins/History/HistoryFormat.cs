using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Explorer.Core.History;

namespace Orleans.Lattice.Explorer.Plugins.History;

/// <summary>
/// The revision timeline's presentation vocabulary: the badges, labels, class
/// suffixes and time formats its rows render. Pure functions over already-loaded
/// values, so they reach nothing and belong outside the view's state.
/// </summary>
internal static class HistoryFormat
{
    /// <summary>The dot class for a connection state, mirroring the shell's own indicator.</summary>
    internal static string LiveDotClass(LatticeConnectionState state) => state switch
    {
        LatticeConnectionState.Connected => "on",
        LatticeConnectionState.Connecting or LatticeConnectionState.Reconnecting => "warn",
        _ => "down",
    };

    /// <summary>The label for a connection state.</summary>
    internal static string LiveLabel(LatticeConnectionState state) => state switch
    {
        LatticeConnectionState.Connected => "Live",
        LatticeConnectionState.Connecting => "Connecting",
        LatticeConnectionState.Reconnecting => "Reconnecting",
        _ => "Offline",
    };

    /// <summary>The badge for the retention mode active over the key.</summary>
    internal static string RetentionBadge(HistoryRetentionMode mode, bool valueRetained) => mode switch
    {
        HistoryRetentionMode.FullValue => "full-value",
        HistoryRetentionMode.MetadataOnly => "metadata-only",
        HistoryRetentionMode.Hybrid => valueRetained ? "hybrid (value)" : "hybrid (metadata)",
        _ => mode.ToString(),
    };

    /// <summary>The badge for how far back the timeline reaches.</summary>
    internal static string BoundBadge(EntryHistoryBound bound) => bound switch
    {
        EntryHistoryBound.BoundedByAge => "durable",
        EntryHistoryBound.Truncated => "truncated",
        EntryHistoryBound.WalWindowFallback => "WAL-window fallback",
        _ => bound.ToString(),
    };

    /// <summary>The label for a revision's kind.</summary>
    internal static string KindLabel(HistoryRowKind kind) => kind switch
    {
        HistoryRowKind.Set => "set",
        HistoryRowKind.Delete => "delete",
        HistoryRowKind.CrdtDelta => "crdt-merge",
        HistoryRowKind.RangeTombstone => "range-delete",
        _ => kind.ToString().ToLowerInvariant(),
    };

    /// <summary>The class suffix for a revision's kind.</summary>
    internal static string KindClass(HistoryRowKind kind) => kind switch
    {
        HistoryRowKind.Set => "set",
        HistoryRowKind.Delete => "delete",
        HistoryRowKind.CrdtDelta => "crdt",
        HistoryRowKind.RangeTombstone => "range",
        _ => "other",
    };

    /// <summary>The class list for a timeline row, marking a not-yet-durable live marker.</summary>
    internal static string RowClass(HistoryRevisionRow row) =>
        row.IsLiveTail
            ? $"lx-history-row-{KindClass(row.Kind)} lx-history-row-live"
            : $"lx-history-row-{KindClass(row.Kind)}";

    /// <summary>The label for a CRDT member change.</summary>
    internal static string MemberLabel(CrdtMemberChangeKind kind) =>
        kind == CrdtMemberChangeKind.Added ? "added" : "removed";

    /// <summary>The class suffix for a CRDT member change.</summary>
    internal static string MemberClass(CrdtMemberChangeKind kind) =>
        kind == CrdtMemberChangeKind.Added ? "added" : "removed";

    /// <summary>The class suffix for a diff line.</summary>
    internal static string DiffClass(HistoryDiffLineKind kind) => kind switch
    {
        HistoryDiffLineKind.Added => "added",
        HistoryDiffLineKind.Removed => "removed",
        _ => "unchanged",
    };

    /// <summary>The gutter prefix for a diff line.</summary>
    internal static string DiffPrefix(HistoryDiffLineKind kind) => kind switch
    {
        HistoryDiffLineKind.Added => "+ ",
        HistoryDiffLineKind.Removed => "- ",
        _ => "  ",
    };

    /// <summary>The element text for a CRDT member change, naming an empty element rather than rendering nothing.</summary>
    internal static string ElementText(HistoryMemberChange member) =>
        member.ElementFormat == ValueFormat.Empty ? "(empty)" : member.ElementText;

    /// <summary>The origin label for a revision, naming the local cluster when none is carried.</summary>
    internal static string OriginLabel(string? originClusterId) =>
        string.IsNullOrEmpty(originClusterId) ? "local" : originClusterId;

    /// <summary>The rendered value hash, or a dash when none was recorded.</summary>
    internal static string FormatHash(long hash) =>
        hash == 0 ? "-" : "0x" + ((ulong)hash).ToString("x16");

    /// <summary>The absolute local wall-clock rendering of a hybrid logical clock.</summary>
    internal static string FormatAbsolute(HybridLogicalClock hlc)
    {
        if (hlc.WallClockTicks <= 0)
        {
            return "-";
        }

        var wall = new DateTime(hlc.WallClockTicks, DateTimeKind.Utc).ToLocalTime();
        return $"{wall:yyyy-MM-dd HH:mm:ss} (+{hlc.Counter})";
    }

    /// <summary>The coarse "time ago" rendering of a hybrid logical clock.</summary>
    internal static string FormatRelative(HybridLogicalClock hlc)
    {
        if (hlc.WallClockTicks <= 0)
        {
            return "-";
        }

        var elapsed = DateTime.UtcNow - new DateTime(hlc.WallClockTicks, DateTimeKind.Utc);
        if (elapsed < TimeSpan.Zero)
        {
            elapsed = TimeSpan.Zero;
        }

        if (elapsed.TotalSeconds < 60)
        {
            return $"{(int)elapsed.TotalSeconds}s ago";
        }

        if (elapsed.TotalMinutes < 60)
        {
            return $"{(int)elapsed.TotalMinutes}m ago";
        }

        if (elapsed.TotalHours < 24)
        {
            return $"{(int)elapsed.TotalHours}h ago";
        }

        return $"{(int)elapsed.TotalDays}d ago";
    }
}
