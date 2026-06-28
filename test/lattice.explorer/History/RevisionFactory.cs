using System.Text;
using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Explorer.Tests.History;

/// <summary>
/// Builds <see cref="EntryRevisionRecord"/> fixtures for the history view-model
/// tests, defaulting every field so a test sets only what it asserts on.
/// </summary>
internal static class RevisionFactory
{
    public static EntryRevisionRecord Set(
        long ticks,
        string? value = null,
        bool valueRetained = true,
        HistoryRetentionMode mode = HistoryRetentionMode.FullValue,
        int valueLength = 0,
        long valueHash = 0,
        bool truncated = false,
        string? originClusterId = null)
    {
        var bytes = value is null ? null : Encoding.UTF8.GetBytes(value);
        return new EntryRevisionRecord
        {
            SourceKey = "k",
            Hlc = Hlc(ticks),
            Kind = HistoryRowKind.Set,
            OriginClusterId = originClusterId,
            ValuePreview = valueRetained ? bytes : null,
            ValueLength = valueLength != 0 ? valueLength : bytes?.Length ?? 0,
            ValueHash = valueHash,
            Truncated = truncated,
            Mode = LatticeMergeMode.LwwRegister,
            Retention = new RevisionRetention { Mode = mode, ValueRetained = valueRetained },
        };
    }

    public static EntryRevisionRecord Crdt(
        long ticks,
        IReadOnlyList<CrdtMemberChange>? members = null,
        bool valueRetained = true,
        HistoryRetentionMode mode = HistoryRetentionMode.FullValue)
    {
        return new EntryRevisionRecord
        {
            SourceKey = "k",
            Hlc = Hlc(ticks),
            Kind = HistoryRowKind.CrdtDelta,
            Mode = LatticeMergeMode.OrSet,
            MemberChanges = members ?? Array.Empty<CrdtMemberChange>(),
            Retention = new RevisionRetention { Mode = mode, ValueRetained = valueRetained },
        };
    }

    public static EntryRevisionRecord Delete(long ticks) => new()
    {
        SourceKey = "k",
        Hlc = Hlc(ticks),
        Kind = HistoryRowKind.Delete,
        Retention = new RevisionRetention { Mode = HistoryRetentionMode.FullValue, ValueRetained = false },
    };

    public static EntryRevisionRecord RangeTombstone(long ticks, string endKey) => new()
    {
        SourceKey = "k",
        EndKey = endKey,
        Hlc = Hlc(ticks),
        Kind = HistoryRowKind.RangeTombstone,
        Retention = new RevisionRetention { Mode = HistoryRetentionMode.FullValue, ValueRetained = false },
    };

    public static CrdtMemberChange Member(string element, CrdtMemberChangeKind kind, string replica, long ordinal = 1) => new()
    {
        Element = Encoding.UTF8.GetBytes(element),
        Kind = kind,
        ReplicaId = replica,
        Ordinal = ordinal,
    };

    public static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };
}
