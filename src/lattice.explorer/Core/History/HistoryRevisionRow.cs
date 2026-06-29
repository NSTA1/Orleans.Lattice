using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Data;

namespace Orleans.Lattice.Explorer.Core.History;

/// <summary>
/// A display-ready projection of a single <see cref="EntryRevisionRecord"/> for
/// the History tab. Carries the per-row metadata, the render branch the UI takes,
/// the rendered value (when retained), the decoded CRDT member changes (when a
/// CRDT delta was retained), and - filled in by
/// <see cref="HistoryTimeline.Build"/> from the neighbouring rows - the line diff
/// against the previous value-retaining revision and any retention-shape divider.
/// The wire <see cref="EntryRevisionRecord"/> never reaches the UI.
/// </summary>
public sealed record HistoryRevisionRow
{
    /// <summary>The revision's hybrid-logical-clock timestamp - the timeline order key.</summary>
    public HybridLogicalClock Hlc { get; init; }

    /// <summary>What the underlying source mutation was.</summary>
    public HistoryRowKind Kind { get; init; }

    /// <summary>The render branch the History tab should take for this row.</summary>
    public HistoryRowRenderMode RenderMode { get; init; }

    /// <summary>
    /// The declared convergence rule of the source mutation. A
    /// <see cref="HistoryRowKind.Set"/> row whose <see cref="Mode"/> is not
    /// <see cref="LatticeMergeMode.LwwRegister"/> is a CRDT full-state snapshot
    /// (an anti-entropy / bootstrap resync), not a last-writer-wins overwrite, and
    /// renders as a CRDT membership snapshot rather than a raw value diff.
    /// </summary>
    public LatticeMergeMode Mode { get; init; }

    /// <summary>
    /// Whether this row is a CRDT full-state snapshot (a CRDT-mode
    /// <see cref="HistoryRowKind.Set"/>) rather than an author delta. A snapshot
    /// shows the decoded current membership; the UI flags it distinctly from a
    /// per-write delta so a resync row is not mistaken for a fresh edit.
    /// </summary>
    public bool IsSnapshot { get; init; }

    /// <summary>Identifier of the cluster that authored the source mutation, or <see langword="null"/> for a local write.</summary>
    public string? OriginClusterId { get; init; }

    /// <summary>The full byte length of the source LWW value, or <c>0</c> when the revision carried no value.</summary>
    public int ValueLength { get; init; }

    /// <summary>A content hash (xxHash64) of the source LWW value, or <c>0</c> when the revision carried no value.</summary>
    public long ValueHash { get; init; }

    /// <summary>Whether the value or delta preview was clipped to the preview budget.</summary>
    public bool Truncated { get; init; }

    /// <summary>The retention mode applied when this revision was written.</summary>
    public HistoryRetentionMode RetentionMode { get; init; }

    /// <summary>Whether this revision actually carries its value bytes.</summary>
    public bool ValueRetained { get; init; }

    /// <summary>
    /// The rendered value preview for a <see cref="HistoryRowRenderMode.ValueDiff"/>
    /// row; <see langword="null"/> for every other render mode.
    /// </summary>
    public RenderedValue? Value { get; init; }

    /// <summary>
    /// The line diff against the previous value-retaining revision, populated by
    /// <see cref="HistoryTimeline.Build"/>. Empty for the oldest retained revision
    /// (nothing to diff against) and for non-value rows.
    /// </summary>
    public IReadOnlyList<HistoryDiffLine> Diff { get; init; } = Array.Empty<HistoryDiffLine>();

    /// <summary>
    /// The decoded element-level member changes for a
    /// <see cref="HistoryRowRenderMode.CrdtMembers"/> row; empty when the CRDT
    /// delta was metadata-only or truncated (the UI then shows a metadata affordance).
    /// </summary>
    public IReadOnlyList<HistoryMemberChange> MemberChanges { get; init; } = Array.Empty<HistoryMemberChange>();

    /// <summary>The exclusive upper bound of the swept range for a range-tombstone marker; <see langword="null"/> otherwise.</summary>
    public string? EndKey { get; init; }

    /// <summary>
    /// Whether this row is a live-tail row appended from the forward change feed
    /// (see <see cref="FromLive"/>) rather than a durable revision returned by a
    /// history page. A live-tail row carries only notification metadata and is
    /// never diffed, never advances the diff baseline, and never emits a
    /// retention divider; it is pending a durable backfill.
    /// </summary>
    public bool IsLiveTail { get; init; }

    /// <summary>
    /// The opaque, monotonic resume cursor of the source change notification for a
    /// live-tail row; <see langword="null"/> for a durable revision. Carried for
    /// diagnostics only - cross-source de-duplication keys on <see cref="Hlc"/>,
    /// which both a loaded page row and a live notification carry.
    /// </summary>
    public string? Position { get; init; }

    /// <summary>
    /// Whether the source change was a user-driven write or library maintenance,
    /// for a live-tail row. Defaults to <see cref="MutationCategory.User"/> for a
    /// durable revision (the history view never records maintenance churn).
    /// </summary>
    public MutationCategory Category { get; init; }

    /// <summary>
    /// The retention-shape divider that sits immediately before this row, when its
    /// retention descriptor differs from the chronologically previous revision;
    /// <see langword="null"/> otherwise. Filled in by <see cref="HistoryTimeline.Build"/>.
    /// </summary>
    public RetentionTransition? RetentionChange { get; init; }

    /// <summary>Projects a wire <see cref="EntryRevisionRecord"/> into a per-row view model (no neighbour-derived fields yet).</summary>
    public static HistoryRevisionRow From(EntryRevisionRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);

        var renderMode = ResolveRenderMode(record);
        var value = renderMode == HistoryRowRenderMode.ValueDiff
            ? ValueRenderer.Render(record.ValuePreview ?? Array.Empty<byte>(), record.Truncated)
            : null;

        var members = renderMode == HistoryRowRenderMode.CrdtMembers && record.MemberChanges.Count > 0
            ? MapMembers(record.MemberChanges)
            : Array.Empty<HistoryMemberChange>();

        return new HistoryRevisionRow
        {
            Hlc = record.Hlc,
            Kind = record.Kind,
            RenderMode = renderMode,
            Mode = record.Mode,
            IsSnapshot = renderMode == HistoryRowRenderMode.CrdtMembers && record.Kind == HistoryRowKind.Set,
            OriginClusterId = record.OriginClusterId,
            ValueLength = record.ValueLength,
            ValueHash = record.ValueHash,
            Truncated = record.Truncated,
            RetentionMode = record.Retention.Mode,
            ValueRetained = record.Retention.ValueRetained,
            Value = value,
            MemberChanges = members,
            EndKey = record.EndKey,
        };
    }

    /// <summary>
    /// Projects a forward-feed <see cref="StateChangeNotification"/> into a
    /// lightweight live-tail row. The notification carries only metadata (tree,
    /// key, kind, clock, category, position), so the row has no value preview,
    /// line diff, or CRDT member list - it renders as a metadata-only marker that
    /// is clearly the live tail pending a durable backfill. The live feed only
    /// carries changes emitted while subscribed; it extends the timeline forward
    /// from "now" and is not a substitute for durable history retention.
    /// </summary>
    public static HistoryRevisionRow FromLive(StateChangeNotification notification)
    {
        ArgumentNullException.ThrowIfNull(notification);

        return new HistoryRevisionRow
        {
            Hlc = notification.Hlc,
            Kind = MapLiveKind(notification.Kind),
            RenderMode = HistoryRowRenderMode.LiveTail,

            // The change feed carries no origin-cluster id; the row renders as a
            // local-origin live marker.
            OriginClusterId = null,
            EndKey = notification.EndExclusiveKey,
            IsLiveTail = true,
            Position = notification.Position,
            Category = notification.Category,
        };
    }

    private static HistoryRowKind MapLiveKind(StateChangeKind kind) => kind switch
    {
        StateChangeKind.Set => HistoryRowKind.Set,
        StateChangeKind.Delete => HistoryRowKind.Delete,
        StateChangeKind.DeleteRange => HistoryRowKind.RangeTombstone,
        _ => HistoryRowKind.Set,
    };

    private static HistoryRowRenderMode ResolveRenderMode(EntryRevisionRecord record) => record.Kind switch
    {
        HistoryRowKind.CrdtDelta => HistoryRowRenderMode.CrdtMembers,
        HistoryRowKind.Delete => HistoryRowRenderMode.Delete,
        HistoryRowKind.RangeTombstone => HistoryRowRenderMode.RangeTombstone,
        // A CRDT-mode Set is a full-state snapshot (anti-entropy / bootstrap
        // resync), not an LWW overwrite: render its decoded membership rather than
        // the raw serialized blob that overflows the value preview cap.
        HistoryRowKind.Set when record.Mode != LatticeMergeMode.LwwRegister => HistoryRowRenderMode.CrdtMembers,
        // An LWW Set renders its value only when the bytes were actually retained;
        // otherwise it is a metadata-only row (hash + length, never a diff).
        _ => record.Retention.ValueRetained ? HistoryRowRenderMode.ValueDiff : HistoryRowRenderMode.MetadataOnly,
    };

    private static HistoryMemberChange[] MapMembers(IReadOnlyList<CrdtMemberChange> changes)
    {
        var mapped = new HistoryMemberChange[changes.Count];
        for (var i = 0; i < changes.Count; i++)
        {
            mapped[i] = HistoryMemberChange.From(changes[i]);
        }

        return mapped;
    }
}
