using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Explorer.Core.Data;

namespace Orleans.Lattice.Explorer.Core.History;

/// <summary>
/// The display-ordered timeline the History tab renders: the accumulated
/// revisions of a single key (across one or more loaded pages) enriched with the
/// neighbour-derived line diffs and retention-shape dividers, plus the
/// history-bound metadata (truncated / bounded-by-age / WAL-window fallback) and
/// the active retention mode for the key. Built by <see cref="Build"/> from the
/// per-row view models produced by the reader.
/// </summary>
public sealed record HistoryTimeline
{
    /// <summary>Lookup outcome for the key's history.</summary>
    public StateQueryStatus Status { get; init; }

    /// <summary>The tree id that was queried.</summary>
    public required string TreeId { get; init; }

    /// <summary>The key whose history was queried.</summary>
    public required string Key { get; init; }

    /// <summary>The revisions in display order (newest-first by default).</summary>
    public IReadOnlyList<HistoryRevisionRow> Rows { get; init; } = Array.Empty<HistoryRevisionRow>();

    /// <summary>How the returned timeline is bounded below.</summary>
    public EntryHistoryBound Bound { get; init; }

    /// <summary>On a truncated page, the oldest still-readable revision's clock; <see cref="HybridLogicalClock.Zero"/> otherwise.</summary>
    public HybridLogicalClock EarliestAvailable { get; init; }

    /// <summary>The continuation token for loading the next (older or newer) page, or <see langword="null"/> when fully drained.</summary>
    public string? ContinuationToken { get; init; }

    /// <summary>
    /// The retention mode of the newest revision - the active retention mode the
    /// tab surfaces as a badge - or <see langword="null"/> when there are no rows.
    /// </summary>
    public HistoryRetentionMode? ActiveRetentionMode { get; init; }

    /// <summary>Whether the newest revision retained its value bytes (qualifies a hybrid badge).</summary>
    public bool ActiveValueRetained { get; init; }

    /// <summary>Whether more revisions can be loaded with <see cref="ContinuationToken"/>.</summary>
    public bool HasMore => !string.IsNullOrEmpty(ContinuationToken);

    /// <summary>Whether the timeline has any revisions to render.</summary>
    public bool HasRows => Rows.Count > 0;

    /// <summary>
    /// Builds a display timeline from the chronologically-ordered (oldest-first)
    /// accumulated revisions. Walks them once oldest-to-newest to attach each
    /// value-retaining revision's line diff against the previous retained value
    /// and to mark a retention divider wherever an adjacent pair's retention
    /// descriptor differs, then orders the result newest-first when requested.
    /// </summary>
    /// <param name="treeId">The queried tree id.</param>
    /// <param name="key">The queried key.</param>
    /// <param name="status">The lookup outcome.</param>
    /// <param name="chronological">The accumulated revisions, oldest-first.</param>
    /// <param name="bound">How the timeline is bounded below.</param>
    /// <param name="earliestAvailable">The oldest still-readable clock on a truncated page.</param>
    /// <param name="continuationToken">The token for the next page, or <see langword="null"/>.</param>
    /// <param name="newestFirst">Whether to order the rendered rows newest-first.</param>
    public static HistoryTimeline Build(
        string treeId,
        string key,
        StateQueryStatus status,
        IReadOnlyList<HistoryRevisionRow> chronological,
        EntryHistoryBound bound,
        HybridLogicalClock earliestAvailable,
        string? continuationToken,
        bool newestFirst)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(chronological);

        var enriched = new HistoryRevisionRow[chronological.Count];
        string? previousValue = null;
        HistoryRevisionRow? previous = null;

        for (var i = 0; i < chronological.Count; i++)
        {
            var row = chronological[i];

            var diff = row.RenderMode == HistoryRowRenderMode.ValueDiff && row.Value is { } rendered
                ? HistoryValueDiff.Compute(previousValue, rendered.Content)
                : Array.Empty<HistoryDiffLine>();

            RetentionTransition? divider = null;
            if (previous is { } prev &&
                (prev.RetentionMode != row.RetentionMode || prev.ValueRetained != row.ValueRetained))
            {
                divider = new RetentionTransition
                {
                    From = prev.RetentionMode,
                    FromValueRetained = prev.ValueRetained,
                    To = row.RetentionMode,
                    ToValueRetained = row.ValueRetained,
                };
            }

            enriched[i] = row with { Diff = diff, RetentionChange = divider };

            // Only a value-retaining revision advances the diff baseline, so a
            // metadata-only or delete row between two retained values does not
            // reset the comparison.
            if (row.RenderMode == HistoryRowRenderMode.ValueDiff && row.Value is { } v)
            {
                previousValue = v.Content;
            }

            previous = row;
        }

        IReadOnlyList<HistoryRevisionRow> rows = enriched;
        if (newestFirst && enriched.Length > 1)
        {
            var reversed = new HistoryRevisionRow[enriched.Length];
            for (var i = 0; i < enriched.Length; i++)
            {
                reversed[i] = enriched[enriched.Length - 1 - i];
            }

            rows = reversed;
        }

        var newest = chronological.Count > 0 ? chronological[^1] : null;

        return new HistoryTimeline
        {
            Status = status,
            TreeId = treeId,
            Key = key,
            Rows = rows,
            Bound = bound,
            EarliestAvailable = earliestAvailable,
            ContinuationToken = continuationToken,
            ActiveRetentionMode = newest?.RetentionMode,
            ActiveValueRetained = newest?.ValueRetained ?? false,
        };
    }
}
