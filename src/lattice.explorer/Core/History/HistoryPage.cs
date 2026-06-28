using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;

namespace Orleans.Lattice.Explorer.Core.History;

/// <summary>
/// One page of a key's change history as mapped by <see cref="IHistoryReader"/>:
/// the per-row revision view models in chronological (oldest-first) order plus
/// the history-bound metadata. The History tab accumulates pages and feeds the
/// growing chronological set to <see cref="HistoryTimeline.Build"/> for display.
/// </summary>
public sealed record HistoryPage
{
    /// <summary>Lookup outcome for the key's history.</summary>
    public StateQueryStatus Status { get; init; }

    /// <summary>The page's revisions in chronological (oldest-first) order.</summary>
    public IReadOnlyList<HistoryRevisionRow> Revisions { get; init; } = Array.Empty<HistoryRevisionRow>();

    /// <summary>How the returned timeline is bounded below.</summary>
    public EntryHistoryBound Bound { get; init; }

    /// <summary>On a truncated page, the oldest still-readable revision's clock; <see cref="HybridLogicalClock.Zero"/> otherwise.</summary>
    public HybridLogicalClock EarliestAvailable { get; init; }

    /// <summary>The continuation token for the next page, or <see langword="null"/> when fully drained.</summary>
    public string? ContinuationToken { get; init; }
}
