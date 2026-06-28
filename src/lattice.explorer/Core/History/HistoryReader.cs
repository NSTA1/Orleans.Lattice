using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Core.History;

/// <summary>
/// Default <see cref="IHistoryReader"/> over the state-API change-history surface
/// (<c>GetEntryHistoryAsync</c>). Maps each wire revision to a per-row view model
/// and surfaces the history-bound metadata unchanged.
/// </summary>
public sealed class HistoryReader(ILatticeStateClient client) : IHistoryReader
{
    /// <summary>The per-revision value / delta preview byte budget requested for history reads.</summary>
    public const int HistoryPreviewBudget = 4096;

    private readonly ILatticeStateClient _client = client ?? throw new ArgumentNullException(nameof(client));

    /// <inheritdoc />
    public async Task<HistoryPage> LoadAsync(
        string treeId,
        string key,
        int limit,
        string? continuationToken = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(key);

        var request = new EntryHistoryRequest
        {
            TreeId = treeId,
            Key = key,
            Limit = limit,
            ContinuationToken = string.IsNullOrEmpty(continuationToken) ? null : continuationToken,
            ValuePreviewBudget = HistoryPreviewBudget,

            // Always page oldest-first so accumulated pages form a stable
            // chronological list; the display order is applied by the timeline.
            Reverse = false,
        };

        var response = await _client.GetEntryHistoryAsync(request, cancellationToken).ConfigureAwait(false);

        var revisions = response.Revisions.Count == 0
            ? Array.Empty<HistoryRevisionRow>()
            : MapRevisions(response.Revisions);

        return new HistoryPage
        {
            Status = response.Status,
            Revisions = revisions,
            Bound = response.Bound,
            EarliestAvailable = response.EarliestAvailable,
            ContinuationToken = response.ContinuationToken,
        };
    }

    private static HistoryRevisionRow[] MapRevisions(IReadOnlyList<EntryRevisionRecord> records)
    {
        var mapped = new HistoryRevisionRow[records.Count];
        for (var i = 0; i < records.Count; i++)
        {
            mapped[i] = HistoryRevisionRow.From(records[i]);
        }

        return mapped;
    }
}
