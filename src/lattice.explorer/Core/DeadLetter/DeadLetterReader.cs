using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Core.DeadLetter;

/// <summary>
/// Default <see cref="IDeadLetterReader"/> over the state-API dead-letter surface
/// (<c>GetDeadLetterCountAsync</c> / <c>ListDeadLettersAsync</c>). Maps each wire
/// entry to a per-row view model and preserves the producer's preview bounding
/// unchanged. Read-only: no requeue / replay path is offered.
/// </summary>
public sealed class DeadLetterReader(ILatticeStateClient client) : IDeadLetterReader
{
    private readonly ILatticeStateClient _client = client ?? throw new ArgumentNullException(nameof(client));

    /// <inheritdoc />
    public async Task<int> CountAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        var response = await _client
            .GetDeadLetterCountAsync(new DeadLetterCountRequest { TreeId = treeId }, cancellationToken)
            .ConfigureAwait(false);

        return response.Count;
    }

    /// <inheritdoc />
    public async Task<DeadLetterPage> ListAsync(
        string treeId,
        int pageSize,
        string? continuationToken = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        var request = new DeadLetterQueueRequest
        {
            TreeId = treeId,
            PageSize = pageSize,
            PageToken = string.IsNullOrEmpty(continuationToken) ? null : continuationToken,
        };

        var response = await _client.ListDeadLettersAsync(request, cancellationToken).ConfigureAwait(false);

        var entries = response.Entries.Count == 0
            ? Array.Empty<DeadLetterEntry>()
            : MapEntries(response.Entries);

        return new DeadLetterPage
        {
            Entries = entries,
            ContinuationToken = response.NextPageToken,
        };
    }

    private static DeadLetterEntry[] MapEntries(IReadOnlyList<DeadLetterEntryRecord> records)
    {
        var mapped = new DeadLetterEntry[records.Count];
        for (var i = 0; i < records.Count; i++)
        {
            mapped[i] = DeadLetterEntry.From(records[i]);
        }

        return mapped;
    }
}
