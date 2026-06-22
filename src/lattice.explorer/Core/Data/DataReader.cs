using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Core.Data;

/// <summary>
/// Default <see cref="IDataReader"/> over the state-API entry surface
/// (<c>ScanEntriesAsync</c> / <c>GetEntryAsync</c>).
/// </summary>
public sealed class DataReader(ILatticeStateClient client) : IDataReader
{
    /// <summary>The per-entry value-preview budget requested for list scans.</summary>
    public const int ScanPreviewBudget = 512;

    private readonly ILatticeStateClient _client = client ?? throw new ArgumentNullException(nameof(client));

    /// <inheritdoc />
    public async Task<DataPage> ScanAsync(
        string treeId,
        int pageSize,
        string? continuationToken = null,
        TagFilter? tagFilter = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        var request = new EntryScanRequest
        {
            TreeId = treeId,
            PageSize = DataPaging.Normalize(pageSize),
            ContinuationToken = string.IsNullOrEmpty(continuationToken) ? null : continuationToken,
            ValuePreviewBudget = ScanPreviewBudget,
            IndexName = tagFilter?.IndexName,
            Tag = tagFilter?.Tag,
        };

        var response = await _client.ScanEntriesAsync(request, cancellationToken).ConfigureAwait(false);

        var entries = response.Entries.Count == 0
            ? Array.Empty<DataEntry>()
            : response.Entries.Select(DataEntry.From).ToArray();

        return new DataPage
        {
            Entries = entries,
            ContinuationToken = response.ContinuationToken,
        };
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<string>> ListTagIndexesForTreeAsync(
        string treeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        var names = new List<string>();
        string? token = null;
        do
        {
            var page = await _client.ListTagIndexesAsync(
                new CatalogRequest { SourceTreeId = treeId, PageToken = token },
                cancellationToken).ConfigureAwait(false);

            foreach (var entry in page.Entries)
            {
                names.Add(entry.IndexName);
            }

            token = page.NextPageToken;
        }
        while (!string.IsNullOrEmpty(token));

        return names;
    }

    /// <inheritdoc />
    public async Task<DataEntry?> GetEntryAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(key);

        var request = new EntryGetRequest { TreeId = treeId, Key = key };
        var response = await _client.GetEntryAsync(request, cancellationToken).ConfigureAwait(false);

        return response.Status == StateQueryStatus.Found && response.Entry is not null
            ? DataEntry.From(response.Entry)
            : null;
    }
}
