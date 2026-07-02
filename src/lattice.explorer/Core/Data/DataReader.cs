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
        string? keyPrefix = null,
        EntryScanMode mode = EntryScanMode.Live,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        // A key prefix narrows the scan to a ranged seek over the sorted keys
        // [prefix, prefixUpperBound). The state-API key range is ignored on a
        // tag-filtered scan, so the prefix is honoured only when no tag filter is
        // active (the UI also disables the prefix box while a tag filter is on).
        var applyPrefix = tagFilter is null && !string.IsNullOrEmpty(keyPrefix);

        var request = new EntryScanRequest
        {
            TreeId = treeId,
            PageSize = DataPaging.Normalize(pageSize),
            ContinuationToken = string.IsNullOrEmpty(continuationToken) ? null : continuationToken,
            ValuePreviewBudget = ScanPreviewBudget,
            StartInclusive = applyPrefix ? keyPrefix : null,
            EndExclusive = applyPrefix ? PrefixUpperBound(keyPrefix!) : null,
            IndexName = tagFilter?.IndexName,
            Tag = tagFilter?.Tag,
            // The default is a live cursor so a casual browse does not fan an
            // all-shard snapshot-baseline capture out to every shard root; the
            // Data tab lets the user opt into Snapshot for a consistent view.
            Mode = mode,
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
    public async Task<IReadOnlyList<string>> ListTagValuesForIndexAsync(
        string treeId,
        string indexName,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(indexName);

        var values = new List<string>();
        string? token = null;
        do
        {
            var page = await _client.ListTagValuesAsync(
                new CatalogRequest { SourceTreeId = treeId, IndexName = indexName, PageToken = token },
                cancellationToken).ConfigureAwait(false);

            values.AddRange(page.Entries);
            token = page.NextPageToken;
        }
        while (!string.IsNullOrEmpty(token));

        return values;
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<string>> ListCoveredTreesForIndexAsync(
        string indexName,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(indexName);

        var trees = new List<string>();
        string? token = null;
        do
        {
            var page = await _client.ListCoveredTreesAsync(
                new CatalogRequest { IndexName = indexName, PageToken = token },
                cancellationToken).ConfigureAwait(false);

            trees.AddRange(page.Entries);
            token = page.NextPageToken;
        }
        while (!string.IsNullOrEmpty(token));

        return trees;
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<string>> ListTagsForIndexAsync(
        string indexName,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(indexName);

        var tags = new List<string>();
        string? token = null;
        do
        {
            var page = await _client.ListIndexTagsAsync(
                new CatalogRequest { IndexName = indexName, PageToken = token },
                cancellationToken).ConfigureAwait(false);

            tags.AddRange(page.Entries);
            token = page.NextPageToken;
        }
        while (!string.IsNullOrEmpty(token));

        return tags;
    }

    /// <inheritdoc />
    public async Task<TagMemberPage> ScanTagMembersAsync(
        string indexName,
        string tag,
        int pageSize,
        string? continuationToken = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(indexName);
        ArgumentException.ThrowIfNullOrEmpty(tag);

        var request = new TagMemberScanRequest
        {
            IndexName = indexName,
            Tag = tag,
            PageSize = DataPaging.Normalize(pageSize),
            PageToken = string.IsNullOrEmpty(continuationToken) ? null : continuationToken,
        };

        var page = await _client.ScanTagMembersAsync(request, cancellationToken).ConfigureAwait(false);

        var members = page.Entries.Count == 0
            ? Array.Empty<TagMemberRow>()
            : page.Entries.Select(static m => new TagMemberRow { TreeId = m.TreeId, Key = m.Key }).ToArray();

        return new TagMemberPage
        {
            Members = members,
            ContinuationToken = page.NextPageToken,
        };
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

    /// <summary>
    /// Computes the exclusive upper bound for a starts-with scan over
    /// ordinal-sorted keys: the smallest key that sorts strictly after every key
    /// beginning with <paramref name="prefix"/>. Found by incrementing the last
    /// code unit below <see cref="char.MaxValue"/> and dropping the tail. Returns
    /// <see langword="null"/> when the prefix is empty or consists entirely of
    /// <c>U+FFFF</c> code units, for which no finite upper bound exists (the scan
    /// then runs to the last key).
    /// </summary>
    internal static string? PrefixUpperBound(string prefix)
    {
        for (var i = prefix.Length - 1; i >= 0; i--)
        {
            if (prefix[i] < char.MaxValue)
            {
                var bound = new char[i + 1];
                prefix.AsSpan(0, i + 1).CopyTo(bound);
                bound[i]++;
                return new string(bound);
            }
        }

        return null;
    }

    /// <inheritdoc />
    public async Task CancelScanAsync(string treeId, string? continuationToken, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        // An empty token names no cursor: nothing to release, so skip the round trip.
        if (string.IsNullOrEmpty(continuationToken))
        {
            return;
        }

        var request = new EntryScanCancelRequest { TreeId = treeId, ContinuationToken = continuationToken };
        await _client.CancelScanAsync(request, cancellationToken).ConfigureAwait(false);
    }
}
