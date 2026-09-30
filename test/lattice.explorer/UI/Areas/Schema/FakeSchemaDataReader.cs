using System.Text;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Data;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

/// <summary>
/// A scripted <see cref="IDataReader"/> for the rule builder's sample: per-tree
/// values in key order, a fault, values whose scan preview is cut short (so the
/// full read is exercised), and a gate that holds the scan until a test releases it.
/// </summary>
internal sealed class FakeSchemaDataReader : IDataReader
{
    /// <summary>The values, by tree and key.</summary>
    public Dictionary<string, SortedDictionary<string, byte[]>> Trees { get; } = new(StringComparer.Ordinal);

    /// <summary>Keys whose scan preview is reported as cut short, so the sample reads them in full.</summary>
    public HashSet<string> Truncated { get; } = new(StringComparer.Ordinal);

    /// <summary>A fault a scan throws, when set.</summary>
    public Exception? ScanFault { get; set; }

    /// <summary>When set, a scan waits for it.</summary>
    public TaskCompletionSource? ScanGate { get; set; }

    /// <summary>How many scans ran.</summary>
    public int Scans { get; private set; }

    /// <summary>The cursors released.</summary>
    public List<string> Released { get; } = [];

    /// <summary>Sets a tree's values from JSON texts.</summary>
    /// <param name="treeId">The tree.</param>
    /// <param name="values">The keys and JSON texts.</param>
    public void Use(string treeId, params (string Key, string Json)[] values)
    {
        var tree = new SortedDictionary<string, byte[]>(StringComparer.Ordinal);
        foreach (var (key, json) in values)
        {
            tree[key] = Encoding.UTF8.GetBytes(json);
        }

        Trees[treeId] = tree;
    }

    /// <inheritdoc />
    public async Task<DataPage> ScanAsync(string treeId, int pageSize, string? continuationToken = null, TagFilter? tagFilter = null, string? keyPrefix = null, EntryScanMode mode = EntryScanMode.Live, CancellationToken cancellationToken = default)
    {
        Scans++;
        if (ScanGate is { } gate)
        {
            await gate.Task.WaitAsync(cancellationToken);
        }

        if (ScanFault is { } fault)
        {
            throw fault;
        }

        var tree = Trees.GetValueOrDefault(treeId) ?? [];
        var entries = tree.Take(pageSize).Select(pair => new DataEntry
        {
            Key = pair.Key,
            Value = Truncated.Contains(pair.Key) ? pair.Value[..Math.Min(4, pair.Value.Length)] : pair.Value,
            ValueLength = pair.Value.Length,
            Truncated = Truncated.Contains(pair.Key),
        }).ToArray();
        return new DataPage { Entries = entries, ContinuationToken = tree.Count > pageSize ? "more" : null };
    }

    /// <inheritdoc />
    public Task<DataEntry?> GetEntryAsync(string treeId, string key, CancellationToken cancellationToken = default) =>
        Task.FromResult(Trees.GetValueOrDefault(treeId)?.GetValueOrDefault(key) is { } value
            ? new DataEntry { Key = key, Value = value, ValueLength = value.Length }
            : null);

    /// <inheritdoc />
    public Task CancelScanAsync(string treeId, string? continuationToken, CancellationToken cancellationToken = default)
    {
        Released.Add(continuationToken ?? string.Empty);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<TagIndexRef>> ListTagIndexesForTreeAsync(string treeId, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException();

    /// <inheritdoc />
    public Task<IReadOnlyList<string>> ListTagValuesForIndexAsync(string treeId, string indexName, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException();

    /// <inheritdoc />
    public Task<IReadOnlyList<string>> ListCoveredTreesForIndexAsync(string indexName, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException();

    /// <inheritdoc />
    public Task<IReadOnlyList<string>> ListTagsForIndexAsync(string indexName, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException();

    /// <inheritdoc />
    public Task<TagMemberPage> ScanTagMembersAsync(string indexName, string tag, int pageSize, string? continuationToken = null, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException();
}
