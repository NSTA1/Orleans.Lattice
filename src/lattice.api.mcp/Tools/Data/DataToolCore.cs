using Orleans.Lattice.Api.Data;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The pure adapter mapping behind the data tools: each method translates a
/// tool's inputs into an <see cref="ILatticeDataApi"/> call and shapes the facade
/// result into the tool's plain structured-content DTO. Held separate from
/// <see cref="DataToolGroup"/> - which owns the MCP tool wiring and resolves the
/// facade from the request service provider - so the translation is unit-testable
/// against a fake facade without the MCP invocation envelope. The mapping adds no
/// authorization path of its own: fail-closed behaviour is inherited from the
/// facade (a denied read reports absent; a denied write throws).
/// </summary>
internal static partial class DataToolCore
{
    /// <summary>Maps <see cref="ILatticeDataApi.GetAsync"/> onto the <c>data_get</c> result.</summary>
    public static async Task<DataGetToolResult> GetAsync(
        ILatticeDataApi api,
        string treeId,
        string key,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(api);

        var result = await api.GetAsync(treeId, key, cancellationToken).ConfigureAwait(false);
        return new DataGetToolResult
        {
            TreeId = result.TreeId,
            Key = result.Key,
            Found = result.Found,
            Value = result.Value,
            MergeMode = result.MergeMode?.ToString(),
            Raw = result.Raw,
        };
    }

    /// <summary>Maps <see cref="ILatticeDataApi.ReadRangeAsync"/> onto the <c>data_read_range</c> result.</summary>
    public static async Task<DataRangePageToolResult> ReadRangeAsync(
        ILatticeDataApi api,
        string treeId,
        string? startInclusive,
        string? endExclusive,
        int pageSize,
        string? continuationToken,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(api);

        var request = new DataRangeRequest
        {
            TreeId = treeId,
            StartInclusive = startInclusive,
            EndExclusive = endExclusive,
            PageSize = pageSize,
            ContinuationToken = continuationToken,
        };

        var page = await api.ReadRangeAsync(request, cancellationToken).ConfigureAwait(false);

        var entries = new DataEntryDto[page.Entries.Count];
        for (var i = 0; i < page.Entries.Count; i++)
        {
            entries[i] = new DataEntryDto { Key = page.Entries[i].Key, Value = page.Entries[i].Value };
        }

        return new DataRangePageToolResult
        {
            TreeId = page.TreeId,
            Entries = entries,
            ContinuationToken = page.ContinuationToken,
        };
    }

    /// <summary>Maps <see cref="ILatticeDataApi.SetAsync"/> onto the <c>data_set</c> result.</summary>
    public static async Task<DataSetToolResult> SetAsync(
        ILatticeDataApi api,
        string treeId,
        string key,
        byte[] value,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(api);

        await api.SetAsync(treeId, key, value, cancellationToken).ConfigureAwait(false);
        return new DataSetToolResult { TreeId = treeId, Key = key };
    }

    /// <summary>Maps <see cref="ILatticeDataApi.DeleteAsync"/> onto the <c>data_delete</c> result.</summary>
    public static async Task<DataDeleteToolResult> DeleteAsync(
        ILatticeDataApi api,
        string treeId,
        string key,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(api);

        var deleted = await api.DeleteAsync(treeId, key, cancellationToken).ConfigureAwait(false);
        return new DataDeleteToolResult { TreeId = treeId, Key = key, Deleted = deleted };
    }

    /// <summary>Maps <see cref="ILatticeDataApi.SetManyAtomicAsync"/> onto the <c>data_set_many_atomic</c> result.</summary>
    public static async Task<DataAtomicBatchToolResult> SetManyAtomicAsync(
        ILatticeDataApi api,
        string treeId,
        IReadOnlyList<DataEntryDto>? upserts,
        IReadOnlyList<string>? deleteKeys,
        string operationId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(api);

        var batch = new DataAtomicBatch
        {
            Upserts = ToDataEntries(upserts),
            DeleteKeys = ToStringList(deleteKeys),
        };

        await api.SetManyAtomicAsync(treeId, batch, operationId, cancellationToken).ConfigureAwait(false);
        return new DataAtomicBatchToolResult { TreeId = treeId, OperationId = operationId };
    }

    /// <summary>Maps <see cref="ILatticeDataApi.SetManyAtomicCrossTreeAsync"/> onto the <c>data_set_many_atomic_cross_tree</c> result.</summary>
    public static async Task<DataCrossTreeBatchToolResult> SetManyAtomicCrossTreeAsync(
        ILatticeDataApi api,
        IReadOnlyList<DataTreeBatchDto> batches,
        string operationId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(api);
        ArgumentNullException.ThrowIfNull(batches);

        var treeBatches = new DataTreeBatch[batches.Count];
        for (var i = 0; i < batches.Count; i++)
        {
            var slice = batches[i];
            treeBatches[i] = new DataTreeBatch
            {
                TreeId = slice.TreeId,
                Upserts = ToDataEntries(slice.Upserts),
                DeleteKeys = ToStringList(slice.DeleteKeys),
            };
        }

        var outcome = await api.SetManyAtomicCrossTreeAsync(treeBatches, operationId, cancellationToken)
            .ConfigureAwait(false);

        return new DataCrossTreeBatchToolResult
        {
            OperationId = operationId,
            Outcome = outcome.ToString(),
            Committed = outcome == CrossTreeAtomicWriteOutcome.Committed,
        };
    }

    /// <summary>Maps <see cref="ILatticeDataApi.SetManyAsync"/> onto the <c>data_set_many</c> result.</summary>
    public static async Task<DataSetManyToolResult> SetManyAsync(
        ILatticeDataApi api,
        string treeId,
        IReadOnlyList<DataEntryDto>? upserts,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(api);

        var entries = ToDataEntries(upserts);
        await api.SetManyAsync(treeId, entries, cancellationToken).ConfigureAwait(false);
        return new DataSetManyToolResult { TreeId = treeId, Count = entries.Count };
    }

    private static List<DataEntry> ToDataEntries(IReadOnlyList<DataEntryDto>? upserts)
    {
        if (upserts is null || upserts.Count == 0)
        {
            return [];
        }

        var entries = new List<DataEntry>(upserts.Count);
        for (var i = 0; i < upserts.Count; i++)
        {
            entries.Add(new DataEntry { Key = upserts[i].Key, Value = upserts[i].Value });
        }

        return entries;
    }

    private static List<string> ToStringList(IReadOnlyList<string>? keys)
        => keys is null || keys.Count == 0 ? [] : [.. keys];
}
