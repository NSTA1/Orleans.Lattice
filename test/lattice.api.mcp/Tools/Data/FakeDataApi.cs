using Orleans.Lattice.Api.Data;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// A deterministic in-memory <see cref="ILatticeDataApi"/> test double. Backs a
/// per-(tree, key) store and models the facade's fail-closed contract via a
/// denied-key set: a denied key throws
/// <see cref="LatticeAuthorizationDeniedException"/> on any write and reports
/// absent on a read, exactly as the gated facade behaves for an unauthorized
/// caller. No timing, ordering, or transport behaviour is modelled.
/// </summary>
internal sealed partial class FakeDataApi : ILatticeDataApi
{
    private readonly Dictionary<(string Tree, string Key), byte[]> _store = new();

    /// <summary>Keys (per tree) that are denied to the caller - writes throw, reads report absent.</summary>
    public HashSet<(string Tree, string Key)> Denied { get; } = new();

    /// <summary>The outcome the cross-tree atomic write returns when every leg is authorized.</summary>
    public CrossTreeAtomicWriteOutcome CrossTreeOutcome { get; set; } = CrossTreeAtomicWriteOutcome.Committed;

    /// <summary>Number of live entries currently stored across all trees.</summary>
    public int Count => _store.Count;

    /// <summary>Returns whether a live value is stored at the given tree / key.</summary>
    public bool Contains(string treeId, string key) => _store.ContainsKey((treeId, key));

    public Task SetAsync(string treeId, string key, byte[] value, CancellationToken cancellationToken = default)
    {
        ThrowIfDenied(treeId, key);
        _store[(treeId, key)] = value;
        return Task.CompletedTask;
    }

    public Task<bool> DeleteAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ThrowIfDenied(treeId, key);
        return Task.FromResult(_store.Remove((treeId, key)));
    }

    public Task SetManyAtomicAsync(
        string treeId,
        DataAtomicBatch batch,
        string operationId,
        CancellationToken cancellationToken = default)
    {
        // Authorize every leg before any apply, so a single denied leg aborts the
        // whole batch with nothing persisted.
        foreach (var upsert in batch.Upserts)
        {
            ThrowIfDenied(treeId, upsert.Key);
        }

        foreach (var deleteKey in batch.DeleteKeys)
        {
            ThrowIfDenied(treeId, deleteKey);
        }

        foreach (var upsert in batch.Upserts)
        {
            _store[(treeId, upsert.Key)] = upsert.Value;
        }

        foreach (var deleteKey in batch.DeleteKeys)
        {
            _store.Remove((treeId, deleteKey));
        }

        return Task.CompletedTask;
    }

    public Task<CrossTreeAtomicWriteOutcome> SetManyAtomicCrossTreeAsync(
        IReadOnlyList<DataTreeBatch> batches,
        string operationId,
        CancellationToken cancellationToken = default)
    {
        foreach (var slice in batches)
        {
            foreach (var upsert in slice.Upserts)
            {
                ThrowIfDenied(slice.TreeId, upsert.Key);
            }

            foreach (var deleteKey in slice.DeleteKeys)
            {
                ThrowIfDenied(slice.TreeId, deleteKey);
            }
        }

        if (CrossTreeOutcome != CrossTreeAtomicWriteOutcome.Committed)
        {
            // A precondition miss commits nothing.
            return Task.FromResult(CrossTreeOutcome);
        }

        foreach (var slice in batches)
        {
            foreach (var upsert in slice.Upserts)
            {
                _store[(slice.TreeId, upsert.Key)] = upsert.Value;
            }

            foreach (var deleteKey in slice.DeleteKeys)
            {
                _store.Remove((slice.TreeId, deleteKey));
            }
        }

        return Task.FromResult(CrossTreeAtomicWriteOutcome.Committed);
    }

    public Task SetManyAsync(
        string treeId,
        IReadOnlyList<DataEntry> upserts,
        CancellationToken cancellationToken = default)
    {
        // Authorize every leg before any apply, mirroring the gated facade.
        foreach (var upsert in upserts)
        {
            ThrowIfDenied(treeId, upsert.Key);
        }

        foreach (var upsert in upserts)
        {
            _store[(treeId, upsert.Key)] = upsert.Value;
        }

        return Task.CompletedTask;
    }

    public Task<DataReadResult> GetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        if (Denied.Contains((treeId, key)) || !_store.TryGetValue((treeId, key), out var value))
        {
            return Task.FromResult(new DataReadResult { TreeId = treeId, Key = key, Found = false });
        }

        return Task.FromResult(new DataReadResult { TreeId = treeId, Key = key, Found = true, Value = value });
    }

    public Task<DataRangePage> ReadRangeAsync(DataRangeRequest request, CancellationToken cancellationToken = default)
    {
        var entries = _store
            .Where(kv => kv.Key.Tree == request.TreeId && !Denied.Contains(kv.Key))
            .Where(kv => request.StartInclusive is null || string.CompareOrdinal(kv.Key.Key, request.StartInclusive) >= 0)
            .Where(kv => request.EndExclusive is null || string.CompareOrdinal(kv.Key.Key, request.EndExclusive) < 0)
            .OrderBy(kv => kv.Key.Key, StringComparer.Ordinal)
            .Select(kv => new DataEntry { Key = kv.Key.Key, Value = kv.Value })
            .ToArray();

        return Task.FromResult(new DataRangePage { TreeId = request.TreeId, Entries = entries });
    }

    private void ThrowIfDenied(string treeId, string key)
    {
        if (Denied.Contains((treeId, key)))
        {
            throw new LatticeAuthorizationDeniedException(
                $"The caller may not write '{key}' on tree '{treeId}'.");
        }
    }
}
