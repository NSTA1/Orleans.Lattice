using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Data;

/// <summary>
/// Default <see cref="ILatticeDataApi"/> implementation. Registered as a silo
/// singleton by <c>AddLatticeDataApi</c>; it dials the core
/// <see cref="ILattice"/> grain surface via the cluster grain factory. Each
/// operation calls the same public grain method the in-cluster client uses, so
/// the authorization enforcement wired at the cluster grain fires automatically
/// once the caller identity flows on the ambient credential context.
/// </summary>
internal sealed partial class LatticeDataApi(
    IGrainFactory grainFactory,
    IOptions<LatticeApiDataOptions> apiOptions) : ILatticeDataApi
{
    private readonly IGrainFactory _grainFactory = grainFactory
        ?? throw new ArgumentNullException(nameof(grainFactory));

    private readonly LatticeApiDataOptions _apiOptions = (apiOptions
        ?? throw new ArgumentNullException(nameof(apiOptions))).Value;

    /// <inheritdoc />
    public Task SetAsync(string treeId, string key, byte[] value, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        cancellationToken.ThrowIfCancellationRequested();

        var tree = _grainFactory.GetGrain<ILattice>(treeId);
        return tree.SetAsync(key, value, cancellationToken);
    }

    /// <inheritdoc />
    public Task<bool> DeleteAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(key);
        cancellationToken.ThrowIfCancellationRequested();

        var tree = _grainFactory.GetGrain<ILattice>(treeId);
        return tree.DeleteAsync(key, cancellationToken);
    }

    /// <inheritdoc />
    public Task SetManyAtomicAsync(
        string treeId,
        DataAtomicBatch batch,
        string operationId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(batch);
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        cancellationToken.ThrowIfCancellationRequested();

        var upserts = ToKeyValuePairs(batch.Upserts);
        var deletes = batch.DeleteKeys is { Count: > 0 }
            ? (IReadOnlyList<string>)batch.DeleteKeys
            : Array.Empty<string>();

        var tree = _grainFactory.GetGrain<ILattice>(treeId);
        return tree.SetManyAtomicAsync(upserts, deletes, operationId, cancellationToken);
    }

    /// <inheritdoc />
    public Task<CrossTreeAtomicWriteOutcome> SetManyAtomicCrossTreeAsync(
        IReadOnlyList<DataTreeBatch> batches,
        string operationId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(batches);
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        cancellationToken.ThrowIfCancellationRequested();

        var treeBatches = new List<LatticeTreeBatch>(batches.Count);
        foreach (var batch in batches)
        {
            ArgumentNullException.ThrowIfNull(batch);
            var upserts = ToKeyValuePairs(batch.Upserts);
            var deletes = batch.DeleteKeys is { Count: > 0 } ? [.. batch.DeleteKeys] : (List<string>?)null;

            // A cross-tree slice carrying deletes rides them 1:1 alongside the
            // upserts: append delete keys (with an empty value buffer) and a
            // parallel delete flag so the coordinator retracts them atomically.
            if (deletes is { Count: > 0 })
            {
                var flags = new List<bool>(upserts.Count + deletes.Count);
                for (var i = 0; i < upserts.Count; i++)
                {
                    flags.Add(false);
                }

                foreach (var deleteKey in deletes)
                {
                    upserts.Add(new KeyValuePair<string, byte[]>(deleteKey, Array.Empty<byte>()));
                    flags.Add(true);
                }

                treeBatches.Add(new LatticeTreeBatch(batch.TreeId, upserts, EntryDeletes: flags));
            }
            else
            {
                treeBatches.Add(new LatticeTreeBatch(batch.TreeId, upserts));
            }
        }

        return _grainFactory.SetManyAtomicAsync(treeBatches, operationId, cancellationToken);
    }

    /// <inheritdoc />
    public Task SetManyAsync(
        string treeId,
        IReadOnlyList<DataEntry> upserts,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(upserts);
        cancellationToken.ThrowIfCancellationRequested();

        var pairs = ToKeyValuePairs(upserts is List<DataEntry> list ? list : [.. upserts]);

        var tree = _grainFactory.GetGrain<ILattice>(treeId);
        return tree.SetManyAsync(pairs, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<DataReadResult> GetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(key);
        cancellationToken.ThrowIfCancellationRequested();

        var tree = _grainFactory.GetGrain<ILattice>(treeId);

        // A read must never materialise a tree: probing an unknown tree reports a
        // clean miss rather than routing into the shard root (which would register
        // the tree and seed its shard roots as a write side-effect of a read).
        if (!await tree.TreeExistsAsync(cancellationToken).ConfigureAwait(false))
        {
            return new DataReadResult
            {
                TreeId = treeId,
                Key = key,
                Found = false,
                Value = Array.Empty<byte>(),
            };
        }

        // A versioned read (single shard RPC, same cost as the plain get) so the
        // result can report the entry's per-key merge mode and flag the payload
        // raw - a typed CRDT's value bytes are its internal serialization, never a
        // decoded logical projection on the data plane.
        var versioned = await tree.GetWithVersionAsync(key, cancellationToken).ConfigureAwait(false);
        var found = versioned.Value is not null;

        return new DataReadResult
        {
            TreeId = treeId,
            Key = key,
            Found = found,
            Value = versioned.Value ?? Array.Empty<byte>(),
            MergeMode = found ? versioned.MergeMode : null,
            Raw = found,
        };
    }

    /// <inheritdoc />
    public async Task<DataRangePage> ReadRangeAsync(DataRangeRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrEmpty(request.TreeId);
        cancellationToken.ThrowIfCancellationRequested();

        var tree = _grainFactory.GetGrain<ILattice>(request.TreeId);
        var fresh = string.IsNullOrEmpty(request.ContinuationToken);

        string cursorId;
        if (fresh)
        {
            // An unknown tree yields an empty, fully-drained page rather than a
            // fault, so a client can probe a range without a prior existence check.
            if (!await tree.TreeExistsAsync(cancellationToken).ConfigureAwait(false))
            {
                return new DataRangePage
                {
                    TreeId = request.TreeId,
                    Entries = Array.Empty<DataEntry>(),
                    ContinuationToken = null,
                };
            }

            cursorId = await tree.OpenEntryCursorAsync(
                request.StartInclusive,
                request.EndExclusive,
                reverse: false,
                pointInTime: false,
                cancellationToken).ConfigureAwait(false);
        }
        else
        {
            cursorId = request.ContinuationToken!;
        }

        var pageSize = ClampPageSize(request.PageSize);

        LatticeCursorEntriesPage page;
        try
        {
            page = await tree.NextEntriesAsync(cursorId, pageSize, cancellationToken).ConfigureAwait(false);
        }
        catch (InvalidOperationException ex) when (!fresh)
        {
            // A client-supplied continuation token that names an unknown, drained,
            // or already-closed cursor is a malformed request, not a server fault.
            throw new ArgumentException(
                $"The continuation token '{request.ContinuationToken}' is invalid or has expired.",
                nameof(request),
                ex);
        }

        var entries = new List<DataEntry>(page.Entries.Count);
        foreach (var entry in page.Entries)
        {
            cancellationToken.ThrowIfCancellationRequested();
            // The bulk range read returns raw stored bytes verbatim; flag them so a
            // consumer never mistakes a typed CRDT's internal serialization for a
            // decoded value. Per-key mode is not resolved here (the cursor carries
            // none) - a self-describing per-key read is the point read or scan_entries.
            entries.Add(new DataEntry { Key = entry.Key, Value = entry.Value, Raw = true });
        }

        string? continuation = page.HasMore ? cursorId : null;
        if (!page.HasMore)
        {
            // Drained: release the server-side cursor promptly.
            await tree.CloseCursorAsync(cursorId, cancellationToken).ConfigureAwait(false);
        }

        return new DataRangePage
        {
            TreeId = request.TreeId,
            Entries = entries,
            ContinuationToken = continuation,
        };
    }

    private static List<KeyValuePair<string, byte[]>> ToKeyValuePairs(List<DataEntry>? upserts)
    {
        if (upserts is not { Count: > 0 })
        {
            return [];
        }

        var pairs = new List<KeyValuePair<string, byte[]>>(upserts.Count);
        foreach (var entry in upserts)
        {
            ArgumentNullException.ThrowIfNull(entry);
            pairs.Add(new KeyValuePair<string, byte[]>(entry.Key, entry.Value));
        }

        return pairs;
    }

    private int ClampPageSize(int requested)
    {
        var size = requested > 0 ? requested : _apiOptions.DefaultRangePageSize;
        return Math.Min(size, _apiOptions.MaxRangePageSize);
    }
}
