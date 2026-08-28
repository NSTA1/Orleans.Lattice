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
    IOptions<LatticeApiDataOptions> apiOptions,
    ITenantContextResolver tenantResolver) : ILatticeDataApi
{
    private readonly IGrainFactory _grainFactory = grainFactory
        ?? throw new ArgumentNullException(nameof(grainFactory));

    private readonly LatticeApiDataOptions _apiOptions = (apiOptions
        ?? throw new ArgumentNullException(nameof(apiOptions))).Value;

    private readonly ITenantContextResolver _tenantResolver = tenantResolver
        ?? throw new ArgumentNullException(nameof(tenantResolver));

    /// <summary>
    /// Resolves the caller-supplied, tenant-local tree name to the effective tree
    /// id and dials that tree.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the seam that gives an external data-plane caller its own tenant
    /// namespace. Without it the facade dialled the caller's bare name verbatim, so
    /// every tenant asking for <c>orders</c> was handed the <em>same</em> physical
    /// tree: two tenants collided in one namespace, and a tenant could not address
    /// its own <c>t/{tenant}/orders</c> at all (the reserved-namespace guard
    /// correctly refuses a caller-supplied <c>t/</c> id, since composition is
    /// internal).
    /// </para>
    /// <para>
    /// Zero-cost when tenancy is off: the core no-op resolver resolves the reserved
    /// default tenant synchronously and returns the caller's bare name unchanged -
    /// the same string reference, no allocation, no await - so a non-tenancy
    /// cluster behaves byte-for-byte as before.
    /// </para>
    /// </remarks>
    private ValueTask<ILattice> TreeAsync(string treeId, CancellationToken cancellationToken)
    {
        // Guarded here so the rejection names the facade's own parameter, matching
        // the contract every sibling verb documents.
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        var pending = _tenantResolver.ResolveEffectiveTreeIdAsync(treeId, cancellationToken);

        // Warm path: the effective id resolved synchronously (the null resolver
        // always does), so the grain is dialled with no await and no allocation.
        if (pending.IsCompletedSuccessfully)
        {
            return new ValueTask<ILattice>(_grainFactory.GetGrain<ILattice>(pending.Result));
        }

        return AwaitTreeAsync(pending);
    }

    private async ValueTask<ILattice> AwaitTreeAsync(ValueTask<string> pending) =>
        _grainFactory.GetGrain<ILattice>(await pending.ConfigureAwait(false));

    /// <summary>
    /// Resolves the tree for a <em>read</em> that is documented to answer without
    /// side-effects on an unknown tree, returning <c>null</c> when the tree is not
    /// registered in the catalogue.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Routing an operation into a tree's shard root activates the shard-root and
    /// leaf grains, and the options resolver those grains go through lazily seeds a
    /// catalogue registration for any tree that has no structural pin - so a plain
    /// read of a name nobody ever created durably creates it, with a full default
    /// shard configuration. That turns every read verb into an unbounded catalogue
    /// growth vector, and lets a caller holding only read grants provision trees it
    /// is not permitted to create.
    /// </para>
    /// <para>
    /// Probing first costs one extra grain call on the miss path only and is the
    /// same shape <see cref="GetAsync"/> and <see cref="ReadRangeAsync"/> already
    /// use. The probe is read-gated, and reports a read denial as absence; for a
    /// read verb that is exactly right, because the caller receives the documented
    /// empty answer either way. It is therefore <em>only</em> valid for reads: a
    /// mutating verb resolved this way would turn an authorization denial into a
    /// no-op-shaped success, so the delete verbs answer from an ungated catalogue
    /// probe inside the core grain instead, sequenced after their own gate.
    /// </para>
    /// </remarks>
    private async ValueTask<ILattice?> ExistingTreeAsync(string treeId, CancellationToken cancellationToken)
    {
        var tree = await TreeAsync(treeId, cancellationToken).ConfigureAwait(false);
        return await tree.TreeExistsAsync(cancellationToken).ConfigureAwait(false) ? tree : null;
    }

    /// <inheritdoc />
    public async Task SetAsync(string treeId, string key, byte[] value, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        cancellationToken.ThrowIfCancellationRequested();

        var tree = await TreeAsync(treeId, cancellationToken).ConfigureAwait(false);
        await tree.SetAsync(key, value, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<bool> DeleteAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(key);
        cancellationToken.ThrowIfCancellationRequested();

        // Deleting from a tree that does not exist is a documented no-op and must
        // not materialise one. The short-circuit deliberately lives in the core
        // grain rather than here: it has to run *after* the delete gate so a caller
        // who is not entitled to delete still gets a denial rather than a
        // no-op-shaped answer. See LatticeGrain.DeleteAsyncCore.
        var tree = await TreeAsync(treeId, cancellationToken).ConfigureAwait(false);
        return await tree.DeleteAsync(key, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<DataRangeDeleteResult> DeleteRangeAsync(
        DataRangeDeleteRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrEmpty(request.TreeId);
        ArgumentNullException.ThrowIfNull(request.StartInclusive);
        ArgumentNullException.ThrowIfNull(request.EndExclusive);
        cancellationToken.ThrowIfCancellationRequested();

        var stepSize = Math.Max(1, _apiOptions.RangeDeleteStepSize);

        // As for the single-key delete: an unknown tree is a documented no-op whose
        // short-circuit lives behind the range-delete gate, in the cursor grain.
        var tree = await TreeAsync(request.TreeId, cancellationToken).ConfigureAwait(false);
        var deleted = await tree.DeleteRangeAsync(
            request.StartInclusive,
            request.EndExclusive,
            stepSize,
            maxAttempts: null,
            cancellationToken).ConfigureAwait(false);

        return new DataRangeDeleteResult
        {
            TreeId = request.TreeId,
            DeletedCount = deleted,
        };
    }

    /// <inheritdoc />
    public async Task SetManyAtomicAsync(
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

        var tree = await TreeAsync(treeId, cancellationToken).ConfigureAwait(false);
        await tree.SetManyAtomicAsync(upserts, deletes, operationId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<CrossTreeAtomicWriteOutcome> SetManyAtomicCrossTreeAsync(
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

            // Every slice's tree name is composed under the caller's tenant too, so
            // a cross-tree atomic batch cannot straddle namespaces by naming an
            // unqualified tree that belongs to someone else.
            var effectiveTreeId = await _tenantResolver
                .ResolveEffectiveTreeIdAsync(batch.TreeId, cancellationToken).ConfigureAwait(false);

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

                treeBatches.Add(new LatticeTreeBatch(effectiveTreeId, upserts, EntryDeletes: flags));
            }
            else
            {
                treeBatches.Add(new LatticeTreeBatch(effectiveTreeId, upserts));
            }
        }

        return await _grainFactory
            .SetManyAtomicAsync(treeBatches, operationId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task SetManyAsync(
        string treeId,
        IReadOnlyList<DataEntry> upserts,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(upserts);
        cancellationToken.ThrowIfCancellationRequested();

        var pairs = ToKeyValuePairs(upserts is List<DataEntry> list ? list : [.. upserts]);

        var tree = await TreeAsync(treeId, cancellationToken).ConfigureAwait(false);
        await tree.SetManyAsync(pairs, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<DataReadResult> GetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(key);
        cancellationToken.ThrowIfCancellationRequested();

        var tree = await TreeAsync(treeId, cancellationToken).ConfigureAwait(false);

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

        var tree = await TreeAsync(request.TreeId, cancellationToken).ConfigureAwait(false);
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
