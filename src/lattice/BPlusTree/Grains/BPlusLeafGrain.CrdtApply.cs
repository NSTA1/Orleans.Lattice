using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Producer-side typed CRDT delta-apply path for the leaf grain. The
/// public lattice surface routes per-key delta applies through this
/// seam so accessors (OrSet, PnCounter, etc.) can collapse the
/// historical read-merge-write round trip into a single grain call.
/// <para>
/// Step-3 scope: the new path appends a CRDT-flavoured WAL record (the
/// receiver's existing apply path already handles
/// <see cref="WalRecord.Delta"/> on every CRDT mode) and re-serialises
/// the post-merge typed state back into the legacy byte[] row so the
/// observable read seam (<see cref="BPlusLeafGrain.GetAsync"/>) keeps
/// returning the canonical state without re-tooling. Later steps swap
/// the byte[] row for an in-grain typed-state cache plus WAL-replay
/// rebuild; this method is the seam those steps extend.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// Process-wide fallback registry used when the grain activation
    /// has no DI-registered <see cref="CrdtShapeRegistry"/> (unit tests
    /// that construct the grain directly via the
    /// <see cref="IGrainContext"/> substitute). Hosts always resolve
    /// the DI-registered instance because
    /// <see cref="LatticeServiceCollectionExtensions.AddLattice"/>
    /// registers it unconditionally; this fallback only covers the
    /// closed-shape modes (OrSet, PnCounter, VersionVector,
    /// MvRegister) because OR-Map requires per-tree generic
    /// registration that has no sensible default.
    /// </summary>
    private static readonly CrdtShapeRegistry FallbackCrdtShapeRegistry = new();

    private CrdtShapeRegistry? _resolvedCrdtShapeRegistry;

    private CrdtShapeRegistry ResolveCrdtShapeRegistry() =>
        _resolvedCrdtShapeRegistry ??=
            context.ActivationServices.GetService<CrdtShapeRegistry>()
            ?? FallbackCrdtShapeRegistry;

    /// <inheritdoc />
    public async Task<CrdtApplyResult> ApplyCrdtDeltaAsync(string key, LatticeMergeMode mode, byte[] deltaBytes)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(deltaBytes);
        if (mode == LatticeMergeMode.LwwRegister)
        {
            throw new ArgumentException(
                "ApplyCrdtDeltaAsync does not accept LatticeMergeMode.LwwRegister; "
                + "use SetAsync or SetIfVersionAsync for LWW writes.",
                nameof(mode));
        }

        var registry = ResolveCrdtShapeRegistry();
        var shape = registry.TryGet(state.State.TreeId ?? string.Empty, mode);
        if (shape is null)
        {
            throw new InvalidOperationException(
                "No CrdtShape is registered for tree '"
                + (state.State.TreeId ?? string.Empty)
                + "' at mode '"
                + mode
                + "'. Register a shape via ISiloBuilder.AddOrMapShape<TKey, TValue>(treeName) "
                + "for OR-Map trees; closed-shape modes resolve through the global registry "
                + "fallback automatically.");
        }

        // step 1 (merge) - decode current state (or empty), fold the
        // typed delta, re-serialise the post-merge state. The
        // re-serialisation keeps the legacy byte[] row consistent so
        // GetAsync continues to return the canonical post-merge bytes.
        // The typed shadow short-circuits the existing-state decode on
        // the consecutive-mutations-to-the-same-key hot path: when a
        // post-merge typed instance is already in the cache (stored by
        // the previous ApplyCrdtDeltaAsync commit under the matching
        // shape's state type), re-use it instead of paying the
        // DeserializeState pass.
        var typedDelta = shape.DeserializeDelta(deltaBytes);
        var hasExistingRow = Cache.TryGetRow(key, out var existing)
            && !existing.IsTombstone
            && existing.Value is { Length: > 0 };
        object typedState;
        if (hasExistingRow && Cache.TryGetTyped<object>(key, out var shadowed))
        {
            typedState = shadowed;
        }
        else if (hasExistingRow)
        {
            typedState = shape.DeserializeState(existing.Value!);
        }
        else
        {
            typedState = shape.CreateEmpty();
        }
        shape.MergeDelta(typedState, typedDelta);
        var postMergeBytes = shape.SerializeState(typedState);

        // step 2 (stamp) - advance the leaf HLC and publish the
        // Version[ReplicaId] advance for the cache filter, same shape
        // as CommitSetAsync.
        var stamp = AdvanceClockOrOverride();
        PublishVersionAdvance(stamp);
        BumpLocalRevision();
        var postMergeEntry = LwwValue<byte[]>.Create(postMergeBytes, stamp) with
        {
            OriginClusterId = LatticeOriginContext.Current,
            VectorClock = LatticeVectorClockContext.Current,
        };

        // step 3 (wal) - append a CRDT-flavoured Set whose Delta slot
        // carries the producer's typed delta bytes verbatim. The
        // canonical encoder strips Value on encode for CRDT modes so
        // the wire stays delta-only; the in-grain instance retains
        // both the typed delta and the post-merge state-row payload.
        var writer = ResolveCommitLogWriter();
        if (writer is not null)
        {
            var record = WalRecordBuilder.ForCrdtDelta(
                state.State.TreeId ?? string.Empty,
                state.State.ShardIndex ?? 0,
                key,
                mode,
                postMergeEntry,
                deltaBytes);
            await writer.AppendAsync(record);
        }

        // step 4 (apply) - merge the post-merge state into the leaf
        // projection through the standard StoreEntry funnel so the
        // projection-digest XOR and per-key delivery sequence advance
        // exactly as they would for an LWW Set. StoreEntry's byte
        // write evicts any prior typed shadow for the key, so we
        // re-store the freshly merged typed instance immediately
        // afterwards to keep the shadow consistent with the row.
        var options = await GetOptionsAsync();
        StoreEntry(key, postMergeEntry);
        Cache.StoreTyped(key, typedState);
        SplitResult? splitResult = null;
        if (Cache.Count > options.MaxLeafKeys)
        {
            splitResult = await SplitAsync();
        }

        // step 5 (observer) - publish under a commit-log scope so a
        // replication-aware observer detects the source and avoids
        // re-appending its own input back into the WAL. Also wraps the
        // observer publish in a LatticeDeltaContext.With(deltaBytes)
        // scope so mutation observers see LatticeMutation.Delta
        // populated with the typed delta payload (same contract the
        // legacy accessor's CAS-loop path provided via its outer
        // LatticeDeltaContext scope).
        if (mutationObservers.HasObservers)
        {
            var published = Cache.TryGetRow(key, out var committed) ? committed : postMergeEntry;
            using (LatticeCommitLogContext.BeginScope())
            using (LatticeDeltaContext.With(deltaBytes))
            {
                await PublishSetAsync(key, published);
            }
        }

        // step 6 (digest) - propagate the projection-hash delta upward
        // so the chained subtree fold stays current.
        await PublishDigestUpwardAsync();

        return new CrdtApplyResult { Version = stamp, Split = splitResult };
    }
}
