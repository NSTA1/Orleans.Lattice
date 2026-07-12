using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Producer-side typed CRDT delta-apply path for the leaf grain. The
/// public lattice surface routes per-key delta applies through this
/// seam so accessors (OrSet, PnCounter, etc.) can collapse the
/// historical read-merge-write round trip into a single grain call.
/// <para>
/// The apply folds the typed delta into the post-merge state and keeps
/// the observable read seam (<see cref="BPlusLeafGrain.GetAsync"/>)
/// returning the canonical post-merge bytes. It picks one of two paths
/// per apply:
/// </para>
/// <para>
/// <b>Eager path</b> (no streaming serialiser exists for the shape, or
/// an existing row's stamp dominates): serialise the post-merge state
/// once and re-use that array for the byte[] row and the
/// projection-digest fold, exactly as the historical path did. The
/// durable WAL record itself never carries the post-merge bytes - it is
/// delta-only (the encoder strips <see cref="WalRecord.Value"/> for this
/// record shape) - so the eager serialisation is load-bearing only for
/// the in-memory row and digest, not for the log.
/// </para>
/// <para>
/// <b>Deferred path</b> (a streaming serialiser is available and the new
/// stamp wins): the post-merge byte[] row is <b>not</b> materialised,
/// <i>even when a commit-log writer is present</i>. The typed shadow
/// (<see cref="LeafEntryCache.StoreTyped"/>) is authoritative for the
/// key; the projection-digest fold is fed from a reused streaming buffer
/// so the per-apply allocation is O(delta) instead of O(state); the
/// delta-only WAL record is still appended (it never needed the
/// post-merge bytes); and the canonical row bytes are produced lazily by
/// serialising the shadow at the first consumer (a GetAsync read,
/// snapshot/persist capture, split/projection, replication ship, observer
/// publish, or any row enumeration), all of which funnel through
/// <see cref="LeafEntryCache.TryGetRow"/> /
/// <see cref="LeafEntryCache.EnumerateRows"/> /
/// <see cref="LeafEntryCache.UnderlyingRows"/>. The streaming serialiser
/// and the lazy array serialiser are byte-identical, so a materialised
/// read matches the digest contribution already folded in. Because the
/// WAL record is delta-only, a cold-rebuild replay reconstructs the
/// post-fold state by folding the WAL delta into the prior visible state
/// (see <c>ApplySet</c>), so the deferred producer path leaves no state
/// row that replay needs to read back.
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

    /// <summary>
    /// Per-activation reusable buffer that the deferred CRDT-apply path streams
    /// the post-merge (and, on the steady-state hot path, the pre-merge) state
    /// serialisation into so the projection-digest fold consumes the bytes
    /// without allocating an O(state) <c>byte[]</c> row every apply. Reset via
    /// <see cref="System.Buffers.ArrayBufferWriter{T}.ResetWrittenCount"/>
    /// (which retains the backing array) between uses, so after warm-up the
    /// per-apply allocation stays flat in the post-merge state size.
    /// </summary>
    private System.Buffers.ArrayBufferWriter<byte>? _crdtSerializeBuffer;

    private CrdtShapeRegistry ResolveCrdtShapeRegistry() =>
        _resolvedCrdtShapeRegistry ??=
            context.ActivationServices.GetService<CrdtShapeRegistry>()
            ?? FallbackCrdtShapeRegistry;

    private ILatticeMergeModeResolver? _resolvedMergeModeResolver;
    private bool _mergeModeResolverResolved;

    /// <summary>
    /// Resolves the declared <see cref="LatticeMergeMode"/> for this leaf's
    /// tree via the DI-registered <see cref="ILatticeMergeModeResolver"/>,
    /// falling back to <see cref="LatticeMergeMode.LwwRegister"/> when no
    /// resolver is registered (single-cluster hosts) or the tree is not
    /// replicated. Used by the prepared-commit path to record a CRDT-mode
    /// prepared write's merge mode in the pending-tx delta side-map so the
    /// terminal drain can fold its typed delta. The same resolver instance
    /// stamps <see cref="WalRecord.Mode"/> at WAL-write time, so the
    /// foreground commit and the activation-time replay derive an identical
    /// mode and reconstruct the side-map deterministically.
    /// </summary>
    private LatticeMergeMode ResolveMergeMode()
    {
        if (!_mergeModeResolverResolved)
        {
            _resolvedMergeModeResolver =
                context.ActivationServices.GetService<ILatticeMergeModeResolver>();
            _mergeModeResolverResolved = true;
        }
        return _resolvedMergeModeResolver?.Resolve(state.State.TreeId ?? string.Empty)
            ?? LatticeMergeMode.LwwRegister;
    }

    /// <inheritdoc />
    public async Task<CrdtApplyResult> ApplyCrdtDeltaAsync(string key, LatticeMergeMode mode, byte[] deltaBytes)
    {
        EnsureInternalOrigin(LatticeOperation.CrdtApply);
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

        // step 1 (merge) - resolve the current typed state (shadow, cold
        // decode, or empty), then fold the typed delta. The typed shadow
        // short-circuits the existing-state decode on the consecutive-
        // mutations-to-the-same-key hot path: when a post-merge typed
        // instance is already in the cache (stored by the previous
        // ApplyCrdtDeltaAsync commit under the matching shape's state
        // type), re-use it instead of paying the DeserializeState pass.
        // The actual MergeDelta fold happens inside the path branch below
        // because the deferred path must first re-stream the pre-merge
        // state to fold the prior digest contribution out.
        //
        // Determinism: the durable delta may carry a self-describing version
        // envelope (the ingest/apply boundary stamps and, in strict-ingest,
        // upcasts it once). We strip that envelope here with a version-agnostic
        // strip to recover the raw typed-CRDT body the shape deserialises, but we
        // persist the *enveloped* deltaBytes verbatim in the WAL below. Every later
        // fold - a fresh apply, a cold WAL replay, or a snapshot-restore projection
        // fold - strips the same durable bytes to the same body, so the fold is
        // byte-identical across apply time and replay time and never upcasts at
        // fold time. Identity (deltaBytes itself) when no versioning is active.
        var foldDelta = StripDeltaForFold(deltaBytes);
        var typedDelta = shape.DeserializeDelta(foldDelta);
        var hasExistingRow = Cache.TryPeekRow(key, out var existing, out var existingDeferred)
            && !existing.IsTombstone
            && (existingDeferred || existing.Value is { Length: > 0 });
        object typedState;
        if (Cache.TryGetTyped<object>(key, out var shadowed))
        {
            // A live typed shadow is authoritative whenever present (and it is
            // always present for a deferred row, whose canonical bytes are
            // reproduced from it). Re-use it without touching the byte row.
            typedState = shadowed;
        }
        else if (hasExistingRow && !existingDeferred)
        {
            typedState = shape.DeserializeState(existing.Value!);
        }
        else if (hasExistingRow)
        {
            // Defensive: a deferred row with no live shadow should not occur
            // (defer always re-stores the shadow, and every shadow eviction
            // also clears the deferred marker), but if it does, materialise
            // the row and decode it so correctness never depends on the
            // invariant holding.
            Cache.TryGetRow(key, out existing);
            typedState = shape.DeserializeState(existing.Value!);
        }
        else
        {
            typedState = shape.CreateEmpty();
        }

        // step 2 (stamp) - advance the leaf HLC and publish the
        // Version[ReplicaId] advance for the cache filter, same shape
        // as CommitSetAsync.
        var stamp = AdvanceClockOrOverride();
        PublishVersionAdvance(stamp);
        BumpLocalRevision();

        // step 3 (path select) - the post-merge full-state re-serialisation is
        // only load-bearing for code that consumes the canonical byte[] row:
        // a GetAsync read, snapshot/persist capture, split/projection,
        // replication digest, and shadow eviction. The durable commit-log
        // record is delta-only (the encoder strips Value for this record
        // shape), so it never needs the post-merge bytes. When a streaming
        // serialiser is available and the new stamp dominates any existing row
        // (so the row-level LWW keeps the new value), defer the O(state) row
        // materialisation even when a commit-log writer is wired: feed the
        // digest fold from a reused streaming buffer, append the delta-only WAL
        // record, and store a placeholder row whose canonical bytes are
        // produced lazily at the first read / enumerate / snapshot seam. The
        // typed shadow is authoritative for the key until then; a cold-rebuild
        // replay reconstructs the post-fold state by folding the WAL delta
        // (see ApplySet), so the deferred path leaves no state row replay needs.
        // Otherwise take the eager path, which serialises once and re-uses that
        // array for the in-memory row and the digest (the WAL stays delta-only
        // on both paths).
        var writer = ResolveCommitLogWriter();
        var canDefer = shape.SerializeStateInto is not null
            && (!hasExistingRow || existing.Timestamp.CompareTo(stamp) < 0);

        LwwValue<byte[]> postMergeEntry;
        if (!canDefer)
        {
            shape.MergeDelta(typedState, typedDelta);
            var postMergeBytes = shape.SerializeState(typedState);
            postMergeEntry = LwwValue<byte[]>.Create(postMergeBytes, stamp) with
            {
                OriginClusterId = LatticeOriginContext.Current,
                VectorClock = LatticeVectorClockContext.Current,
            };

            // step 4a (wal) - append a CRDT-flavoured Set whose Delta slot
            // carries the producer's typed delta bytes verbatim. The record is
            // delta-only: WalRecordBuilder.ForCrdtDelta leaves Value null and
            // the canonical encoder strips it on encode for CRDT modes, so the
            // wire and the in-memory record both stay delta-only. The eager
            // post-merge serialisation below feeds only the in-grain byte[] row
            // and the projection digest, not the log.
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

            // step 5a (apply) - merge the post-merge state into the leaf
            // projection through the standard StoreEntry funnel so the
            // projection-digest XOR and per-key delivery sequence advance
            // exactly as they would for an LWW Set. StoreEntry's byte write
            // evicts any prior typed shadow for the key, so we re-store the
            // freshly merged typed instance immediately afterwards to keep
            // the shadow consistent with the row.
            StoreEntry(key, postMergeEntry);
            Cache.StoreTyped(key, typedState);
        }
        else
        {
            // Deferred path. postMergeEntry carries the canonical metadata with
            // a null Value placeholder; the bytes are reproduced on demand from
            // the typed shadow.
            postMergeEntry = new LwwValue<byte[]>
            {
                Value = null,
                Timestamp = stamp,
                IsTombstone = false,
                OriginClusterId = LatticeOriginContext.Current,
                VectorClock = LatticeVectorClockContext.Current,
            };

            // step 4b (wal) - append the same delta-only CRDT Set the eager
            // path appends. The post-merge state is never serialised for this
            // record: WalRecordBuilder.ForCrdtDelta leaves Value null and the
            // canonical encoder strips it regardless, so the record carries the
            // typed delta bytes verbatim. A cold-rebuild replay folds those
            // bytes back into the prior visible state (see ApplySet), so the
            // null row placeholder this path stores is never read by replay.
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

            var buffer = _crdtSerializeBuffer ??= new System.Buffers.ArrayBufferWriter<byte>();
            var serializeInto = shape.SerializeStateInto!;

            if (_maintainProjectionDigest)
            {
                // Fold the prior contribution out before mutating the shadow:
                // the pre-merge serialised bytes are exactly what the previous
                // apply folded in for this key.
                EnsureProjectionHashInitialized();
                Span<byte> oldContribution = stackalloc byte[ProjectionHashSize];
                var hasOld = false;
                if (hasExistingRow)
                {
                    if (!existingDeferred && existing.Value is { Length: > 0 })
                    {
                        ComputeEntryContribution(key, in existing, existing.Value, hasValue: true, oldContribution);
                    }
                    else
                    {
                        // typedState is still pre-merge here; re-stream it.
                        buffer.ResetWrittenCount();
                        serializeInto(typedState, buffer);
                        ComputeEntryContribution(key, in existing, buffer.WrittenSpan, hasValue: true, oldContribution);
                    }
                    hasOld = true;
                }

                shape.MergeDelta(typedState, typedDelta);

                buffer.ResetWrittenCount();
                serializeInto(typedState, buffer);
                var serializedLength = buffer.WrittenSpan.Length;
                Span<byte> newContribution = stackalloc byte[ProjectionHashSize];
                ComputeEntryContribution(key, in postMergeEntry, buffer.WrittenSpan, hasValue: true, newContribution);
                XorFoldContributionDelta(oldContribution, hasOld, newContribution, hasNew: true);

                StoreDeferredCrdtRow(key, in postMergeEntry, shape, typedState, serializedLength);
            }
            else
            {
                // Digest maintenance disabled: no fold, but still record the
                // serialised length for byte-accurate StateBytes accounting.
                shape.MergeDelta(typedState, typedDelta);
                buffer.ResetWrittenCount();
                serializeInto(typedState, buffer);
                StoreDeferredCrdtRow(key, in postMergeEntry, shape, typedState, buffer.WrittenSpan.Length);
            }

            // Mirror StoreEntry's per-key delivery-sequence advance so the
            // delivery cursor and replication ship path observe the apply.
            BumpDeliverySequenceFor(key);
            Cache.StoreTyped(key, typedState);
        }

        // Record the per-key merge mode so a snapshot capture of this leaf's
        // committed cache labels the key with its true CRDT mode rather than the
        // coarse declared tree mode. Set after both StoreEntry/StoreTyped paths
        // because StoreEntry's byte-row write evicts any prior recorded mode.
        Cache.SetMergeMode(key, mode);

        // step 4c (post-merge observer) - consult the registered merge observer
        // with the decoded inputs / result and the record's declared CRDT mode.
        // For a CRDT record AcceptTransformed is rejected (throws): mutating
        // canonical merged bytes would break WAL-replay determinism, since a
        // cold rebuild folds the durable delta into the prior visible state and
        // must reconstruct identical bytes. Accept / AcceptWithEvent are
        // non-mutating, so the return value is not stored here. Zero-cost when
        // inactive (cached flag): no observer call, and the deferred row is not
        // materialised on the default null-observer path. Cache.TryGetRow
        // materialises a deferred row on demand so the observer sees canonical
        // merged bytes.
        if (MergeObserverActive)
        {
            var mergedBytes = Cache.TryGetRow(key, out var mergedRow) && mergedRow.Value is not null
                ? mergedRow.Value
                : postMergeEntry.Value ?? Array.Empty<byte>();
            byte[]? localInput = hasExistingRow && !existingDeferred ? existing.Value : null;
            await ApplyMergeObserverAsync(
                key, mode, localInput, null, mergedBytes, CancellationToken.None,
                incomingDeltaForVersion: deltaBytes);
        }

        var options = await GetOptionsAsync();
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
        // LatticeDeltaContext scope). Cache.TryGetRow materialises a
        // deferred row on demand so observers always see canonical bytes.
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

    /// <summary>
    /// Stores a deferred CRDT post-merge row whose canonical bytes are
    /// reproduced lazily by serialising the captured typed shadow. The
    /// materialiser uses <see cref="CrdtShape.SerializeState"/> (the array
    /// serialiser), which is byte-identical to the streaming
    /// <see cref="CrdtShape.SerializeStateInto"/> used to feed the digest fold,
    /// so a later materialised read yields bytes whose digest contribution
    /// matches the one already folded in.
    /// </summary>
    private void StoreDeferredCrdtRow(
        string key,
        in LwwValue<byte[]> metadataRow,
        CrdtShape shape,
        object typedState,
        long serializedLength)
    {
        Cache.StoreDeferredRow(
            key,
            in metadataRow,
            () => shape.SerializeState(typedState),
            serializedLength);
    }
}
