using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Transient, in-memory fold of a write-ahead-log slice into a committed
/// key/value projection, shared by the two paths that need a CRDT-correct
/// replay outside the live leaf's mutable <c>Cache</c>:
/// <list type="number">
/// <item><description>
/// <see cref="SnapshotLeafGrain"/>'s legacy from-zero replay (the
/// <c>token == Empty</c> back-compat path).
/// </description></item>
/// <item><description>
/// the per-leaf capture-time tail fold that builds a
/// <see cref="Orleans.Lattice.BPlusTree.State.SnapshotShardBaseline"/> over
/// <c>(leaf_frontier, capturedHead]</c>.
/// </description></item>
/// </list>
/// Extracting one implementation keeps the two paths from diverging on the
/// subtle bits: LWW merge, the per-saga pending buckets, the CRDT typed-delta
/// terminal fold (which is <b>not</b> idempotent, so each record must be
/// applied exactly once), and range-tombstone application.
/// <para>
/// The folder is deliberately <b>ownership-agnostic</b>. The shared WAL
/// partition carries records for every shard and leaf, and the two callers
/// resolve "does this record belong here" differently (the snapshot leaf by
/// the pinned map's virtual-slot owner; the live leaf by its
/// <c>ShouldApplyDuringReplay</c> shard-stamp / key-range / map filter). Each
/// caller therefore pre-filters and only hands owned records to
/// <see cref="Apply"/>; the folder never second-guesses ownership.
/// </para>
/// <para>
/// Saga terminals (<see cref="MutationKind.TxCommit"/> /
/// <see cref="MutationKind.TxAbort"/>) and
/// <see cref="MutationKind.DeleteRange"/> must be deferred to a second pass
/// after every partition's per-key Set/Delete/prepare records have been
/// absorbed - see <see cref="IsDeferredKind"/>. Driving that two-pass split
/// is the caller's responsibility; the folder exposes the classifier and
/// applies whatever it is given.
/// </para>
/// </summary>
internal sealed class SnapshotProjectionFolder(
    string treeId,
    CrdtShapeRegistry crdtShapes,
    ILatticeEnvelopeCodec? envelopeCodec = null)
{
    private readonly SortedDictionary<string, LwwValue<byte[]>> _entries = new(StringComparer.Ordinal);
    private readonly Dictionary<Guid, Dictionary<string, LwwValue<byte[]>>> _pendingTx = new();
    private readonly Dictionary<Guid, Dictionary<string, (byte[] Delta, LatticeMergeMode Mode)>> _pendingTxDeltas = new();

    // Sagas whose terminal this fold applied (issue #4485): the orphan-guard
    // input when the still-pending buckets are resolved against a capture's
    // decision snapshot.
    private readonly HashSet<Guid> _terminalApplied = [];

    // Per-key durable merge-mode discriminator, mirroring the live leaf cache's
    // _mergeModes map: only CRDT keys are present (a plain last-writer-wins
    // fold removes the key), so an absent key materialises a null discriminator
    // and the capture path falls back to the declared tree mode.
    private readonly Dictionary<string, LatticeMergeMode> _modes = new(StringComparer.Ordinal);

    /// <summary>
    /// The folded committed projection, sorted by key. Live values and
    /// tombstones; tombstones are retained so the caller's scan-time filter
    /// decides visibility, exactly as the leaf-snapshot blob convention does.
    /// Exposed (not copied) so the snapshot leaf can serve range scans
    /// directly off it.
    /// </summary>
    public SortedDictionary<string, LwwValue<byte[]>> Entries => _entries;

    /// <summary>Number of sagas still pending (prepared, terminal not yet folded).</summary>
    public int PendingSagaCount => _pendingTx.Count;

    /// <summary>
    /// Returns <see langword="true"/> for the mutation kinds a two-pass
    /// replay must defer to pass 2 (saga terminals and range deletes).
    /// </summary>
    public static bool IsDeferredKind(MutationKind kind) =>
        kind is MutationKind.TxCommit or MutationKind.TxAbort or MutationKind.DeleteRange;

    /// <summary>
    /// Seeds a committed row directly into the projection, merging under LWW
    /// against any existing value. Used to pre-load a leaf's frozen cache
    /// baseline before the tail fold. The optional <paramref name="mergeMode"/>
    /// carries the durable per-key merge-mode discriminator through from the
    /// seed row so a materialised baseline (or a raw-entry scan served off the
    /// fold) stays mode-faithful.
    /// </summary>
    public void SeedRow(string key, LwwValue<byte[]> value, LatticeMergeMode? mergeMode = null)
    {
        MergeIntoEntries(key, value);
        if (mergeMode is { } mode)
        {
            RecordMode(key, mode);
        }
    }

    /// <summary>
    /// Returns the folded per-key merge-mode discriminator for
    /// <paramref name="key"/>, or <see langword="null"/> when the key is a plain
    /// last-writer-wins row. Consumed by the snapshot-leaf raw-entry scan so a
    /// backup capture reads each key's true merge mode.
    /// </summary>
    public LatticeMergeMode? GetMode(string key) =>
        _modes.TryGetValue(key, out var mode) ? mode : null;

    // Records (CRDT) or clears (LWW) the per-key merge-mode discriminator,
    // keeping parity with the live leaf cache: a LwwRegister fold removes the
    // key so the row materialises a null discriminator.
    private void RecordMode(string key, LatticeMergeMode mode)
    {
        if (mode == LatticeMergeMode.LwwRegister)
        {
            _modes.Remove(key);
        }
        else
        {
            _modes[key] = mode;
        }
    }

    /// <summary>
    /// Seeds a prepared (but not yet terminal) saga mutation into the pending
    /// buckets, identical to absorbing a prepared Set/Delete from the WAL.
    /// Used to pre-load a leaf's frozen pending-tx state before the tail fold
    /// so a terminal in the tail resolves against the real prepared mutation.
    /// </summary>
    public void SeedPending(Guid txId, string key, LwwValue<byte[]> value, byte[]? delta, LatticeMergeMode mode) =>
        AddPreparedMutation(txId, key, value, delta, mode);

    /// <summary>
    /// Folds one already-ownership-filtered WAL mutation into the projection.
    /// Mirrors the live leaf's <c>ILeafProjection.Apply</c> kind dispatch but
    /// stores into this folder's own dictionaries. Unknown kinds are dropped
    /// (forward-compat), matching the snapshot-leaf replay.
    /// </summary>
    public void Apply(in LatticeMutation mutation)
    {
        switch (mutation.Kind)
        {
            case MutationKind.Set:
                if (mutation.IsPrepared)
                    AddPreparedMutation(mutation.TransactionId, mutation.Key, BuildLww(mutation, isTombstone: mutation.IsTombstone), mutation.Delta, mutation.Mode);
                else if (mutation.Mode != LatticeMergeMode.LwwRegister && mutation.Delta is not null)
                {
                    MergeIntoEntries(mutation.Key, BuildFoldedCrdtSet(mutation));
                    RecordMode(mutation.Key, mutation.Mode);
                }
                else
                {
                    MergeIntoEntries(mutation.Key, BuildLww(mutation, isTombstone: mutation.IsTombstone));
                    _modes.Remove(mutation.Key);
                }
                break;
            case MutationKind.Delete:
                if (mutation.IsPrepared)
                    AddPreparedMutation(mutation.TransactionId, mutation.Key, BuildLww(mutation, isTombstone: true));
                else
                {
                    MergeIntoEntries(mutation.Key, BuildLww(mutation, isTombstone: true));
                    _modes.Remove(mutation.Key);
                }
                break;
            case MutationKind.DeleteRange:
                ApplyDeleteRange(mutation);
                break;
            case MutationKind.TxCommit:
                ApplyTxCommit(mutation.TransactionId);
                break;
            case MutationKind.TxAbort:
                ApplyTxAbort(mutation.TransactionId);
                break;
            case MutationKind.Tombstone:
                ApplyTombstoneReap(mutation);
                break;
            default:
                break;
        }
    }

    /// <summary>
    /// Materialises the folded projection into the canonical byte-row list
    /// (including tombstones) for persistence into a
    /// <see cref="Orleans.Lattice.BPlusTree.State.SnapshotShardBaseline"/>.
    /// </summary>
    public List<LeafSnapshotRow> Materialize()
    {
        var rows = new List<LeafSnapshotRow>(_entries.Count);
        foreach (var (key, value) in _entries)
        {
            rows.Add(new LeafSnapshotRow(key, value, GetMode(key)));
        }
        return rows;
    }

    private static LwwValue<byte[]> BuildLww(in LatticeMutation mutation, bool isTombstone) => new()
    {
        Value = isTombstone ? null : mutation.Value,
        Timestamp = mutation.Timestamp,
        IsTombstone = isTombstone,
        ExpiresAtTicks = isTombstone ? 0 : mutation.ExpiresAtTicks,
        OriginClusterId = mutation.OriginClusterId,
        VectorClock = mutation.VectorClock,
    };

    /// <summary>
    /// Folds a non-prepared, CRDT-mode Set record onto the current visible
    /// state for its key. A direct CRDT-delta WAL record is delta-only - the
    /// producer never materialises the post-merge state into
    /// <see cref="LatticeMutation.Value"/> and the canonical encoder strips the
    /// slot, so a replay observes <c>Value == null</c> with the typed delta in
    /// <see cref="LatticeMutation.Delta"/> and the convergence rule in
    /// <see cref="LatticeMutation.Mode"/>. Installing that null verbatim (the
    /// plain-LWW path) would drop the key's accumulated CRDT state entirely;
    /// instead fold the delta into the prior post-fold bytes exactly as the
    /// live leaf's <c>ApplySet</c> does. Folds compose incrementally across
    /// successive deltas for the same key because each fold reads the prior
    /// folded state back out of <see cref="_entries"/> and the caller applies
    /// records in WAL offset order.
    /// </summary>
    private LwwValue<byte[]> BuildFoldedCrdtSet(in LatticeMutation mutation) => new()
    {
        Value = FoldPreparedCrdtDelta(mutation.Key, mutation.Delta!, mutation.Mode),
        Timestamp = mutation.Timestamp,
        IsTombstone = false,
        ExpiresAtTicks = mutation.ExpiresAtTicks,
        OriginClusterId = mutation.OriginClusterId,
        VectorClock = mutation.VectorClock,
    };

    private void MergeIntoEntries(string key, LwwValue<byte[]> incoming)
    {
        if (_entries.TryGetValue(key, out var existing))
        {
            _entries[key] = LwwValue<byte[]>.Merge(existing, incoming);
        }
        else
        {
            _entries[key] = incoming;
        }
    }

    private void AddPreparedMutation(Guid txId, string key, LwwValue<byte[]> incoming, byte[]? delta = null, LatticeMergeMode mode = LatticeMergeMode.LwwRegister)
    {
        if (!_pendingTx.TryGetValue(txId, out var bucket))
        {
            bucket = new Dictionary<string, LwwValue<byte[]>>(StringComparer.Ordinal);
            _pendingTx[txId] = bucket;
        }
        bucket[key] = incoming;

        if (delta is not null && mode != LatticeMergeMode.LwwRegister)
        {
            if (!_pendingTxDeltas.TryGetValue(txId, out var deltaBucket))
            {
                deltaBucket = new Dictionary<string, (byte[], LatticeMergeMode)>(StringComparer.Ordinal);
                _pendingTxDeltas[txId] = deltaBucket;
            }
            deltaBucket[key] = (delta, mode);
        }
    }

    private void ApplyDeleteRange(in LatticeMutation mutation)
    {
        var endExclusive = mutation.EndExclusiveKey;
        if (endExclusive is null)
            return;
        var startInclusive = mutation.Key;
        if (string.CompareOrdinal(startInclusive, endExclusive) >= 0)
            return;

        List<string>? toRewrite = null;
        var matchedKeys = mutation.MatchedKeys;
        if (matchedKeys is not null)
        {
            foreach (var key in matchedKeys)
            {
                if (string.CompareOrdinal(key, startInclusive) < 0
                    || string.CompareOrdinal(key, endExclusive) >= 0)
                    continue;
                if (_entries.ContainsKey(key))
                    (toRewrite ??= []).Add(key);
            }
        }
        else
        {
            foreach (var (key, _) in _entries)
            {
                if (string.CompareOrdinal(key, startInclusive) < 0)
                    continue;
                if (string.CompareOrdinal(key, endExclusive) >= 0)
                    break;
                (toRewrite ??= []).Add(key);
            }
        }

        if (toRewrite is null)
            return;

        var tombstone = new LwwValue<byte[]>
        {
            Value = null,
            Timestamp = mutation.Timestamp,
            IsTombstone = true,
            ExpiresAtTicks = 0,
            OriginClusterId = mutation.OriginClusterId,
            VectorClock = mutation.VectorClock,
        };

        foreach (var key in toRewrite)
        {
            MergeIntoEntries(key, tombstone);
            _modes.Remove(key);
        }
    }

    private void ApplyTxCommit(Guid txId)
    {
        _terminalApplied.Add(txId);
        _pendingTxDeltas.Remove(txId, out var deltaBucket);
        if (!_pendingTx.Remove(txId, out var bucket))
            return;
        foreach (var (key, value) in bucket)
        {
            if (deltaBucket is not null && deltaBucket.TryGetValue(key, out var dm))
            {
                var folded = FoldPreparedCrdtDelta(key, dm.Delta, dm.Mode);
                MergeIntoEntries(key, value with { Value = folded });
                RecordMode(key, dm.Mode);
            }
            else
            {
                MergeIntoEntries(key, value);
                _modes.Remove(key);
            }
        }
    }

    private byte[] FoldPreparedCrdtDelta(string key, byte[] delta, LatticeMergeMode mode)
    {
        var shape = crdtShapes.TryGet(treeId, mode)
            ?? throw new InvalidOperationException(
                $"No CrdtShape is registered for tree '{treeId}' at mode '{mode}'. "
                + "A prepared CRDT-mode entry cannot fold its typed delta on the snapshot "
                + "leaf's terminal commit without a shape descriptor.");

        // Strip any self-describing version envelope from the durable delta before
        // deserialising (version-agnostic; identity when no versioning is active).
        // The durable WAL delta is stored enveloped at the version it was ingested /
        // upcast to, and this restore-replay fold recovers the same raw body the
        // live leaf's apply folded, so the rebuilt projection is byte-identical.
        //
        // The delta and the stored state both route through this one local function
        // deliberately. They were previously two expressions and only the delta one
        // stripped, which handed the shape an enveloped state whose 0xFE magic fails
        // a JSON decode at byte zero - a row that still reads back cleanly but can
        // never be written again, because every retry re-decodes the same bytes.
        // Sharing the expression makes that asymmetry non-expressible here.
        byte[] StripForFold(byte[] bytes) =>
            envelopeCodec is not null and not NullLatticeEnvelopeCodec && envelopeCodec.IsActive(treeId)
                ? envelopeCodec.StripForFold(bytes)
                : bytes;

        var typedDelta = shape.DeserializeDelta(StripForFold(delta));
        object typedState;
        if (_entries.TryGetValue(key, out var existing)
            && !existing.IsTombstone
            && existing.Value is { Length: > 0 } existingBytes)
        {
            typedState = shape.DeserializeState(StripForFold(existingBytes));
        }
        else
        {
            typedState = shape.CreateEmpty();
        }
        shape.MergeDelta(typedState, typedDelta);
        return shape.SerializeState(typedState);
    }

    private void ApplyTxAbort(Guid txId)
    {
        _terminalApplied.Add(txId);
        _pendingTxDeltas.Remove(txId);
        _pendingTx.Remove(txId);
    }

    /// <summary>
    /// The transaction ids of the sagas still pending (prepared, terminal not
    /// yet folded), for resolving them against a capture's decision snapshot.
    /// </summary>
    public IReadOnlyCollection<Guid> PendingTransactionIds => _pendingTx.Keys;

    /// <summary>
    /// Resolves every still-pending prepared bucket against a snapshot capture's
    /// decision snapshot (D0) exactly as a live multi-key read resolves it
    /// (issue #4485), then drops the buckets. Per key, the deciding bucket is
    /// chosen with <see cref="AtomicVisibilityGate.SelectDecidingPrepare"/> and
    /// resolved with <see cref="AtomicVisibilityGate.ResolveKey"/>:
    /// <list type="bullet">
    /// <item><description>
    /// a saga Committed in D0 whose terminal has not landed here installs its
    /// prepared value (a CRDT delta is folded into the row), stamped to
    /// dominate the pre-saga row, so the key reads post-saga;
    /// </description></item>
    /// <item><description>
    /// a saga Indeterminate in D0 removes the key, which reads as absent (the
    /// #2328 export vocabulary: neither candidate value is asserted);
    /// </description></item>
    /// <item><description>
    /// anything else (InFlight, Aborted, an already-terminal orphan, or a
    /// prepare the row supersedes) leaves the pre-saga row.
    /// </description></item>
    /// </list>
    /// </summary>
    /// <param name="decisions">The per-txid status as of D0, for every pending txid.</param>
    /// <param name="alreadyTerminal">
    /// The sagas whose terminal the source leaf had already applied when it was
    /// frozen; together with the terminals folded from the tail they feed the
    /// orphan guard.
    /// </param>
    public void ResolvePendingAgainst(IReadOnlyDictionary<Guid, TxStatus> decisions, IReadOnlyCollection<Guid>? alreadyTerminal)
    {
        ArgumentNullException.ThrowIfNull(decisions);
        if (_pendingTx.Count == 0)
            return;

        var view = new TxDecisionView(decisions);
        var byKey = new Dictionary<string, List<(Guid TxId, LwwValue<byte[]> Value)>>(StringComparer.Ordinal);
        foreach (var (txId, bucket) in _pendingTx)
        {
            foreach (var (key, value) in bucket)
            {
                if (!byKey.TryGetValue(key, out var list))
                {
                    list = [];
                    byKey[key] = list;
                }

                list.Add((txId, value));
            }
        }

        foreach (var (key, list) in byKey)
        {
            var candidates = new PreparedCandidate[list.Count];
            for (var i = 0; i < list.Count; i++)
            {
                var (txId, value) = list[i];
                var terminal = _terminalApplied.Contains(txId)
                    || (alreadyTerminal is not null && alreadyTerminal.Contains(txId));
                candidates[i] = new PreparedCandidate(
                    view.Resolve(txId),
                    terminal,
                    IsSupersededByRow(key, txId, value),
                    value.Timestamp);
            }

            var chosen = AtomicVisibilityGate.SelectDecidingPrepare(candidates);
            if (chosen < 0)
                continue;

            var candidate = candidates[chosen];
            switch (AtomicVisibilityGate.ResolveKey(candidate.Status, candidate.AlreadyTerminal, preparedHiddenByTombstoneOrExpiry: false))
            {
                case PendingReadOutcome.SurfacePrepared:
                    InstallPrepared(list[chosen].TxId, key, list[chosen].Value);
                    break;
                case PendingReadOutcome.Hidden:
                    _entries.Remove(key);
                    _modes.Remove(key);
                    break;
            }
        }

        _pendingTx.Clear();
        _pendingTxDeltas.Clear();
    }

    /// <summary>
    /// Whether the folded row for <paramref name="key"/> already supersedes the
    /// saga's prepared value, by the same comparison the live leaf's commit drain
    /// makes: a newer, non-migrated row wins, and a CRDT-delta prepare is folded
    /// rather than merged so no row supersedes it.
    /// </summary>
    private bool IsSupersededByRow(string key, Guid txId, LwwValue<byte[]> value)
    {
        if (!_entries.TryGetValue(key, out var row) || row.IsMigrated)
            return false;
        if (_pendingTxDeltas.TryGetValue(txId, out var deltas) && deltas.ContainsKey(key))
            return false;
        return row.Timestamp.CompareTo(value.Timestamp) > 0;
    }

    /// <summary>
    /// Installs a committed saga's prepared value for <paramref name="key"/> as
    /// the folded row, stamped strictly above any existing row so it wins the
    /// row-level merge exactly as the live commit drain's re-stamp does.
    /// </summary>
    private void InstallPrepared(Guid txId, string key, LwwValue<byte[]> value)
    {
        var stamp = value.Timestamp;
        if (_entries.TryGetValue(key, out var row) && row.Timestamp.CompareTo(stamp) >= 0)
        {
            stamp = new HybridLogicalClock
            {
                WallClockTicks = row.Timestamp.WallClockTicks,
                Counter = row.Timestamp.Counter + 1,
            };
        }

        if (_pendingTxDeltas.TryGetValue(txId, out var deltas) && deltas.TryGetValue(key, out var dm))
        {
            var folded = FoldPreparedCrdtDelta(key, dm.Delta, dm.Mode);
            _entries[key] = value with { Value = folded, Timestamp = stamp, IsTombstone = false };
            RecordMode(key, dm.Mode);
            return;
        }

        _entries[key] = value with { Timestamp = stamp };
        _modes.Remove(key);
    }

    private void ApplyTombstoneReap(in LatticeMutation mutation)
    {
        if (!_entries.TryGetValue(mutation.Key, out var existing))
            return;
        if (existing.Timestamp > mutation.Timestamp)
            return;
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        if (!existing.IsTombstone && !existing.IsExpired(nowTicks))
            return;
        _entries.Remove(mutation.Key);
        _modes.Remove(mutation.Key);
    }
}
