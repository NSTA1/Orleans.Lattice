using System.Buffers;
using System.Buffers.Binary;
using System.IO.Hashing;
using System.Text;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Leaf-side projection-digest implementation. Maintains a running
/// 16-byte XOR-fold over per-entry XxHash128 contributions in
/// <c>state.State.ProjectionHash</c>, updated incrementally on every
/// mutation through <see cref="UpdateProjectionHash(string, in LwwValue{byte[]}?, in LwwValue{byte[]}?)"/>.
/// The public <see cref="BPlusLeafGrain.GetProjectionDigestAsync"/>
/// reads that field and folds in the entry count and
/// <c>ProjectionCheckpointOffset</c> via XxHash128, so two silos that have
/// applied the same prefix of the same WAL produce byte-identical
/// digests in O(1) per call rather than O(entries). XxHash128 is a
/// non-cryptographic hash chosen for its uniform output distribution
/// (which the XOR-fold algebra requires) and ~10x lower CPU cost than
/// SHA-256 on the per-mutation hot path; the digest is a drift-detection
/// fingerprint, not an authentication tag, so collision resistance against
/// an adversary is not required.
/// <para>
/// When <see cref="LatticeOptions.MaintainProjectionDigest"/> is set to
/// <c>false</c> the funnels (<c>StoreEntry</c> / <c>RemoveEntry</c>) take a
/// trimmed path that skips both the XOR fold and the upward
/// <c>ChildDigestSnapshot</c> publication, and
/// <see cref="GetProjectionDigestAsync"/> throws
/// <see cref="InvalidOperationException"/> so callers cannot observe a
/// stale aggregate. The persisted <c>ProjectionHash</c> column is left
/// untouched across the quiescent period; mutations resume incremental
/// maintenance from the persisted bytes the next time the option flips
/// back to <c>true</c>.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    private const int ProjectionHashSize = 16;

    /// <summary>
    /// Cached <see cref="XxHash128"/> reused across every per-entry
    /// contribution computed inside this grain activation. Lazily
    /// created on first use and reset (not recreated) between
    /// contributions via <see cref="NonCryptographicHashAlgorithm.TryGetHashAndReset"/>.
    /// Caching matters because every mutation may produce one or two
    /// contributions (insert vs replace), and a freshly-allocated
    /// hasher per call would dominate the fold's allocation profile
    /// on the hot path.
    /// </summary>
    private XxHash128? _entryHasher;

    /// <summary>
    /// Internal hook invoked from <see cref="BPlusLeafGrain"/>'s
    /// <c>OnDeactivateAsync</c> to release the cached hasher.
    /// <see cref="XxHash128"/> is not <see cref="IDisposable"/>, so this
    /// helper merely drops the reference to allow it to be collected
    /// alongside the activation.
    /// </summary>
    internal void DisposeProjectionHasher()
    {
        _entryHasher = null;
    }

    /// <summary>
    /// Centralised entry-write funnel. LWW-merges <paramref name="incoming"/>
    /// against any existing entry, writes the merged value back into
    /// <see cref="LeafNodeState.Entries"/>, and XOR-folds the contribution
    /// delta into the running projection hash. Every leaf-state write that
    /// adds-or-replaces a key must funnel through this helper so the hash
    /// stays in sync with <c>Entries</c>.
    /// </summary>
    private LwwValue<byte[]> StoreEntry(string key, in LwwValue<byte[]> incoming)
    {
        if (!_maintainProjectionDigest)
        {
            // Maintenance disabled - take the minimum-cost path: LWW merge
            // (or insert) the value, bump the delivery sequence, and skip
            // both the per-entry XOR fold and the upward ChildDigestSnapshot
            // publish. The persisted ProjectionHash is left untouched so
            // it remains a valid prefix of a previous-enabled state.
            if (Cache.TryGetRow(key, out var existingNoDigest))
            {
                var mergedNoDigest = LwwValue<byte[]>.Merge(existingNoDigest, incoming);
                Cache.StoreRow(key, mergedNoDigest);
                BumpDeliverySequenceFor(key);
                return mergedNoDigest;
            }
            Cache.StoreRow(key, incoming);
            BumpDeliverySequenceFor(key);
            return incoming;
        }

        EnsureProjectionHashInitialized();
        if (Cache.TryGetRow(key, out var existing))
        {
            var merged = LwwValue<byte[]>.Merge(existing, incoming);
            Cache.StoreRow(key, merged);
            // No-op merges (incoming dominated by existing) leave the
            // entry's contribution bytes unchanged; the equality check
            // inside UpdateProjectionHash skips both XORs and avoids
            // flipping the dirty flag, so a re-application at or below
            // the persisted Timestamp does not trigger a redundant
            // upward publish.
            UpdateProjectionHash(key, existing, merged);
            BumpDeliverySequenceFor(key);
            return merged;
        }
        else
        {
            Cache.StoreRow(key, incoming);
            UpdateProjectionHash(key, oldValue: null, incoming);
            BumpDeliverySequenceFor(key);
            return incoming;
        }
    }

    /// <summary>
    /// Centralised entry-removal funnel. Removes the entry (if present) and
    /// XOR-folds the removed contribution out of the running projection hash.
    /// Every leaf-state write that physically removes a key must funnel
    /// through this helper.
    /// </summary>
    private bool RemoveEntry(string key)
    {
        if (!Cache.TryGetRow(key, out var existing))
        {
            return false;
        }
        if (!_maintainProjectionDigest)
        {
            // Maintenance disabled - drop the entry and skip both the
            // XOR-out of its contribution and the upward publish.
            Cache.Remove(key);
            BumpDeliverySequenceFor(key);
            return true;
        }
        EnsureProjectionHashInitialized();
        Cache.Remove(key);
        UpdateProjectionHash(key, existing, newValue: null);
        BumpDeliverySequenceFor(key);
        return true;
    }

    /// <summary>
    /// Test-only accessor for the persisted projection-hash slot. Exposed
    /// internal so the walk-state oracle can compare the running
    /// XOR-fold against a fresh walk of <see cref="LeafNodeState.Entries"/>.
    /// </summary>
    internal byte[]? PersistedProjectionHash => state.State.ProjectionHash;

    /// <inheritdoc />
    public async Task<LeafProjectionDigest> GetProjectionDigestAsync()
    {
        // Read-path entry must observe the resolved opt-out, even on a
        // freshly-activated grain that has not yet seen a mutation. The
        // cached field defaults to the option's compile-time default;
        // a call to GetOptionsAsync() forces resolver hydration which
        // overwrites the cache from the per-tree configured value
        // before we branch on it.
        var options = await GetOptionsAsync();
        if (!options.MaintainProjectionDigest)
        {
            throw new InvalidOperationException(
                $"Projection-digest maintenance is disabled for this tree " +
                $"({nameof(LatticeOptions)}.{nameof(LatticeOptions.MaintainProjectionDigest)} = false), " +
                "so the persisted running hash is not the source of truth and the " +
                "digest API is unavailable. Set the option to true to resume maintenance.");
        }

        EnsureProjectionHashInitialized();

        var hasher = new XxHash128();
        Span<byte> scratch = stackalloc byte[8];

        // Fold the persisted XOR running-hash directly into the trailing
        // XxHash128 block, then chain the entry count and the checkpoint
        // offset so two leaves with identical entries but different
        // applied-prefix positions report distinct digests.
        hasher.Append(state.State.ProjectionHash!);

        var entryCount = (long)Cache.Count;
        var checkpointOffset = state.State.ProjectionCheckpointOffset;

        BinaryPrimitives.WriteInt64LittleEndian(scratch, entryCount);
        hasher.Append(scratch[..8]);
        BinaryPrimitives.WriteInt64LittleEndian(scratch, checkpointOffset);
        hasher.Append(scratch[..8]);

        var hash = hasher.GetHashAndReset();
        return new LeafProjectionDigest
        {
            Hash = hash,
            EntryCount = entryCount,
            CheckpointOffset = checkpointOffset,
            Version = LeafProjectionDigest.CurrentVersion,
        };
    }

    /// <summary>
    /// Lazily backfills <c>state.State.ProjectionHash</c> if persisted state
    /// pre-dates the slot or carries a hash from an older algorithm whose
    /// width no longer matches <see cref="ProjectionHashSize"/>. Cost is
    /// one full walk over <see cref="Entries"/>; every subsequent mutation
    /// maintains the field incrementally.
    /// </summary>
    private void EnsureProjectionHashInitialized()
    {
        if (state.State.ProjectionHash is null
            || state.State.ProjectionHash.Length != ProjectionHashSize)
        {
            state.State.ProjectionHash = ComputeFullProjectionHashFromState();
        }
    }

    /// <summary>
    /// XOR-folds the delta between an old and new entry value into the
    /// running projection hash. <paramref name="oldValue"/> is XOR'd out
    /// (self-inverse), <paramref name="newValue"/> is XOR'd in. Either may
    /// be <c>null</c> for pure insertion or pure deletion. Caller must have
    /// already invoked <see cref="EnsureProjectionHashInitialized"/>.
    /// <para>
    /// Flips <c>_digestDirty</c> when the running hash actually changes so
    /// the parent-publication path can elide no-op publishes. A merge that
    /// produces a contribution byte-identical to the prior contribution
    /// (replay of an already-applied LWW value at the same Timestamp,
    /// OriginClusterId, VectorClock, and value bytes) self-cancels under
    /// the XOR and leaves the hash unchanged - the dirty flag stays as
    /// it was.
    /// </para>
    /// </summary>
    private void UpdateProjectionHash(string key, in LwwValue<byte[]>? oldValue, in LwwValue<byte[]>? newValue)
    {
        var hash = state.State.ProjectionHash!;
        Span<byte> oldContribution = stackalloc byte[ProjectionHashSize];
        Span<byte> newContribution = stackalloc byte[ProjectionHashSize];
        var hasOld = false;
        var hasNew = false;

        if (oldValue is { } ov)
        {
            ComputeEntryContribution(key, in ov, oldContribution);
            hasOld = true;
        }
        if (newValue is { } nv)
        {
            ComputeEntryContribution(key, in nv, newContribution);
            hasNew = true;
        }

        // Short-circuit a no-op update: both contributions present and
        // byte-identical (a re-application of an already-applied LWW
        // value) - the XOR pair would self-cancel and the publication
        // would be a redundant cross-grain call.
        if (hasOld && hasNew && oldContribution.SequenceEqual(newContribution))
        {
            return;
        }

        if (hasOld)
        {
            for (var i = 0; i < ProjectionHashSize; i++) hash[i] ^= oldContribution[i];
        }
        if (hasNew)
        {
            for (var i = 0; i < ProjectionHashSize; i++) hash[i] ^= newContribution[i];
        }

        // At this point the running hash has changed (we did not take
        // the no-op fast path above), so the parent's snapshot for
        // this leaf is stale and must be refreshed.
        _digestDirty = true;
    }

    /// <summary>
    /// Walks the current <see cref="Entries"/> and produces the XOR-fold of
    /// every entry's contribution. Used for lazy backfill of legacy state
    /// and exposed (internal) as the regression-test oracle for the
    /// incremental fold.
    /// </summary>
    internal byte[] ComputeFullProjectionHashFromState()
    {
        var hash = new byte[ProjectionHashSize];
        Span<byte> contribution = stackalloc byte[ProjectionHashSize];
        foreach (var (key, lww) in Cache.EnumerateRows())
        {
            ComputeEntryContribution(key, in lww, contribution);
            for (var i = 0; i < ProjectionHashSize; i++) hash[i] ^= contribution[i];
        }
        return hash;
    }

    /// <summary>
    /// Computes the deterministic 16-byte XxHash128 contribution of a single
    /// entry. The contribution shape is stable wire-format: changing it
    /// changes every silo's digest. Fields are length-prefixed where they
    /// can collide and follow the historical
    /// <c>(key, hlc, isTombstone, expiresAt, origin, vectorClock, value)</c>
    /// ordering established by the on-demand walk this method replaced.
    /// Reuses the activation-cached <see cref="_entryHasher"/> via
    /// <see cref="NonCryptographicHashAlgorithm.TryGetHashAndReset"/> so the
    /// hot path allocates no hasher instance per call.
    /// </summary>
    private void ComputeEntryContribution(string key, in LwwValue<byte[]> lww, Span<byte> dest16)
    {
        _entryHasher ??= new XxHash128();
        var hasher = _entryHasher;
        Span<byte> scratch = stackalloc byte[8];

        // (key) - UTF-8 bytes, length-prefixed so adjacent fields cannot collide.
        FeedString(hasher, key, scratch);

        // (hlc.WallClockTicks, hlc.Counter)
        BinaryPrimitives.WriteInt64LittleEndian(scratch, lww.Timestamp.WallClockTicks);
        hasher.Append(scratch[..8]);
        BinaryPrimitives.WriteInt32LittleEndian(scratch[..4], lww.Timestamp.Counter);
        hasher.Append(scratch[..4]);

        // (isTombstone)
        scratch[0] = lww.IsTombstone ? (byte)1 : (byte)0;
        hasher.Append(scratch[..1]);

        // (expiresAtTicks)
        BinaryPrimitives.WriteInt64LittleEndian(scratch, lww.ExpiresAtTicks);
        hasher.Append(scratch[..8]);

        // (originClusterId) - null encoded as length 0xFFFFFFFF, distinct from empty string.
        FeedNullableString(hasher, lww.OriginClusterId, scratch);

        // (vector-clock fingerprint) - sorted (replicaId, hlc) pairs.
        FeedVectorClock(hasher, lww.VectorClock, scratch);

        // (value) - only for live, non-tombstone entries; tombstones encode -1.
        if (!lww.IsTombstone && lww.Value is not null)
        {
            BinaryPrimitives.WriteInt32LittleEndian(scratch[..4], lww.Value.Length);
            hasher.Append(scratch[..4]);
            hasher.Append(lww.Value);
        }
        else
        {
            BinaryPrimitives.WriteInt32LittleEndian(scratch[..4], -1);
            hasher.Append(scratch[..4]);
        }

        if (!hasher.TryGetHashAndReset(dest16, out var written) || written != ProjectionHashSize)
        {
            // Defensive: XxHash128 always produces 16 bytes; this branch is unreachable
            // unless the caller passed a smaller destination span.
            throw new InvalidOperationException("XxHash128 contribution did not produce 16 bytes.");
        }
    }

    private static void FeedString(XxHash128 hasher, string value, Span<byte> scratch)
    {
        var byteCount = Encoding.UTF8.GetByteCount(value);
        BinaryPrimitives.WriteInt32LittleEndian(scratch[..4], byteCount);
        hasher.Append(scratch[..4]);
        if (byteCount == 0) return;

        if (byteCount <= 256)
        {
            Span<byte> buf = stackalloc byte[256];
            var written = Encoding.UTF8.GetBytes(value, buf);
            hasher.Append(buf[..written]);
        }
        else
        {
            var rented = ArrayPool<byte>.Shared.Rent(byteCount);
            try
            {
                var written = Encoding.UTF8.GetBytes(value, rented);
                hasher.Append(rented.AsSpan(0, written));
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    private static void FeedNullableString(XxHash128 hasher, string? value, Span<byte> scratch)
    {
        if (value is null)
        {
            BinaryPrimitives.WriteInt32LittleEndian(scratch[..4], -1);
            hasher.Append(scratch[..4]);
            return;
        }
        FeedString(hasher, value, scratch);
    }

    private static void FeedVectorClock(XxHash128 hasher, VersionVector? vc, Span<byte> scratch)
    {
        if (vc is null || vc.Entries.Count == 0)
        {
            BinaryPrimitives.WriteInt32LittleEndian(scratch[..4], -1);
            hasher.Append(scratch[..4]);
            return;
        }

        // Replica ids are sorted with Ordinal so the digest is stable
        // regardless of which replica the dictionary insertion order
        // happened to pick. Rent the scratch array from the shared pool
        // to avoid a per-entry heap allocation on replicated trees.
        var count = vc.Entries.Count;
        var replicas = ArrayPool<string>.Shared.Rent(count);
        try
        {
            var i = 0;
            foreach (var k in vc.Entries.Keys) replicas[i++] = k;
            Array.Sort(replicas, 0, count, StringComparer.Ordinal);

            BinaryPrimitives.WriteInt32LittleEndian(scratch[..4], count);
            hasher.Append(scratch[..4]);

            for (var j = 0; j < count; j++)
            {
                var replica = replicas[j];
                FeedString(hasher, replica, scratch);
                var clock = vc.Entries[replica];
                BinaryPrimitives.WriteInt64LittleEndian(scratch, clock.WallClockTicks);
                hasher.Append(scratch[..8]);
                BinaryPrimitives.WriteInt32LittleEndian(scratch[..4], clock.Counter);
                hasher.Append(scratch[..4]);
            }
        }
        finally
        {
            // clearArray: true so we don't pin string references in the pool.
            ArrayPool<string>.Shared.Return(replicas, clearArray: true);
        }
    }
}
