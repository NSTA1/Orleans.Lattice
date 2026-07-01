using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Activation-scoped delivery-cursor partial for <see cref="Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain"/>.
/// Implements the in-memory sequence map consumed by
/// <see cref="LeafCacheGrain"/> through <see cref="BPlusLeafGrain.GetDeltaSinceCursorAsync"/>.
/// <para>
/// The cursor is intentionally non-persistent: a leaf re-activation
/// produces a fresh <see cref="_deliveryEpoch"/>, every cache holding
/// a stale cursor falls back to a full-snapshot delivery on its next
/// refresh, and the WAL replay path remains the sole projection
/// source-of-truth. This keeps the cursor free of any per-write
/// durable I/O - the future leaf-side WAL ownership work
/// (planned successor to the retroactive-pending-tx-sweep fan-out)
/// is unaffected.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// Process-wide monotonic source for <see cref="_deliveryEpoch"/>.
    /// Incremented on every leaf activation (lazily on first use); a
    /// freshly-activated leaf therefore mismatches every cache's
    /// previously-observed epoch, forcing a full-snapshot refresh.
    /// </summary>
    private static long s_deliveryEpochSeed;

    /// <summary>
    /// Per-activation delivery epoch, lazily assigned on first cursor
    /// use. <c>0</c> is reserved for <see cref="LeafDeliveryCursor.Empty"/>
    /// so the very first cache request always trips the
    /// epoch-mismatch fast path.
    /// </summary>
    private long _deliveryEpoch;

    /// <summary>
    /// Per-activation write sequence. Bumped once per
    /// <see cref="StoreEntry(string, in Primitives.LwwValue{byte[]})"/>
    /// and <see cref="RemoveEntry(string)"/>; the resulting sequence
    /// is recorded in <see cref="_keyDeliverySequences"/> against the
    /// touched key.
    /// </summary>
    private long _deliverySequence;

    /// <summary>
    /// Per-key map of the highest delivery sequence at which the key
    /// was written or removed under this activation. Lazy-allocated
    /// to keep the steady-state footprint at zero for leaves that no
    /// cache has yet pulled from.
    /// </summary>
    private Dictionary<string, long>? _keyDeliverySequences;

    /// <summary>
    /// Records that the named key was just written or removed and
    /// returns the current delivery cursor. Called from
    /// <see cref="StoreEntry(string, in Primitives.LwwValue{byte[]})"/>
    /// and <see cref="RemoveEntry(string)"/> after the projection
    /// mutation completes so the cursor stays in lock-step with
    /// <c>Entries</c>.
    /// </summary>
    private void BumpDeliverySequenceFor(string key)
    {
        EnsureDeliveryEpochInitialized();
        _deliverySequence++;
        (_keyDeliverySequences ??= new Dictionary<string, long>(StringComparer.Ordinal))[key] = _deliverySequence;
    }

    /// <summary>
    /// Lazily assigns this activation's <see cref="_deliveryEpoch"/>.
    /// Done on first use rather than from a primary-constructor body
    /// because partial-class primary constructors cannot run extra
    /// initialization here without restructuring every other partial
    /// and the lazy path is allocation-free in steady state.
    /// </summary>
    private void EnsureDeliveryEpochInitialized()
    {
        if (_deliveryEpoch == 0)
        {
            _deliveryEpoch = Interlocked.Increment(ref s_deliveryEpochSeed);
        }
    }

    /// <summary>
    /// Snapshot of the leaf's current cursor. Useful for the
    /// full-snapshot branch in
    /// <see cref="BPlusLeafGrain.GetDeltaSinceCursorAsync"/> and for
    /// unit tests asserting cursor advancement.
    /// </summary>
    internal LeafDeliveryCursor CurrentDeliveryCursor
    {
        get
        {
            EnsureDeliveryEpochInitialized();
            return new LeafDeliveryCursor
            {
                Epoch = _deliveryEpoch,
                Sequence = _deliverySequence,
            };
        }
    }

    /// <inheritdoc cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.GetDeltaSinceCursorAsync"/>
    public Task<StateDelta> GetDeltaSinceCursorAsync(LeafDeliveryCursor sinceCursor)
    {
        EnsureDeliveryEpochInitialized();
        var current = new LeafDeliveryCursor
        {
            Epoch = _deliveryEpoch,
            Sequence = _deliverySequence,
        };

        // Epoch mismatch => fresh cache / stale activation. Hand back a
        // full snapshot of every live entry along with the leaf's
        // current cursor; the cache will adopt the new cursor and
        // resume incremental delivery from there.
        if (sinceCursor.Epoch != current.Epoch)
        {
            var snapshot = new Dictionary<string, LwwValue<byte[]>>(
                Cache.Count,
                StringComparer.Ordinal);
            foreach (var (key, lww) in Cache.EnumerateRows())
            {
                snapshot[key] = lww;
            }

            return Task.FromResult(new StateDelta
            {
                Entries = snapshot,
                Version = state.State.Version.Clone(),
                SplitKey = state.State.SplitKey,
                MovedAwaySlots = state.State.MovedAwaySlots is { Length: > 0 } ms ? ms : null,
                MovedAwayVsc = state.State.MovedAwayVirtualShardCount,
                DeliveryCursor = current,
            });
        }

        // Same epoch, already at head: nothing to ship beyond the
        // metadata signal already carried on the empty-delta envelope.
        if (sinceCursor.Sequence >= current.Sequence
            && state.State.SplitKey is null
            && (state.State.MovedAwaySlots is null || state.State.MovedAwaySlots.Length == 0))
        {
            return Task.FromResult(new StateDelta
            {
                Entries = EmptyEntries,
                Version = state.State.Version.Clone(),
                SplitKey = null,
                MovedAwaySlots = null,
                MovedAwayVsc = null,
                DeliveryCursor = current,
            });
        }

        // Incremental delivery: every key whose recorded sequence is
        // strictly greater than the caller's. The map is keyed by
        // string and bounded by the live working set on the leaf,
        // which in steady state is the same order of magnitude as
        // Entries itself.
        var changed = new Dictionary<string, LwwValue<byte[]>>(StringComparer.Ordinal);
        if (_keyDeliverySequences is { Count: > 0 } map)
        {
            foreach (var (key, seq) in map)
            {
                if (seq <= sinceCursor.Sequence)
                    continue;

                // Only ship entries that still exist in the projection.
                // A key whose latest mutation was a remove has its
                // sequence recorded here but is intentionally absent
                // from Entries; the cache's existing tombstone-merge
                // path is fed by the foreground tombstone-write, not
                // by the post-remove sequence bump.
                if (Cache.TryGetRow(key, out var lww))
                {
                    changed[key] = lww;
                }
            }
        }

        return Task.FromResult(new StateDelta
        {
            Entries = changed,
            Version = state.State.Version.Clone(),
            SplitKey = state.State.SplitKey,
            MovedAwaySlots = state.State.MovedAwaySlots is { Length: > 0 } ms2 ? ms2 : null,
            MovedAwayVsc = state.State.MovedAwayVirtualShardCount,
            DeliveryCursor = current,
        });
    }
}
