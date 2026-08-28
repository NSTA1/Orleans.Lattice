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
    /// <para>
    /// The seed is randomised per process rather than starting at zero.
    /// An epoch is compared across processes - a cache outlives the silo
    /// whose leaf activation minted the cursor it holds - so a counter
    /// starting from zero in every silo hands out the same low integers
    /// everywhere and two activations in different processes can mint the
    /// same epoch. A collision is not benign: it suppresses the
    /// epoch-mismatch full-snapshot path, so the holding cache never
    /// resyncs, keeps serving rows that diverged while it was
    /// disconnected, and - because <c>LeafCacheGrain</c> gates its cache
    /// eviction on the same epoch flip - keeps serving keys the leaf has
    /// since deleted. Seeding from a cryptographically-random 64-bit
    /// value makes a cross-process collision negligible while preserving
    /// the strict per-activation monotonicity within a process that the
    /// increment below relies on. The range is clamped away from the low
    /// integers so a collision cannot be produced by a process that has
    /// only just started, and <c>0</c> stays reserved for
    /// <see cref="LeafDeliveryCursor.Empty"/>.
    /// </para>
    /// </summary>
    private static long s_deliveryEpochSeed = InitialDeliveryEpochSeed();

    /// <summary>
    /// Produces the per-process starting point for
    /// <see cref="s_deliveryEpochSeed"/>: a random value in
    /// <c>[1, long.MaxValue / 2]</c>. The upper bound leaves a process
    /// room to mint activations without overflowing, and the lower bound
    /// keeps <c>0</c> reserved for <see cref="LeafDeliveryCursor.Empty"/>.
    /// </summary>
    private static long InitialDeliveryEpochSeed()
    {
        Span<byte> buffer = stackalloc byte[sizeof(long)];
        System.Security.Cryptography.RandomNumberGenerator.Fill(buffer);

        // Mask off the sign bit before the modulus so the result is always
        // positive (long.MinValue has no positive counterpart, so negating
        // it would overflow).
        var positive = BitConverter.ToInt64(buffer) & long.MaxValue;
        return (positive % (long.MaxValue / 2)) + 1;
    }

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
        //
        // A caller whose sequence is AHEAD of this activation's is
        // treated the same way, as a fail-safe. The epoch seed is
        // randomised per process so a cross-process collision is
        // negligible, but it is not impossible, and the consequence of
        // trusting one is silent: a fresh activation starts at sequence
        // 0 while a cache holding entries is at 1 or more, so under a
        // collided epoch the "already at head" branch below would ship an
        // empty delta forever, leaving the cache serving rows that
        // diverged while it was disconnected - and, because the cache
        // gates its own eviction on the epoch flip, still serving keys
        // this leaf has since deleted. A sequence ahead of ours cannot
        // have been issued by this activation, so it can only be a stale
        // cursor: snapshot instead of trusting it.
        if (sinceCursor.Epoch != current.Epoch || sinceCursor.Sequence > current.Sequence)
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
