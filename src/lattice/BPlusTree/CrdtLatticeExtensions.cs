namespace Orleans.Lattice;

/// <summary>
/// Typed CRDT value-surface accessor extensions on <see cref="ILattice"/>.
/// Each method returns a lightweight, allocation-free accessor that reads
/// and writes a single key under optimistic concurrency, exposing the
/// primitive's natural mutation API (add / remove, increment / decrement,
/// tick / merge) instead of forcing callers to hand-roll byte arrays and
/// CAS retry loops.
/// </summary>
public static class CrdtLatticeExtensions
{
    /// <summary>
    /// Enables many OR-Flags in one batched write: reads every current flag with
    /// a single <see cref="ILattice.GetManyAsync(List{string}, System.Threading.CancellationToken)"/>,
    /// mints each key's enable delta from that snapshot, and applies them all
    /// through one
    /// <see cref="ILattice.ApplyCrdtDeltaManyAsync(List{KeyValuePair{string, byte[]}}, LatticeMergeMode, System.Threading.CancellationToken)"/>.
    /// <para>
    /// This is the batched replacement for a per-key
    /// <c>lattice.OrFlag(key).EnableAsync(replicaId)</c> loop, which costs two
    /// round trips per key (a read to mint the delta, then the apply). Presence
    /// or membership marking - the motivating workload - is exactly that loop.
    /// </para>
    /// <para>
    /// <b>Not atomic</b>, like the underlying batch. Enabling is idempotent under
    /// OR-Flag merge semantics, so a retried batch converges.
    /// </para>
    /// </summary>
    /// <param name="lattice">The OR-Flag-mode tree holding the flags.</param>
    /// <param name="keys">The keys to enable. An empty collection is a no-op.</param>
    /// <param name="replicaId">The replica identity minting the enable dots.</param>
    /// <param name="cancellationToken">Cancels the read or the batched apply.</param>
    /// <exception cref="System.ArgumentNullException">
    /// <paramref name="lattice"/>, <paramref name="keys"/>, or <paramref name="replicaId"/> is <see langword="null"/>.
    /// </exception>
    public static async Task EnableManyAsync(
        this ILattice lattice,
        IReadOnlyCollection<string> keys,
        string replicaId,
        CancellationToken cancellationToken = default)
    {
        var deltas = await MintEnableDeltasAsync(lattice, keys, replicaId, cancellationToken).ConfigureAwait(false);
        if (deltas.Count == 0)
        {
            return;
        }

        await lattice.ApplyCrdtDeltaManyAsync(deltas, LatticeMergeMode.OrFlag, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Stages many OR-Flag enables for a cross-tree atomic write, minting every
    /// delta from a single batched read rather than one read per key. Hand the
    /// returned tokens to
    /// <see cref="LatticeAtomicWriteBuilder.SetMany(IEnumerable{LatticeStagedCrdtWrite})"/>.
    /// <para>
    /// The staging cost is what made a wide atomic CRDT write expensive: each
    /// accessor's <c>Stage*</c> reads its own key, so staging N keys was N reads
    /// before the saga even began. This collapses that to one.
    /// </para>
    /// </summary>
    /// <param name="lattice">The OR-Flag-mode tree holding the flags.</param>
    /// <param name="keys">The keys to stage enables for.</param>
    /// <param name="replicaId">The replica identity minting the enable dots.</param>
    /// <param name="cancellationToken">Cancels the batched read.</param>
    /// <returns>One staging token per key, in <paramref name="keys"/> order.</returns>
    /// <exception cref="System.ArgumentNullException">
    /// <paramref name="lattice"/>, <paramref name="keys"/>, or <paramref name="replicaId"/> is <see langword="null"/>.
    /// </exception>
    public static async Task<IReadOnlyList<LatticeStagedCrdtWrite>> StageEnableManyAsync(
        this ILattice lattice,
        IReadOnlyCollection<string> keys,
        string replicaId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        ArgumentNullException.ThrowIfNull(keys);
        ArgumentNullException.ThrowIfNull(replicaId);
        if (keys.Count == 0)
        {
            return Array.Empty<LatticeStagedCrdtWrite>();
        }

        var current = await ReadFlagsAsync(lattice, keys, cancellationToken).ConfigureAwait(false);
        var staged = new List<LatticeStagedCrdtWrite>(keys.Count);
        foreach (var key in keys)
        {
            current.TryGetValue(key, out var raw);
            var flag = OrFlagAccessor.DecodeFlag(raw);
            var delta = OrFlagAccessor.EnableDeltaFor(flag, replicaId);

            // Mint-once, exactly as the per-key Stage* does: fold the delta into
            // the snapshot so the token carries both the merged state (for the
            // saga's LWW-shaped commit) and the delta (so a remote cluster folds
            // it and converges).
            flag.MergeDelta(delta);
            staged.Add(new LatticeStagedCrdtWrite(
                key,
                JsonLatticeSerializer<OrFlag>.Default.Serialize(flag),
                JsonLatticeSerializer<OrFlagDelta>.Default.Serialize(delta)));
        }

        return staged;
    }

    private static async Task<List<KeyValuePair<string, byte[]>>> MintEnableDeltasAsync(
        ILattice lattice,
        IReadOnlyCollection<string> keys,
        string replicaId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        ArgumentNullException.ThrowIfNull(keys);
        ArgumentNullException.ThrowIfNull(replicaId);
        if (keys.Count == 0)
        {
            return new List<KeyValuePair<string, byte[]>>();
        }

        var current = await ReadFlagsAsync(lattice, keys, cancellationToken).ConfigureAwait(false);
        var deltas = new List<KeyValuePair<string, byte[]>>(keys.Count);
        foreach (var key in keys)
        {
            current.TryGetValue(key, out var raw);
            var flag = OrFlagAccessor.DecodeFlag(raw);
            var delta = OrFlagAccessor.EnableDeltaFor(flag, replicaId);
            deltas.Add(new KeyValuePair<string, byte[]>(
                key, JsonLatticeSerializer<OrFlagDelta>.Default.Serialize(delta)));
        }

        return deltas;
    }

    /// <summary>
    /// Reads the current rows for <paramref name="keys"/> in one batched call.
    /// Absent keys are simply missing from the result and decode as an empty flag.
    /// </summary>
    private static Task<Dictionary<string, byte[]>> ReadFlagsAsync(
        ILattice lattice,
        IReadOnlyCollection<string> keys,
        CancellationToken cancellationToken)
    {
        var probe = new List<string>(keys.Count);
        foreach (var key in keys)
        {
            ArgumentNullException.ThrowIfNull(key);
            probe.Add(key);
        }

        return lattice.GetManyAsync(probe, cancellationToken);
    }

    /// <summary>
    /// Returns a typed accessor for an observed-remove (OR) set stored
    /// under <paramref name="key"/> in <paramref name="lattice"/>.
    /// </summary>
    /// <param name="lattice">The tree containing the set.</param>
    /// <param name="key">The key the set is stored under.</param>
    public static OrSetAccessor OrSet(this ILattice lattice, string key)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        ArgumentException.ThrowIfNullOrEmpty(key);
        return new OrSetAccessor(lattice, key);
    }

    /// <summary>
    /// Returns a typed accessor for an observed-remove (enable-wins) flag
    /// stored under <paramref name="key"/> in <paramref name="lattice"/>.
    /// The flag tracks presence ("enabled") rather than a value, converging
    /// add-wins under concurrent active-active enable / disable. It is the
    /// minimal observed-remove primitive for composite-key membership rows
    /// (e.g. a tag/key secondary index) where the meaningful bit is the
    /// row's presence.
    /// </summary>
    /// <param name="lattice">The tree containing the flag.</param>
    /// <param name="key">The key the flag is stored under.</param>
    public static OrFlagAccessor OrFlag(this ILattice lattice, string key)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        ArgumentException.ThrowIfNullOrEmpty(key);
        return new OrFlagAccessor(lattice, key);
    }

    /// <summary>
    /// Returns a typed accessor for a remove-wins (disable-wins) flag
    /// stored under <paramref name="key"/> in <paramref name="lattice"/>.
    /// The flag tracks presence ("enabled") rather than a value, converging
    /// remove-wins under concurrent active-active enable / disable: a disable
    /// an enable has not observed survives and keeps the flag off, so a
    /// revoke is never silently resurrected by a concurrent re-add. It is the
    /// remove-wins counterpart of <see cref="OrFlag(ILattice, string)"/> for
    /// composite-key membership rows (e.g. a tag/key secondary index,
    /// revocation list, or blocklist) where a removal must win the tie.
    /// </summary>
    /// <param name="lattice">The tree containing the flag.</param>
    /// <param name="key">The key the flag is stored under.</param>
    public static RwFlagAccessor RwFlag(this ILattice lattice, string key)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        ArgumentException.ThrowIfNullOrEmpty(key);
        return new RwFlagAccessor(lattice, key);
    }

    /// <summary>
    /// Returns a typed accessor for a grow-only (G) set stored under
    /// <paramref name="key"/> in <paramref name="lattice"/>. Elements can only
    /// be added; the set converges by union under concurrent active-active
    /// adds. It is the minimal, tombstone-free set primitive for append-only
    /// workloads (tag sets, seen-ids, accumulating audiences); reach for
    /// <see cref="OrSet(ILattice, string)"/> when removal is needed.
    /// </summary>
    /// <param name="lattice">The tree containing the set.</param>
    /// <param name="key">The key the set is stored under.</param>
    public static GSetAccessor GSet(this ILattice lattice, string key)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        ArgumentException.ThrowIfNullOrEmpty(key);
        return new GSetAccessor(lattice, key);
    }

    /// <summary>
    /// Returns a typed accessor for a remove-wins observed-remove set stored
    /// under <paramref name="key"/> in <paramref name="lattice"/> - the
    /// set-granularity generalisation of <see cref="RwFlag(ILattice, string)"/>.
    /// Concurrent active-active add and remove of the same element converge
    /// remove-wins: a remove an add has not observed survives and keeps the
    /// element out, so a revoke is never silently resurrected by a concurrent
    /// re-add. It is the remove-wins counterpart of the add-wins
    /// <see cref="OrSet(ILattice, string)"/> - the natural primitive for
    /// membership revocation lists and blocklists where a removal must win the
    /// tie.
    /// </summary>
    /// <param name="lattice">The tree containing the set.</param>
    /// <param name="key">The key the set is stored under.</param>
    public static RwSetAccessor RwSet(this ILattice lattice, string key)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        ArgumentException.ThrowIfNullOrEmpty(key);
        return new RwSetAccessor(lattice, key);
    }

    /// <summary>
    /// Returns a typed accessor for a monotone <em>max</em> register stored
    /// under <paramref name="key"/> in <paramref name="lattice"/> - the
    /// high-water-mark primitive that keeps the greatest totally-ordered value
    /// ever written (a monotone gauge, a version ceiling, a max-seen reading).
    /// A write advances the register only when the candidate beats the current
    /// value under the total order; concurrent active-active writes from
    /// different clusters converge on the single greatest value because the fold
    /// is a directional max over the total order - commutative, associative, and
    /// idempotent.
    /// </summary>
    /// <typeparam name="T">The user-facing value type. Serialised to and from <see cref="byte"/>[] through the supplied <paramref name="serializer"/> or <see cref="JsonLatticeSerializer{T}"/>.</typeparam>
    /// <param name="lattice">The tree containing the register.</param>
    /// <param name="key">The key the register is stored under.</param>
    /// <param name="orderKeySelector">
    /// Produces the order-preserving total-order key for a value. The key is
    /// carried on the wire alongside the value so the receiver folds without the
    /// domain comparer; the caller is responsible for authoring a key whose
    /// unsigned lexicographic byte order matches the intended value order (e.g.
    /// big-endian encoding of a numeric height).
    /// </param>
    /// <param name="serializer">Optional serializer for <typeparamref name="T"/>. Defaults to <see cref="JsonLatticeSerializer{T}.Default"/>.</param>
    public static MaxRegisterAccessor<T> MaxRegister<T>(this ILattice lattice, string key, Func<T, byte[]> orderKeySelector, ILatticeSerializer<T>? serializer = null)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(orderKeySelector);
        return new MaxRegisterAccessor<T>(lattice, key, orderKeySelector, serializer ?? JsonLatticeSerializer<T>.Default);
    }

    /// <summary>
    /// Returns a typed accessor for a monotone <em>min</em> register stored
    /// under <paramref name="key"/> in <paramref name="lattice"/> - the
    /// low-water-mark primitive that keeps the smallest totally-ordered value
    /// ever written (a min-seen latency floor, a first-seen timestamp). A write
    /// advances the register only when the candidate beats the current value
    /// under the total order; concurrent active-active writes from different
    /// clusters converge on the single smallest value because the fold is a
    /// directional min over the total order - commutative, associative, and
    /// idempotent.
    /// </summary>
    /// <typeparam name="T">The user-facing value type. Serialised to and from <see cref="byte"/>[] through the supplied <paramref name="serializer"/> or <see cref="JsonLatticeSerializer{T}"/>.</typeparam>
    /// <param name="lattice">The tree containing the register.</param>
    /// <param name="key">The key the register is stored under.</param>
    /// <param name="orderKeySelector">
    /// Produces the order-preserving total-order key for a value. The key is
    /// carried on the wire alongside the value so the receiver folds without the
    /// domain comparer; the caller is responsible for authoring a key whose
    /// unsigned lexicographic byte order matches the intended value order (e.g.
    /// big-endian encoding of a numeric reading).
    /// </param>
    /// <param name="serializer">Optional serializer for <typeparamref name="T"/>. Defaults to <see cref="JsonLatticeSerializer{T}.Default"/>.</param>
    public static MinRegisterAccessor<T> MinRegister<T>(this ILattice lattice, string key, Func<T, byte[]> orderKeySelector, ILatticeSerializer<T>? serializer = null)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(orderKeySelector);
        return new MinRegisterAccessor<T>(lattice, key, orderKeySelector, serializer ?? JsonLatticeSerializer<T>.Default);
    }

    /// <summary>
    /// Returns a typed accessor for a positive-negative (PN) counter
    /// stored under <paramref name="key"/> in <paramref name="lattice"/>.
    /// </summary>
    /// <param name="lattice">The tree containing the counter.</param>
    /// <param name="key">The key the counter is stored under.</param>
    public static PnCounterAccessor PnCounter(this ILattice lattice, string key)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        ArgumentException.ThrowIfNullOrEmpty(key);
        return new PnCounterAccessor(lattice, key);
    }

    /// <summary>
    /// Returns a typed accessor for a grow-only (G) counter stored under
    /// <paramref name="key"/> in <paramref name="lattice"/>. The counter only
    /// ever increments and converges by pointwise-max per replica, so
    /// concurrent active-active increments from multiple clusters all count.
    /// It is the monotonic-only counter that
    /// <see cref="PnCounter(ILattice, string)"/> is built from - the natural
    /// choice for monotone metrics, sequence / event counters, and quota
    /// consumption where decrement never happens.
    /// </summary>
    /// <param name="lattice">The tree containing the counter.</param>
    /// <param name="key">The key the counter is stored under.</param>
    public static GCounterAccessor GCounter(this ILattice lattice, string key)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        ArgumentException.ThrowIfNullOrEmpty(key);
        return new GCounterAccessor(lattice, key);
    }

    /// <summary>
    /// Returns a typed accessor for a version vector stored under
    /// <paramref name="key"/> in <paramref name="lattice"/>.
    /// </summary>
    /// <param name="lattice">The tree containing the vector.</param>
    /// <param name="key">The key the vector is stored under.</param>
    public static VersionVectorAccessor VersionVector(this ILattice lattice, string key)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        ArgumentException.ThrowIfNullOrEmpty(key);
        return new VersionVectorAccessor(lattice, key);
    }

    /// <summary>
    /// Returns a typed accessor for a multi-value register stored under
    /// <paramref name="key"/> in <paramref name="lattice"/>. Concurrent
    /// writes from different replicas survive the merge as distinct
    /// dot-tagged values rather than the wire contract silently dropping
    /// one side as last-writer-wins would; the application reads the
    /// conflict set via
    /// <see cref="MvRegisterAccessor{T}.ValuesAsync(CancellationToken)"/>
    /// and resolves it itself.
    /// </summary>
    /// <typeparam name="T">The user-facing value type. Serialised to and from <see cref="byte"/>[] through the supplied <paramref name="serializer"/> or <see cref="JsonLatticeSerializer{T}"/>.</typeparam>
    /// <param name="lattice">The tree containing the register.</param>
    /// <param name="key">The key the register is stored under.</param>
    /// <param name="serializer">Optional serializer for <typeparamref name="T"/>. Defaults to <see cref="JsonLatticeSerializer{T}.Default"/>.</param>
    public static MvRegisterAccessor<T> MvRegister<T>(this ILattice lattice, string key, ILatticeSerializer<T>? serializer = null)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        ArgumentException.ThrowIfNullOrEmpty(key);
        return new MvRegisterAccessor<T>(lattice, key, serializer ?? JsonLatticeSerializer<T>.Default);
    }

    /// <summary>
    /// Returns a typed accessor for an observed-remove map stored
    /// under <paramref name="key"/> in <paramref name="lattice"/>.
    /// Keys inside the map follow add-wins observed-remove semantics
    /// and per-key values are folded through
    /// <see cref="Orleans.Lattice.ICrdt{TSelf}.MergeFrom(TSelf)"/> rather
    /// than collapsed by last-writer-wins, so concurrent writes from
    /// different replicas under the same map key converge into a
    /// single recursively-merged value.
    /// </summary>
    /// <typeparam name="TKey">
    /// The map key type. Must support reasonable dictionary equality
    /// (e.g. <see cref="string"/>, <see cref="int"/>, <see cref="Guid"/>).
    /// </typeparam>
    /// <typeparam name="TValue">
    /// The recursively-mergeable value CRDT, constrained by
    /// <see cref="Orleans.Lattice.ICrdt{TSelf}"/> with a public parameterless
    /// constructor. Use any of the existing primitives
    /// (<see cref="Orleans.Lattice.OrSet"/>,
    /// <see cref="Orleans.Lattice.PnCounter"/>,
    /// <see cref="Orleans.Lattice.VersionVector"/>,
    /// <see cref="Orleans.Lattice.MvRegister"/>) or a custom
    /// <see cref="Orleans.Lattice.ICrdt{TSelf}"/>-implementing type.
    /// </typeparam>
    /// <param name="lattice">The tree containing the map.</param>
    /// <param name="key">The key the map is stored under.</param>
    public static OrMapAccessor<TKey, TValue> OrMap<TKey, TValue>(this ILattice lattice, string key)
        where TKey : notnull
        where TValue : Orleans.Lattice.ICrdt<TValue>, new()
    {
        ArgumentNullException.ThrowIfNull(lattice);
        ArgumentException.ThrowIfNullOrEmpty(key);
        return new OrMapAccessor<TKey, TValue>(lattice, key);
    }

    /// <summary>
    /// Returns a typed accessor for a Replicated Growable Array
    /// (RGA) sequence stored under <paramref name="key"/> in
    /// <paramref name="lattice"/>. Concurrent inserts under the same
    /// parent converge on a deterministic order via the standard RGA
    /// descending <c>(Counter, ReplicaId)</c> tie-break, and removes
    /// tombstone the targeted node so a later re-insert against the
    /// same parent still resolves correctly. The dominant use case is
    /// collaborative editing of an ordered list or text buffer;
    /// pairing this with mutation observers gives a real-time
    /// list / text channel out of the box.
    /// </summary>
    /// <typeparam name="T">The user-facing element type. Serialised to and from <see cref="byte"/>[] through the supplied <paramref name="serializer"/> or <see cref="JsonLatticeSerializer{T}"/>.</typeparam>
    /// <param name="lattice">The tree containing the sequence.</param>
    /// <param name="key">The key the sequence is stored under.</param>
    /// <param name="serializer">Optional serializer for <typeparamref name="T"/>. Defaults to <see cref="JsonLatticeSerializer{T}.Default"/>.</param>
    public static RgaAccessor<T> Sequence<T>(this ILattice lattice, string key, ILatticeSerializer<T>? serializer = null)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        ArgumentException.ThrowIfNullOrEmpty(key);
        return new RgaAccessor<T>(lattice, key, serializer ?? JsonLatticeSerializer<T>.Default);
    }
}
