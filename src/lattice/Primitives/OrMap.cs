namespace Orleans.Lattice;

using System.Runtime.InteropServices;

/// <summary>
/// An observed-remove (OR) map CRDT keyed by <typeparamref name="TKey"/>
/// whose values are themselves recursively-mergeable CRDTs constrained
/// by <see cref="ICrdt{TSelf}"/>. Keys follow add-wins semantics (every
/// <see cref="Set(TKey, string, TValue)"/> mints a fresh causal dot; a
/// <see cref="Remove(TKey)"/> tombstones only the dots it observed, so
/// a concurrent <see cref="Set(TKey, string, TValue)"/> on another
/// replica survives the merge), and per-key values are folded through
/// <see cref="ICrdt{TSelf}.MergeFrom(TSelf)"/> rather than collapsed by
/// last-writer-wins.
/// <para>
/// State shape: <see cref="Adds"/> stores one
/// <see cref="OrMapEntry{TValue}"/> per dot per key (dots are unique by
/// <c>(<see cref="OrSetDot.ReplicaId"/>, <see cref="OrSetDot.Counter"/>)</c>),
/// <see cref="Tombstones"/> stores observed-removed dots per key. The
/// "current" value at a key is the lattice-merge of every live
/// (un-tombstoned) entry's <see cref="OrMapEntry{TValue}.Value"/>;
/// concurrent writes from different replicas under the same key
/// converge by recursing into the value CRDT's own
/// <see cref="ICrdt{TSelf}.MergeFrom(TSelf)"/> rather than discarding
/// all but one. <see cref="Merge(OrMap{TKey, TValue}, OrMap{TKey, TValue})"/>
/// is commutative, associative, and idempotent.
/// </para>
/// </summary>
/// <typeparam name="TKey">
/// The key type. Must support reasonable dictionary equality (e.g.
/// <see cref="string"/>, <see cref="int"/>, <see cref="Guid"/>); the
/// map uses the type's default <see cref="EqualityComparer{T}"/>.
/// </typeparam>
/// <typeparam name="TValue">
/// The value CRDT. Must implement <see cref="ICrdt{TSelf}"/> with a
/// public parameterless constructor so the map can synthesise an
/// identity element when materialising the merged value at a key.
/// </typeparam>
[GenerateSerializer]
[Alias(TypeAliases.OrMap)]
public sealed class OrMap<TKey, TValue> : ICrdt<OrMap<TKey, TValue>>
    where TKey : notnull
    where TValue : ICrdt<TValue>, new()
{
    /// <summary>
    /// Per-key list of dot-tagged value snapshots. An entry is live
    /// (and therefore contributes to the merged value at its key) iff
    /// its dot is not present in <see cref="Tombstones"/> for the same
    /// key.
    /// </summary>
    [Id(0)]
    public Dictionary<TKey, List<OrMapEntry<TValue>>> Adds { get; set; }

    /// <summary>
    /// Per-key observed-removed dots. A dot in this map cancels the
    /// matching <see cref="OrMapEntry{TValue}"/> in <see cref="Adds"/>
    /// on merge.
    /// </summary>
    [Id(1)]
    public Dictionary<TKey, List<OrSetDot>> Tombstones { get; set; }

    /// <summary>
    /// Dot context: per-replica highest counter ever minted or observed
    /// for that replica across every dot in the map (live or tombstoned).
    /// Lets <c>NextCounter</c> mint a fresh dot in O(1) instead of
    /// rescanning every dot on every <see cref="Set(TKey, string, TValue)"/>.
    /// <para>
    /// This is a serialized cache, not a semantic witness: it never
    /// influences the lattice merge (which unions dots directly), only the
    /// counter chosen for the next local write. It is kept consistent by
    /// every mutator (<see cref="Set(TKey, string, TValue)"/>,
    /// <see cref="MergeFrom(OrMap{TKey, TValue})"/>,
    /// <see cref="MergeDelta(OrMapDelta{TKey, TValue})"/>,
    /// <see cref="Clone"/>) and is rebuilt lazily from the dots on the
    /// first write after loading a legacy payload that predates this field
    /// (older payloads deserialize it as empty, which is backward
    /// compatible).
    /// </para>
    /// </summary>
    [Id(2)]
    public Dictionary<string, long> Context { get; set; }

    /// <summary>Creates an empty observed-remove map.</summary>
    public OrMap()
    {
        Adds = new();
        Tombstones = new();
        Context = [];
    }

    // Direct-assign constructor for the clone fast path: takes ownership of
    // already-built backing stores so the clone allocates no discarded
    // empty-collection shells from field initializers that an object
    // initializer would immediately overwrite. Mirrors OrSet.
    private OrMap(
        Dictionary<TKey, List<OrMapEntry<TValue>>> adds,
        Dictionary<TKey, List<OrSetDot>> tombstones,
        Dictionary<string, long> context)
    {
        Adds = adds;
        Tombstones = tombstones;
        Context = context;
    }

    /// <summary>Returns <c>true</c> when no key has any live (un-tombstoned) dot.</summary>
    public bool IsEmpty
    {
        get
        {
            // No key has ever been removed: every stored entry is live, so the
            // emptiness check reduces to "does any key hold an entry" without a
            // per-key tombstone probe. Append-only maps stay in this branch for
            // their whole lifetime.
            if (Tombstones.Count == 0)
            {
                foreach (var entries in Adds.Values)
                {
                    if (entries.Count > 0) return false;
                }
                return true;
            }
            foreach (var (key, entries) in Adds)
            {
                if (LiveEntryCount(key, entries) > 0) return false;
            }
            return true;
        }
    }

    /// <inheritdoc />
    /// <remarks>
    /// An <see cref="OrMap{TKey, TValue}"/> is bottom when no key has
    /// any live dot. Tombstones may still be present and are preserved
    /// for causal-history purposes.
    /// </remarks>
    public bool IsBottom => IsEmpty;

    /// <summary>Returns the number of keys with at least one live entry.</summary>
    public int Count
    {
        get
        {
            var n = 0;
            // No key has ever been removed: every non-empty key is live, so
            // count them without a per-key tombstone probe.
            if (Tombstones.Count == 0)
            {
                foreach (var entries in Adds.Values)
                {
                    if (entries.Count > 0) n++;
                }
                return n;
            }
            foreach (var (key, entries) in Adds)
            {
                if (LiveEntryCount(key, entries) > 0) n++;
            }
            return n;
        }
    }

    /// <summary>
    /// Writes <paramref name="value"/> at <paramref name="key"/> from
    /// <paramref name="replicaId"/>, minting a fresh dot
    /// <c>(<paramref name="replicaId"/>, <c>NextCounter</c>)</c> where
    /// <c>NextCounter</c> is one greater than the highest counter
    /// observed for <paramref name="replicaId"/> across every dot in
    /// the map (live or tombstoned), read in O(1) from
    /// <see cref="Context"/>. The previous value snapshots at
    /// the same key are not removed - the next merge folds every live
    /// entry's <see cref="OrMapEntry{TValue}.Value"/> through the
    /// value CRDT's <see cref="ICrdt{TSelf}.MergeFrom(TSelf)"/>.
    /// </summary>
    /// <param name="key">The key to write under. Must not be <c>null</c>.</param>
    /// <param name="replicaId">The replica authoring the write. Must be non-empty.</param>
    /// <param name="value">The CRDT value snapshot to attach to the new dot. Must not be <c>null</c>.</param>
    public void Set(TKey key, string replicaId, TValue value)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        ArgumentNullException.ThrowIfNull(value);

        var counter = NextCounter(replicaId);
        if (!Adds.TryGetValue(key, out var entries))
        {
            entries = new List<OrMapEntry<TValue>>(1);
            Adds[key] = entries;
        }
        entries.Add(new OrMapEntry<TValue>(replicaId, counter, value));

        // counter is strictly greater than any prior counter for this
        // replica, so record it as the new per-replica maximum.
        Context[replicaId] = counter;
    }

    /// <summary>
    /// Removes <paramref name="key"/> by tombstoning every dot
    /// currently observed for it. Concurrent writes on other replicas
    /// (with dots not yet observed locally) survive a later merge
    /// because their dots are not tombstoned here - the add-wins
    /// variant. Returns <c>true</c> when at least one dot was newly
    /// tombstoned.
    /// </summary>
    /// <param name="key">The key to remove. Must not be <c>null</c>.</param>
    public bool Remove(TKey key)
    {
        ArgumentNullException.ThrowIfNull(key);
        if (!Adds.TryGetValue(key, out var entries) || entries.Count == 0) return false;

        if (!Tombstones.TryGetValue(key, out var tomb))
        {
            // First-time tombstoning of this key: every live dot is
            // brand-new to the tombstone list, so we can copy them
            // straight in without a per-dot dedup pass.
            tomb = new List<OrSetDot>(entries.Count);
            foreach (var entry in entries)
            {
                tomb.Add(new OrSetDot { ReplicaId = entry.ReplicaId, Counter = entry.Counter });
            }
            Tombstones[key] = tomb;
            return tomb.Count > 0;
        }

        // Existing tombstones: dedup against the current list. For
        // small lists (the common case) a linear scan avoids the
        // HashSet allocation entirely; only fall back to a hash set
        // when the lists are large enough that O(n*m) becomes a real
        // cost.
        var anyAdded = false;
        if (tomb.Count <= LinearDedupThreshold)
        {
            foreach (var entry in entries)
            {
                var dot = new OrSetDot { ReplicaId = entry.ReplicaId, Counter = entry.Counter };
                if (!ListContainsDot(tomb, dot))
                {
                    tomb.Add(dot);
                    anyAdded = true;
                }
            }
            return anyAdded;
        }

        var tombSet = new HashSet<OrSetDot>(tomb.Count + entries.Count);
        foreach (var d in tomb) tombSet.Add(d);
        foreach (var entry in entries)
        {
            var dot = new OrSetDot { ReplicaId = entry.ReplicaId, Counter = entry.Counter };
            if (tombSet.Add(dot))
            {
                tomb.Add(dot);
                anyAdded = true;
            }
        }
        return anyAdded;
    }

    /// <summary>Linear-vs-hash crossover for tombstone dedup. Below this size, a list scan is cheaper than allocating a hash set.</summary>
    private const int LinearDedupThreshold = 16;

    private static bool ListContainsDot(List<OrSetDot> list, OrSetDot dot)
    {
        for (var i = 0; i < list.Count; i++)
        {
            if (list[i].Counter == dot.Counter && string.Equals(list[i].ReplicaId, dot.ReplicaId, StringComparison.Ordinal)) return true;
        }
        return false;
    }

    /// <summary>
    /// Returns <c>true</c> when <paramref name="key"/> has at least
    /// one live (un-tombstoned) dot.
    /// </summary>
    public bool ContainsKey(TKey key)
    {
        ArgumentNullException.ThrowIfNull(key);
        if (!Adds.TryGetValue(key, out var entries)) return false;
        // No key has ever been removed: the stored entries are all live, so
        // skip the per-key tombstone probe entirely.
        if (Tombstones.Count == 0) return entries.Count > 0;
        return LiveEntryCount(key, entries) > 0;
    }

    /// <summary>
    /// Returns the lattice-merged value at <paramref name="key"/>, or
    /// <c>null</c> when the key is absent or every observed dot for it
    /// has been tombstoned. The returned instance is a fresh
    /// <typeparamref name="TValue"/> built by folding every live
    /// entry's <see cref="OrMapEntry{TValue}.Value"/> through
    /// <see cref="ICrdt{TSelf}.MergeFrom(TSelf)"/>; mutating it does
    /// not change the map.
    /// </summary>
    public TValue? Get(TKey key)
    {
        ArgumentNullException.ThrowIfNull(key);
        if (!Adds.TryGetValue(key, out var entries) || entries.Count == 0) return default;

        // Skip the hash-set allocation entirely when this key has no
        // tombstones (the common case). For small tombstone lists a
        // linear scan is also cheaper than a HashSet alloc.
        Tombstones.TryGetValue(key, out var tomb);
        var tombCount = tomb is null ? 0 : tomb.Count;

        TValue? merged = default;
        if (tombCount == 0)
        {
            // Steady-state fast path: a key with exactly one live entry and no
            // tombstones is the dominant case. Return a defensive clone of the
            // sole value instead of allocating an identity TValue and folding a
            // full MergeFrom (which, for dictionary-backed value CRDTs, walks
            // and copies the whole value state). Clone preserves the contract
            // that the caller receives an instance it may mutate freely.
            if (entries.Count == 1) return entries[0].Value.Clone();

            foreach (var entry in entries)
            {
                if (merged is null) merged = new TValue();
                merged.MergeFrom(entry.Value);
            }
            return merged;
        }

        if (tombCount <= LinearDedupThreshold)
        {
            foreach (var entry in entries)
            {
                var dot = new OrSetDot { ReplicaId = entry.ReplicaId, Counter = entry.Counter };
                if (ListContainsDot(tomb!, dot)) continue;
                if (merged is null) merged = new TValue();
                merged.MergeFrom(entry.Value);
            }
            return merged;
        }

        var tombSet = new HashSet<OrSetDot>(tombCount);
        foreach (var d in tomb!) tombSet.Add(d);
        foreach (var entry in entries)
        {
            if (tombSet.Contains(new OrSetDot { ReplicaId = entry.ReplicaId, Counter = entry.Counter })) continue;
            if (merged is null) merged = new TValue();
            merged.MergeFrom(entry.Value);
        }
        return merged;
    }

    /// <summary>
    /// Enumerates every live key in the map in deterministic order
    /// (the default ordering for <typeparamref name="TKey"/> via
    /// <see cref="Comparer{T}"/>).
    /// </summary>
    public IEnumerable<TKey> Keys()
    {
        if (Adds.Count == 0) return Array.Empty<TKey>();

        var noTombstones = Tombstones.Count == 0;
        var live = new List<TKey>(Adds.Count);
        foreach (var (key, entries) in Adds)
        {
            // No removes anywhere: every non-empty key is live, so skip the
            // per-key tombstone probe.
            if (noTombstones)
            {
                if (entries.Count > 0) live.Add(key);
                continue;
            }
            if (LiveEntryCount(key, entries) > 0) live.Add(key);
        }
        if (live.Count == 0) return Array.Empty<TKey>();
        live.Sort(Comparer<TKey>.Default);
        return live;
    }

    /// <summary>
    /// Lattice merge: pointwise union of <see cref="Adds"/> (per key,
    /// per dot) and <see cref="Tombstones"/>; surviving dots' values
    /// are folded through <see cref="ICrdt{TSelf}.MergeFrom(TSelf)"/>.
    /// Commutative, associative, idempotent.
    /// </summary>
    public static OrMap<TKey, TValue> Merge(OrMap<TKey, TValue> left, OrMap<TKey, TValue> right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        var result = left.Clone();
        result.MergeFrom(right);
        return result;
    }

    /// <inheritdoc />
    public void MergeFrom(OrMap<TKey, TValue> other)
    {
        ArgumentNullException.ThrowIfNull(other);

        // Capture this map's own per-replica maxima before merging so a
        // legacy self (empty Context, non-empty dots) is not left with a
        // Context that reflects only the incoming side.
        EnsureContextRebuilt();

        // Tombstones first so the per-dot dedup in the Adds pass below
        // sees every observed-removed dot before it folds value
        // snapshots.
        foreach (var (key, dots) in other.Tombstones)
        {
            if (dots.Count == 0) continue;
            if (!Tombstones.TryGetValue(key, out var existing))
            {
                Tombstones[key] = new List<OrSetDot>(dots);
                continue;
            }

            // Small list: linear dedup avoids the HashSet alloc.
            if (existing.Count + dots.Count <= LinearDedupThreshold)
            {
                foreach (var d in dots)
                {
                    if (!ListContainsDot(existing, d)) existing.Add(d);
                }
                continue;
            }

            var seen = new HashSet<OrSetDot>(existing.Count + dots.Count);
            foreach (var d in existing) seen.Add(d);
            foreach (var d in dots)
            {
                if (seen.Add(d)) existing.Add(d);
            }
        }

        foreach (var (key, otherEntries) in other.Adds)
        {
            if (otherEntries.Count == 0) continue;

            if (!Adds.TryGetValue(key, out var localEntries))
            {
                // Copy-by-reference is intentional: callers that need
                // structural isolation use Clone().
                Adds[key] = new List<OrMapEntry<TValue>>(otherEntries);
                continue;
            }

            // Small lists: linear scan avoids the per-key dictionary
            // allocation. The crossover threshold matches the
            // tombstone dedup heuristic.
            if (localEntries.Count + otherEntries.Count <= LinearDedupThreshold)
            {
                foreach (var e in otherEntries)
                {
                    var existing = FindByDot(localEntries, e.ReplicaId, e.Counter);
                    if (existing is not null)
                    {
                        // Same dot from both sides: lattice-merge the
                        // value snapshots so the result is
                        // deterministic even if a transport or out-of-
                        // band path produced divergent values under
                        // the same author dot.
                        existing.Value.MergeFrom(e.Value);
                    }
                    else
                    {
                        localEntries.Add(e);
                    }
                }
                continue;
            }

            // Large lists: index local by dot to make same-dot
            // collision detection O(1).
            var byDot = new Dictionary<OrSetDot, OrMapEntry<TValue>>(localEntries.Count);
            foreach (var e in localEntries)
            {
                byDot[new OrSetDot { ReplicaId = e.ReplicaId, Counter = e.Counter }] = e;
            }
            foreach (var e in otherEntries)
            {
                var dot = new OrSetDot { ReplicaId = e.ReplicaId, Counter = e.Counter };
                if (byDot.TryGetValue(dot, out var existing))
                {
                    existing.Value.MergeFrom(e.Value);
                }
                else
                {
                    localEntries.Add(e);
                    byDot[dot] = e;
                }
            }
        }

        MergeContextFrom(other);
    }

    private static OrMapEntry<TValue>? FindByDot(List<OrMapEntry<TValue>> entries, string replicaId, long counter)
    {
        for (var i = 0; i < entries.Count; i++)
        {
            var e = entries[i];
            if (e.Counter == counter && string.Equals(e.ReplicaId, replicaId, StringComparison.Ordinal)) return e;
        }
        return null;
    }

    /// <summary>
    /// Folds an <see cref="OrMapDelta{TKey, TValue}"/> into this map.
    /// The merge is equivalent to constructing a transient
    /// <see cref="OrMap{TKey, TValue}"/> from
    /// <see cref="OrMapDelta{TKey, TValue}.Adds"/> and
    /// <see cref="OrMapDelta{TKey, TValue}.Tombstones"/> and calling
    /// <see cref="MergeFrom(OrMap{TKey, TValue})"/>; commutative,
    /// associative, and idempotent against arrival order and duplicate
    /// delivery.
    /// </summary>
    /// <param name="delta">
    /// The typed CRDT delta authored by the producing call site. Null
    /// inner collections are treated as empty.
    /// </param>
    public void MergeDelta(OrMapDelta<TKey, TValue> delta)
    {
        // Keep the counter cache dominating every dot, including on a
        // legacy map whose Context is still empty on first delta apply.
        EnsureContextRebuilt();

        var adds = delta.Adds;
        if (adds is { Count: > 0 })
        {
            foreach (var add in adds)
            {
                BumpContext(add.ReplicaId, add.Counter);
                if (!Adds.TryGetValue(add.Key, out var entries))
                {
                    entries = new List<OrMapEntry<TValue>>(1);
                    Adds[add.Key] = entries;
                }

                var existing = FindByDot(entries, add.ReplicaId, add.Counter);
                if (existing is not null)
                {
                    // Same dot from both sides: lattice-merge the value
                    // snapshots so the result is deterministic even if
                    // a transport or out-of-band path produced
                    // divergent values under the same author dot.
                    existing.Value.MergeFrom(add.Value);
                }
                else
                {
                    entries.Add(new OrMapEntry<TValue>(add.ReplicaId, add.Counter, add.Value));
                }
            }
        }

        var tombs = delta.Tombstones;
        if (tombs is { Count: > 0 })
        {
            foreach (var t in tombs)
            {
                BumpContext(t.ReplicaId, t.Counter);
                if (!Tombstones.TryGetValue(t.Key, out var existing))
                {
                    existing = new List<OrSetDot>(1);
                    Tombstones[t.Key] = existing;
                }
                var dot = new OrSetDot { ReplicaId = t.ReplicaId, Counter = t.Counter };
                if (!ListContainsDot(existing, dot)) existing.Add(dot);
            }
        }
    }

    /// <summary>Creates a deep, independent copy of this map: every per-key list is duplicated and each entry's nested value snapshot is itself cloned, so mutating the copy (or folding another state into it) never affects the receiver, per the <see cref="ICrdt{TSelf}.Clone"/> contract.</summary>
    public OrMap<TKey, TValue> Clone()
    {
        // Presize both backing dictionaries to the source key counts so the
        // entry-by-entry fill below never triggers an intermediate rehash
        // grow. Mirrors the VersionVector.Clone presize.
        var adds = new Dictionary<TKey, List<OrMapEntry<TValue>>>(Adds.Count);
        foreach (var (key, entries) in Adds)
        {
            // Deep-copy each dot entry AND its nested value CRDT. MergeFrom
            // folds a same-dot value collision in place via
            // existing.Value.MergeFrom(other.Value), so sharing the entry
            // objects or their nested values with the clone source would let a
            // later merge mutate the source - breaking the purity of the
            // static Merge (which is Clone().MergeFrom(...)) and the
            // ICrdt.Clone contract that "mutating the returned value must never
            // affect the receiver." Mirrors Rga.Clone's per-node deep copy.
            var copy = new List<OrMapEntry<TValue>>(entries.Count);
            foreach (var entry in entries)
            {
                copy.Add(new OrMapEntry<TValue>(entry.ReplicaId, entry.Counter, entry.Value.Clone()));
            }
            adds[key] = copy;
        }
        var tombstones = new Dictionary<TKey, List<OrSetDot>>(Tombstones.Count);
        foreach (var (key, dots) in Tombstones)
        {
            tombstones[key] = new List<OrSetDot>(dots);
        }
        // Copy the dot-context through its own comparer so the Dictionary
        // copy constructor bulk-copies the backing store instead of
        // rehashing every replica key. A fresh StringComparer.Ordinal is
        // ordinally identical but reference-distinct from the source
        // comparer, defeating that fast path. Mirrors Rga.Clone.
        var context = new Dictionary<string, long>(Context, Context.Comparer);
        // The direct-assign constructor takes the filled stores as-is, so the
        // clone allocates no discarded empty-collection shells.
        return new OrMap<TKey, TValue>(adds, tombstones, context);
    }

    private long NextCounter(string replicaId)
    {
        EnsureContextRebuilt();
        return (Context.TryGetValue(replicaId, out var current) ? current : 0) + 1;
    }

    /// <summary>
    /// Rebuilds <see cref="Context"/> from the dots the first time it is
    /// needed on a map loaded from a legacy payload that predates the
    /// field (deserialized with an empty <see cref="Context"/> but
    /// non-empty dots). A no-op once the cache is populated - every
    /// mutator keeps it consistent from then on - and a no-op on a
    /// genuinely empty map. O(total dots) exactly once per legacy load.
    /// </summary>
    private void EnsureContextRebuilt()
    {
        if (Context.Count > 0) return;
        if (Adds.Count == 0 && Tombstones.Count == 0) return;

        foreach (var entries in Adds.Values)
        {
            foreach (var e in entries) BumpContext(e.ReplicaId, e.Counter);
        }
        foreach (var dots in Tombstones.Values)
        {
            foreach (var d in dots) BumpContext(d.ReplicaId, d.Counter);
        }
    }

    private void BumpContext(string replicaId, long counter)
    {
        // Single-probe pointwise-max: hash replicaId once and bump the slot
        // only when the incoming counter is strictly greater. A missing slot
        // is added zero-initialised, so the !existed branch installs counter -
        // identical to the previous TryGetValue-then-indexer form with one
        // fewer hash and bucket walk. Mirrors VersionVector.Merge's fold.
        ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(Context, replicaId, out var existed);
        if (!existed || counter > slot) slot = counter;
    }

    /// <summary>
    /// Folds <paramref name="other"/>'s per-replica maxima into this
    /// map's <see cref="Context"/> so the cache still dominates every dot
    /// after a merge. New payloads carry a maintained <see cref="Context"/>
    /// (pointwise-max fold); a legacy <paramref name="other"/> with an
    /// empty context but non-empty dots is folded directly from its dots
    /// without mutating it.
    /// </summary>
    private void MergeContextFrom(OrMap<TKey, TValue> other)
    {
        foreach (var (replicaId, counter) in other.Context) BumpContext(replicaId, counter);

        if (other.Context.Count == 0 && (other.Adds.Count > 0 || other.Tombstones.Count > 0))
        {
            foreach (var entries in other.Adds.Values)
            {
                foreach (var e in entries) BumpContext(e.ReplicaId, e.Counter);
            }
            foreach (var dots in other.Tombstones.Values)
            {
                foreach (var d in dots) BumpContext(d.ReplicaId, d.Counter);
            }
        }
    }

    private int LiveEntryCount(TKey key, List<OrMapEntry<TValue>> entries)
    {
        if (!Tombstones.TryGetValue(key, out var tomb) || tomb.Count == 0) return entries.Count;

        if (tomb.Count <= LinearDedupThreshold || entries.Count <= LinearDedupThreshold)
        {
            // Tiny tombstone list, or few live entries: a linear membership
            // scan beats allocating a HashSet. When the live-entry list is the
            // small side - the common case for a churned key that carries a
            // long observed-remove history but only a handful of live entries -
            // the scan is O(entries.Count * tomb.Count), bounded by
            // O(LinearDedupThreshold * tomb.Count): the same asymptotic as
            // building the tomb-sized set, but allocation-free. The prior guard
            // checked only the tombstone count and so built the HashSet on
            // every read (IsEmpty / Count / Contains / Keys) of a
            // heavily-tombstoned key even for a single live entry.
            var n = 0;
            foreach (var e in entries)
            {
                var dot = new OrSetDot { ReplicaId = e.ReplicaId, Counter = e.Counter };
                if (!ListContainsDot(tomb, dot)) n++;
            }
            return n;
        }

        var tombSet = new HashSet<OrSetDot>(tomb.Count);
        foreach (var d in tomb) tombSet.Add(d);
        var live = 0;
        foreach (var e in entries)
        {
            if (!tombSet.Contains(new OrSetDot { ReplicaId = e.ReplicaId, Counter = e.Counter })) live++;
        }
        return live;
    }
}
