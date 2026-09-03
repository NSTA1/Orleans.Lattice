namespace Orleans.Lattice;

using System.Runtime.InteropServices;

/// <summary>
/// A multi-value register CRDT. Unlike a last-writer-wins register,
/// which silently collapses concurrent writes from different replicas
/// to whichever wall-clock timestamp happens to be larger, an
/// <see cref="MvRegister"/> preserves every concurrent write as a
/// distinct dot-tagged value so application code can resolve the
/// merge itself (e.g. show the user the conflicting candidates).
/// <para>
/// State shape: a dot-tagged set of live <c>Entries</c> plus a
/// <see cref="Context"/> mapping each replica id to the highest
/// counter ever observed for that replica. A write on
/// <c>replicaId</c> via <see cref="Set(string, byte[])"/>
/// mints a fresh dot <c>(replicaId, Context[replicaId] + 1)</c>, drops
/// every entry the writer has observed (every entry whose dot is
/// dominated by the new <see cref="Context"/>), and records the new
/// dot+value. The merge keeps a side's entry iff its dot is not
/// dominated by the other side's <see cref="Context"/>; concurrent
/// writes whose dot contexts do not dominate each other therefore
/// survive together.
/// </para>
/// <para>
/// Values are opaque <see cref="byte"/> arrays; the typed
/// <see cref="MvRegisterAccessor{T}"/> serialises domain values through
/// an injectable <see cref="ILatticeSerializer{T}"/>.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.MvRegister)]
public sealed class MvRegister : ICrdt<MvRegister>
{
    /// <summary>
    /// The currently-live dot-tagged values. An <see cref="MvRegister"/>
    /// is single-valued in the steady state (one entry); concurrent
    /// writes from different replicas that did not observe each other
    /// produce a transient multi-valued state until a future write
    /// dominates them.
    /// </summary>
    [Id(0)]
    public List<MvRegisterEntry> Entries { get; set; }

    /// <summary>
    /// Dot context: per-replica highest-observed counter. Acts as the
    /// dominance witness for the merge - an entry whose dot is
    /// dominated by the other side's context has been observed-and-
    /// superseded and must not be re-introduced on merge.
    /// </summary>
    [Id(1)]
    public Dictionary<string, long> Context { get; set; }

    /// <summary>
    /// Cached materialised snapshot of the single-valued read produced by
    /// <see cref="ValuesShared"/>. A steady-state (single-valued) register can
    /// hand the same one-element list to repeated internal reads between writes
    /// instead of allocating a fresh array each call. Reset to <c>null</c> by
    /// every mutation that can change the live entry (<see cref="Set"/>,
    /// <see cref="MergeFrom"/>, <see cref="MergeDelta"/>). Not serialised
    /// (no <c>[Id]</c>): it is derived state that rebuilds lazily on the
    /// next read after a copy or deserialize.
    /// <para>
    /// The cached arrays alias the live <see cref="Entries"/> buffers, which is
    /// why the cache backs the internal <see cref="ValuesShared"/> view and never
    /// the public <see cref="Values"/> projection - see the buffer-ownership
    /// remarks on <see cref="ICrdt{TSelf}"/>.
    /// </para>
    /// </summary>
    [NonSerialized]
    private IReadOnlyList<byte[]>? _singleValueSnapshot;

    /// <summary>Creates an empty multi-value register.</summary>
    public MvRegister()
    {
        Entries = [];
        Context = [];
    }

    // Direct-assign constructor for the clone fast path: takes ownership of
    // already-built backing stores so the clone allocates no discarded
    // empty-collection shells from field initializers that an object
    // initializer would immediately overwrite. The derived single-value
    // snapshot is left null to rebuild lazily on the next read. Mirrors OrSet.
    private MvRegister(List<MvRegisterEntry> entries, Dictionary<string, long> context)
    {
        Entries = entries;
        Context = context;
    }

    /// <summary>Returns <c>true</c> when no live values remain.</summary>
    public bool IsEmpty => Entries.Count == 0;

    /// <inheritdoc />
    /// <remarks>
    /// An <see cref="MvRegister"/> is bottom when no live entries
    /// remain - i.e. <see cref="IsEmpty"/>. The dot
    /// <see cref="Context"/> may still be populated and is preserved
    /// for causal-history purposes; a containing composite treats the
    /// slot as empty.
    /// </remarks>
    public bool IsBottom => IsEmpty;

    /// <summary>Returns the number of currently-live values.</summary>
    public int Count => Entries.Count;

    /// <summary>
    /// Writes <paramref name="value"/> from <paramref name="replicaId"/>,
    /// minting a fresh dot <c>(replicaId, Context[replicaId] + 1)</c>.
    /// Every existing entry whose dot the writer has observed (every
    /// dot whose <c>Counter &lt;= Context[ReplicaId]</c>) is dropped:
    /// the new write supersedes them. Concurrent writes whose dots are
    /// not in the writer's context survive the next merge.
    /// </summary>
    /// <param name="replicaId">The replica authoring the write. Must be non-empty.</param>
    /// <param name="value">The value bytes to store. Must not be <c>null</c>.</param>
    public void Set(string replicaId, byte[] value)
    {
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        ArgumentNullException.ThrowIfNull(value);

        var nextCounter = NextCounter(replicaId);

        // Drop every entry the writer has observed by compacting Entries in
        // place, then append the new dot. An entry whose dot is dominated by
        // the writer's context is by construction one the writer saw and is
        // now superseding; the surviving entries are exactly the concurrent
        // writes from other replicas the writer has not yet observed. In the
        // steady state the register is single-valued and that lone entry is
        // observed-and-dropped, so this reuses the existing one-element list
        // instead of allocating a fresh list + backing array on every write.
        var entries = Entries;
        var write = 0;
        for (var read = 0; read < entries.Count; read++)
        {
            var entry = entries[read];
            if (!IsObserved(entry, Context))
            {
                entries[write++] = entry;
            }
        }
        if (write < entries.Count)
        {
            entries.RemoveRange(write, entries.Count - write);
        }
        entries.Add(new MvRegisterEntry
        {
            ReplicaId = replicaId,
            Counter = nextCounter,
            Value = value,
        });
        Context[replicaId] = nextCounter;
        _singleValueSnapshot = null;
    }

    /// <summary>
    /// Returns the set of currently-live values in deterministic
    /// order (by <c>(ReplicaId, Counter)</c> ascending). A
    /// single-valued register returns exactly one element; a
    /// concurrently-written register returns the conflicting
    /// candidates.
    /// <para>
    /// This is the <em>egress</em> seam of the buffer-ownership rule documented
    /// on <see cref="ICrdt{TSelf}"/>: each returned array is a copy, so a caller
    /// that writes through a returned value cannot reach this register's stored
    /// entries. An empty value costs nothing (an empty span's <c>ToArray</c>
    /// returns the shared <see cref="Array.Empty{T}"/> singleton). Call sites
    /// inside this assembly that immediately consume the bytes (deserialising
    /// them, say) should use <see cref="ValuesShared"/> instead and skip the copy.
    /// </para>
    /// </summary>
    public IReadOnlyList<byte[]> Values()
    {
        var shared = ValuesShared();
        var count = shared.Count;
        if (count == 0) return Array.Empty<byte[]>();

        // Reuse the cached ordering (the sort is the expensive part) and pay
        // only the per-value copy the egress contract requires.
        var copy = new byte[count][];
        for (var i = 0; i < count; i++) copy[i] = shared[i].AsSpan().ToArray();
        return copy;
    }

    /// <summary>
    /// The ordering-only view behind <see cref="Values"/>: the same
    /// deterministically-ordered live values, but sharing the stored
    /// <see cref="MvRegisterEntry.Value"/> buffers rather than copying them.
    /// <para>
    /// Internal by design. The returned arrays are the register's own durable
    /// buffers, so a caller must treat them as read-only and must not let one
    /// escape to an external caller - that is exactly what the public
    /// <see cref="Values"/> copy exists to prevent. It is offered so an internal
    /// consumer that immediately deserialises each value (the typed
    /// <c>MvRegisterAccessor</c>) does not pay a copy it would discard on the
    /// next line.
    /// </para>
    /// </summary>
    internal IReadOnlyList<byte[]> ValuesShared()
    {
        var count = Entries.Count;
        if (count == 0) return Array.Empty<byte[]>();

        // Steady-state fast path: a single-valued register needs neither a
        // sort nor a LINQ pipeline - the lone value is already "ordered".
        // The one-element snapshot is cached and reused across repeated reads
        // between writes. Any mutation resets the cache.
        if (count == 1) return _singleValueSnapshot ??= new[] { Entries[0].Value };

        // Multi-value (transient concurrent-write) path: copy the entries
        // out and sort with a single comparer that replicates the former
        // OrderBy(ReplicaId, Ordinal).ThenBy(Counter) composite key, then
        // project the now-ordered values. Dots are unique by construction,
        // so the (ReplicaId, Counter) key is a total order and the result
        // matches the previous stable LINQ ordering exactly.
        var entries = new MvRegisterEntry[count];
        Entries.CopyTo(entries);
        Array.Sort(entries, EntryOrdering.Instance);

        var ordered = new byte[count][];
        for (var i = 0; i < count; i++) ordered[i] = entries[i].Value;
        return ordered;
    }

    /// <summary>
    /// Total ordering over <see cref="MvRegisterEntry"/> dots that
    /// replicates the former <c>OrderBy(ReplicaId, Ordinal)</c> then
    /// <c>ThenBy(Counter)</c> composite key used by <see cref="Values"/>.
    /// A cached singleton so the multi-value sort path allocates no
    /// per-call comparer.
    /// </summary>
    private sealed class EntryOrdering : IComparer<MvRegisterEntry>
    {
        public static readonly EntryOrdering Instance = new();

        public int Compare(MvRegisterEntry x, MvRegisterEntry y)
        {
            var byReplica = string.CompareOrdinal(x.ReplicaId, y.ReplicaId);
            return byReplica != 0 ? byReplica : x.Counter.CompareTo(y.Counter);
        }
    }

    /// <summary>
    /// Lattice merge: keep entries whose dots are not dominated by
    /// the other side's <see cref="Context"/>; take the pointwise
    /// maximum of the two contexts. Commutative, associative, and
    /// idempotent.
    /// </summary>
    public static MvRegister Merge(MvRegister left, MvRegister right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        var result = left.Clone();
        result.MergeFrom(right);
        return result;
    }

    /// <summary>
    /// In-place lattice merge: applies <paramref name="other"/> into
    /// this register. Equivalent to <see cref="Merge(MvRegister, MvRegister)"/>
    /// followed by replacing the receiver, but avoids the intermediate clone.
    /// </summary>
    public void MergeFrom(MvRegister other)
    {
        ArgumentNullException.ThrowIfNull(other);

        // The dominance checks below must see the pre-merge witnesses on
        // each side - a union-then-filter against the merged context would
        // drop every entry whose dot is present on both sides. The local
        // context is read directly because it is not mutated until the
        // pointwise-max fold further down, which runs strictly after the
        // only read of it (the other-side survivor scan), so no defensive
        // snapshot clone is required.
        var otherContext = other.Context;
        var otherEntries = other.Entries;
        var localEntries = Entries;

        // The "same dot on both sides" case is handled by structural
        // presence, not by dominance: if a dot is still present on a
        // side, that side has not superseded it, so the entry must
        // survive even when the side's context dominates the dot's
        // counter. A register is single-valued in the steady state and
        // multi-valued only transiently, so both entry lists are tiny;
        // a linear ContainsDot scan beats allocating two HashSets per
        // merge on the replication hot path.
        //
        // The survivors list is allocated lazily: under at-least-once and
        // duplicate delivery the common merge is idempotent (every local
        // entry is kept and no other-side entry is added), so the result is
        // the local entry list unchanged. In that steady state we never
        // materialise a survivors list and leave Entries untouched; the list
        // is only built the first time the merge drops a local entry or adds
        // an other-side entry.
        var localCount = localEntries.Count;
        List<MvRegisterEntry>? survivors = null;

        // Keep a local entry iff the other side either still has the
        // same dot (so it has not been superseded there) or has never
        // observed it. When both sides still carry the same dot but disagree
        // on the value under it, keep the deterministically-greater value so
        // the merge stays commutative (see FindDot / CompareValueBytes). A
        // winning other-side value is Adopt-ed (copied): a fold from a peer must
        // not leave this register aliased to the peer's buffer. Keeping the
        // local entry copies nothing.
        for (var i = 0; i < localCount; i++)
        {
            var entry = localEntries[i];
            if (FindDot(otherEntries, entry.ReplicaId, entry.Counter) is { } dup)
            {
                var kept = CompareValueBytes(dup.Value, entry.Value) > 0 ? Adopt(dup) : entry;
                if (survivors is not null)
                {
                    survivors.Add(kept);
                }
                else if (!ReferenceEquals(kept.Value, entry.Value))
                {
                    // First value change: materialise the kept prefix, then
                    // the resolved entry in this local slot.
                    survivors = new List<MvRegisterEntry>(localCount + otherEntries.Count);
                    for (var k = 0; k < i; k++) survivors.Add(localEntries[k]);
                    survivors.Add(kept);
                }
            }
            else if (!IsObserved(entry, otherContext))
            {
                survivors?.Add(entry);
            }
            else if (survivors is null)
            {
                // First drop: materialise the list with the kept prefix.
                survivors = new List<MvRegisterEntry>(localCount + otherEntries.Count);
                for (var k = 0; k < i; k++) survivors.Add(localEntries[k]);
            }
        }

        // Add an other-side entry iff we have not already taken its
        // dot from the local side and we (the local side) have not
        // observed-and-superseded it. The adopted entry's value is copied - it
        // belongs to a peer that keeps using it.
        foreach (var entry in otherEntries)
        {
            if (ContainsDot(localEntries, entry.ReplicaId, entry.Counter)) continue;
            if (IsObserved(entry, Context)) continue;

            // First addition with no prior drop: every local entry survived,
            // so seed the list from the full local entry list before adding.
            if (survivors is null)
            {
                survivors = new List<MvRegisterEntry>(localCount + otherEntries.Count);
                survivors.AddRange(localEntries);
            }

            survivors.Add(Adopt(entry));
        }

        // Pointwise-max of the two contexts.
        foreach (var (replicaId, counter) in otherContext)
        {
            // Single-probe fold: a missing slot is added zero-initialised and
            // installed with counter; an existing slot advances only when the
            // incoming counter is strictly greater. Same result as the prior
            // TryGetValue-then-indexer form with one fewer hash per replica.
            ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(Context, replicaId, out var existed);
            if (!existed || counter > slot) slot = counter;
        }

        // Only replace Entries when a structural change was made; an
        // idempotent merge leaves the local entry list as-is.
        if (survivors is not null)
        {
            Entries = survivors;
            _singleValueSnapshot = null;
        }
    }

    /// <summary>Creates a deep copy of this register.</summary>
    /// <remarks>
    /// The <em>egress</em> seam of the buffer-ownership rule documented on
    /// <see cref="ICrdt{TSelf}"/>: every entry's value bytes are copied, not
    /// shared, so a caller that mutates a value reached through the clone cannot
    /// write back into this register. Composites lean on exactly this -
    /// <c>OrMap.Get</c> hands back <c>Clone()</c> precisely so the caller may
    /// mutate what it read - so a shallow entry copy here would re-open the leak
    /// one level up. The copy is a span copy rather than <see cref="Array.Clone"/>
    /// (identical allocation, roughly 3-4x faster on the <c>ordedup</c>
    /// microbench), and an empty span's <c>ToArray</c> returns the shared
    /// <see cref="Array.Empty{T}"/> singleton, so an empty value costs nothing.
    /// </remarks>
    public MvRegister Clone()
    {
        // Presize exactly and fill in one pass: the entry list must be rebuilt
        // per-entry (rather than bulk-copied through the List copy constructor)
        // because each entry's value buffer is copied on the way out.
        var entries = new List<MvRegisterEntry>(Entries.Count);
        foreach (var entry in Entries) entries.Add(Adopt(entry));
        return new(
            entries,
            // Copy the dot-context through its own comparer so the Dictionary
            // copy constructor bulk-copies the backing store instead of
            // rehashing every replica key. A fresh StringComparer.Ordinal is
            // ordinally identical but reference-distinct from the source
            // comparer, defeating that fast path. Mirrors Rga.Clone.
            new Dictionary<string, long>(Context, Context.Comparer));
    }

    /// <summary>
    /// Returns <paramref name="entry"/> with its value bytes copied, for use
    /// wherever an entry authored by somebody else (a peer register, a delta) is
    /// taken into this register's durable state, or handed back out to a caller.
    /// <para>
    /// This is the buffer-ownership rule on <see cref="ICrdt{TSelf}"/> applied at
    /// its two non-ingress seams. Only an <em>adopted</em> entry is copied, so a
    /// fold that keeps the local side allocates nothing, and an empty value
    /// reuses the shared <see cref="Array.Empty{T}"/> singleton.
    /// </para>
    /// </summary>
    private static MvRegisterEntry Adopt(MvRegisterEntry entry) =>
        entry with { Value = entry.Value.AsSpan().ToArray() };

    private long NextCounter(string replicaId) =>
        Context.TryGetValue(replicaId, out var current) ? current + 1 : 1;

    private static bool IsObserved(MvRegisterEntry entry, Dictionary<string, long> context) =>
        context.TryGetValue(entry.ReplicaId, out var observed) && entry.Counter <= observed;

    private static bool IsObserved(MvRegisterEntry entry, IReadOnlyDictionary<string, long>? context) =>
        context is not null && context.TryGetValue(entry.ReplicaId, out var observed) && entry.Counter <= observed;

    private static bool ContainsDot(List<MvRegisterEntry> entries, string replicaId, long counter)
    {
        foreach (var entry in entries)
        {
            if (entry.Counter == counter && entry.ReplicaId == replicaId) return true;
        }
        return false;
    }

    private static MvRegisterEntry? FindDot(List<MvRegisterEntry> entries, string replicaId, long counter)
    {
        foreach (var entry in entries)
        {
            if (entry.Counter == counter && entry.ReplicaId == replicaId) return entry;
        }
        return null;
    }

    private static MvRegisterEntry? FindDot(IReadOnlyList<MvRegisterEntry> entries, string replicaId, long counter)
    {
        for (var i = 0; i < entries.Count; i++)
        {
            var entry = entries[i];
            if (entry.Counter == counter && entry.ReplicaId == replicaId) return entry;
        }
        return null;
    }

    // Lexicographic byte-order comparison used to break a same-dot value
    // collision deterministically (the greater byte sequence wins) so
    // MergeFrom / MergeDelta stay commutative even when two writes minted the
    // same (replicaId, counter) dot carrying different values. Mirrors
    // Rga.CompareBytes: compare byte-by-byte, then by length, so a shorter
    // prefix sorts before a longer extension of it.
    // Lexicographic byte-order comparison used to break a same-dot value
    // collision deterministically (the greater byte sequence wins) so
    // MergeFrom / MergeDelta stay commutative even when two writes minted the
    // same (replicaId, counter) dot carrying different values. Mirrors
    // Rga.CompareBytes: a differing byte decides, otherwise a shorter prefix
    // sorts before a longer extension of it. Only the sign is load-bearing, so
    // the vectorized SequenceCompareTo is interchangeable with the scalar loop
    // it replaced (see Rga.CompareBytes for the measured delta).
    private static int CompareValueBytes(byte[] left, byte[] right)
    {
        if (ReferenceEquals(left, right)) return 0;
        return ((ReadOnlySpan<byte>)left).SequenceCompareTo(right);
    }

    /// <summary>
    /// Folds an <see cref="MvRegisterDelta"/> into this register. The
    /// result is equivalent to constructing a transient
    /// <see cref="MvRegister"/> with <see cref="MvRegisterDelta.Entries"/>
    /// and <see cref="MvRegisterDelta.Context"/> and calling
    /// <see cref="MergeFrom(MvRegister)"/>, but folds the delta's entries
    /// and context in directly without allocating that intermediate
    /// register (and its copied entries list and context dictionary).
    /// Commutative, associative, and idempotent against arrival order and
    /// duplicate delivery.
    /// </summary>
    /// <param name="delta">
    /// The typed CRDT delta authored by the producing call site. Null
    /// inner collections are treated as empty.
    /// </param>
    public void MergeDelta(MvRegisterDelta delta)
    {
        var otherEntries = delta.Entries;
        var otherContext = delta.Context;
        var hasEntries = otherEntries is { Count: > 0 };
        var hasContext = otherContext is { Count: > 0 };

        // Nothing to fold: leave the receiver untouched. The prior
        // build-a-throwaway-register form replaced Entries with an equal
        // copy in this case; skipping that copy is semantically identical
        // (the surviving entries and context are unchanged).
        if (!hasEntries && !hasContext) return;

        // Fold the delta directly into this register rather than
        // materialising a throwaway MvRegister (plus a copied entries List
        // and Context Dictionary) and calling MergeFrom. The dominance
        // semantics are identical to MergeFrom with the delta's entries and
        // context as the other side; only the intermediate allocations are
        // removed. MergeDelta is on the CRDT apply / replication hot path,
        // so those eliminated allocations are paid on every delta applied.
        var localEntries = Entries;
        var otherCount = hasEntries ? otherEntries!.Count : 0;
        var localCount = localEntries.Count;

        // The survivors list is allocated lazily. Duplicate / retried delta
        // delivery is idempotent: every local entry is kept and no delta
        // entry is added, so the entry list is unchanged. In that steady
        // state we never materialise a survivors list and leave Entries
        // untouched; it is only built the first time the fold drops a local
        // entry or adds a delta entry.
        List<MvRegisterEntry>? survivors = null;

        // Keep a local entry iff the delta still carries the same dot (so it
        // has not been superseded there) or the delta's context has never
        // observed it. A same-dot value disagreement is resolved by the same
        // deterministic max rule MergeFrom uses, so a delta fold agrees with
        // the equivalent full-state merge and is commutative. A winning delta
        // value is Adopt-ed (copied): the producer may retry the delta or fan it
        // out to other receivers, so adopting its buffer would leave them all
        // sharing one array.
        for (var i = 0; i < localCount; i++)
        {
            var entry = localEntries[i];
            var dup = hasEntries ? FindDot(otherEntries!, entry.ReplicaId, entry.Counter) : null;
            if (dup is { } match)
            {
                var kept = CompareValueBytes(match.Value, entry.Value) > 0 ? Adopt(match) : entry;
                if (survivors is not null)
                {
                    survivors.Add(kept);
                }
                else if (!ReferenceEquals(kept.Value, entry.Value))
                {
                    survivors = new List<MvRegisterEntry>(localCount + otherCount);
                    for (var k = 0; k < i; k++) survivors.Add(localEntries[k]);
                    survivors.Add(kept);
                }
            }
            else if (!IsObserved(entry, otherContext))
            {
                survivors?.Add(entry);
            }
            else if (survivors is null)
            {
                // First drop: materialise the list with the kept prefix.
                survivors = new List<MvRegisterEntry>(localCount + otherCount);
                for (var k = 0; k < i; k++) survivors.Add(localEntries[k]);
            }
        }

        // Add a delta entry iff we have not already taken its dot from the
        // local side and the local context has not observed-and-superseded it.
        // The adopted entry's value is copied, for the same reason.
        for (var i = 0; i < otherCount; i++)
        {
            var entry = otherEntries![i];
            if (ContainsDot(localEntries, entry.ReplicaId, entry.Counter)) continue;
            if (IsObserved(entry, Context)) continue;

            // First addition with no prior drop: every local entry survived,
            // so seed the list from the full local entry list before adding.
            if (survivors is null)
            {
                survivors = new List<MvRegisterEntry>(localCount + otherCount);
                survivors.AddRange(localEntries);
            }

            survivors.Add(Adopt(entry));
        }

        // Pointwise-max of the delta context into the local context.
        if (hasContext)
        {
            foreach (var (replicaId, counter) in otherContext!)
            {
                ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(Context, replicaId, out var existed);
                if (!existed || counter > slot) slot = counter;
            }
        }

        // Only replace Entries when a structural change was made; an
        // idempotent delta leaves the local entry list as-is.
        if (survivors is not null)
        {
            Entries = survivors;
            _singleValueSnapshot = null;
        }
    }
}
