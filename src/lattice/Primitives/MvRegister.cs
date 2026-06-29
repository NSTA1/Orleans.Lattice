namespace Orleans.Lattice;

/// <summary>
/// A multi-value register CRDT. Unlike a last-writer-wins register,
/// which silently collapses concurrent writes from different replicas
/// to whichever wall-clock timestamp happens to be larger, an
/// <see cref="MvRegister"/> preserves every concurrent write as a
/// distinct dot-tagged value so application code can resolve the
/// merge itself (e.g. show the user the conflicting candidates).
/// <para>
/// State shape: a dot-tagged set of live <see cref="Entries"/> plus a
/// <see cref="Context"/> mapping each replica id to the highest
/// counter ever observed for that replica. A write on
/// <paramref name="replicaId"/> via <see cref="Set(string, byte[])"/>
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
    public List<MvRegisterEntry> Entries { get; set; } = [];

    /// <summary>
    /// Dot context: per-replica highest-observed counter. Acts as the
    /// dominance witness for the merge - an entry whose dot is
    /// dominated by the other side's context has been observed-and-
    /// superseded and must not be re-introduced on merge.
    /// </summary>
    [Id(1)]
    public Dictionary<string, long> Context { get; set; } = [];

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

        // Drop every entry the writer has observed. An entry whose dot
        // is dominated by the writer's context is by construction one
        // the writer saw and is now superseding; the surviving entries
        // are exactly the concurrent writes from other replicas the
        // writer has not yet observed.
        var survivors = new List<MvRegisterEntry>(Entries.Count + 1);
        foreach (var entry in Entries)
        {
            if (!IsObserved(entry, Context))
            {
                survivors.Add(entry);
            }
        }
        survivors.Add(new MvRegisterEntry
        {
            ReplicaId = replicaId,
            Counter = nextCounter,
            Value = value,
        });
        Entries = survivors;
        Context[replicaId] = nextCounter;
    }

    /// <summary>
    /// Returns the set of currently-live values in deterministic
    /// order (by <c>(ReplicaId, Counter)</c> ascending). A
    /// single-valued register returns exactly one element; a
    /// concurrently-written register returns the conflicting
    /// candidates.
    /// </summary>
    public IReadOnlyList<byte[]> Values()
    {
        var count = Entries.Count;
        if (count == 0) return Array.Empty<byte[]>();

        // Steady-state fast path: a single-valued register needs neither a
        // sort nor a LINQ pipeline - the lone value is already "ordered".
        if (count == 1) return new[] { Entries[0].Value };

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
        var survivors = new List<MvRegisterEntry>(localEntries.Count + otherEntries.Count);

        // Keep a local entry iff the other side either still has the
        // same dot (so it has not been superseded there) or has never
        // observed it.
        foreach (var entry in localEntries)
        {
            if (ContainsDot(otherEntries, entry.ReplicaId, entry.Counter)
                || !IsObserved(entry, otherContext))
            {
                survivors.Add(entry);
            }
        }

        // Add an other-side entry iff we have not already taken its
        // dot from the local side and we (the local side) have not
        // observed-and-superseded it.
        foreach (var entry in otherEntries)
        {
            if (ContainsDot(localEntries, entry.ReplicaId, entry.Counter)) continue;
            if (IsObserved(entry, Context)) continue;
            survivors.Add(entry);
        }

        // Pointwise-max of the two contexts.
        foreach (var (replicaId, counter) in otherContext)
        {
            if (!Context.TryGetValue(replicaId, out var current) || counter > current)
            {
                Context[replicaId] = counter;
            }
        }

        Entries = survivors;
    }

    /// <summary>Creates a deep copy of this register.</summary>
    public MvRegister Clone()
    {
        var copy = new MvRegister
        {
            Entries = new List<MvRegisterEntry>(Entries.Count),
            Context = new Dictionary<string, long>(Context, StringComparer.Ordinal),
        };
        foreach (var entry in Entries)
        {
            // The value bytes are treated as immutable by every
            // production call site, so the reference is shared. The
            // ReplicaId and Counter components are value types or
            // interned strings.
            copy.Entries.Add(entry);
        }
        return copy;
    }

    private long NextCounter(string replicaId) =>
        Context.TryGetValue(replicaId, out var current) ? current + 1 : 1;

    private static bool IsObserved(MvRegisterEntry entry, Dictionary<string, long> context) =>
        context.TryGetValue(entry.ReplicaId, out var observed) && entry.Counter <= observed;

    private static bool ContainsDot(List<MvRegisterEntry> entries, string replicaId, long counter)
    {
        foreach (var entry in entries)
        {
            if (entry.Counter == counter && entry.ReplicaId == replicaId) return true;
        }
        return false;
    }

    /// <summary>
    /// Folds an <see cref="MvRegisterDelta"/> into this register. The
    /// merge is equivalent to constructing a transient
    /// <see cref="MvRegister"/> with <see cref="MvRegisterDelta.Entries"/>
    /// and <see cref="MvRegisterDelta.Context"/> and calling
    /// <see cref="MergeFrom(MvRegister)"/>; commutative, associative,
    /// and idempotent against arrival order and duplicate delivery.
    /// </summary>
    /// <param name="delta">
    /// The typed CRDT delta authored by the producing call site. Null
    /// inner collections are treated as empty.
    /// </param>
    public void MergeDelta(MvRegisterDelta delta)
    {
        var other = new MvRegister();
        var entries = delta.Entries;
        if (entries is { Count: > 0 })
        {
            other.Entries.Capacity = entries.Count;
            foreach (var entry in entries) other.Entries.Add(entry);
        }
        var context = delta.Context;
        if (context is { Count: > 0 })
        {
            foreach (var (replicaId, counter) in context) other.Context[replicaId] = counter;
        }
        MergeFrom(other);
    }
}
