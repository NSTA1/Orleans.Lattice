namespace Orleans.Lattice.Primitives;

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
        if (Entries.Count == 0) return Array.Empty<byte[]>();
        var ordered = Entries
            .OrderBy(static e => e.ReplicaId, StringComparer.Ordinal)
            .ThenBy(static e => e.Counter)
            .Select(static e => e.Value)
            .ToArray();
        return ordered;
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

        // Capture the pre-merge local context so the dominance checks
        // below see the pre-merge witnesses on each side - a
        // union-then-filter against the merged context would drop
        // every entry whose dot is present on both sides.
        var localContext = new Dictionary<string, long>(Context, StringComparer.Ordinal);
        var otherContext = other.Context;

        // Build dot-key sets so the "same dot on both sides" case is
        // handled by structural presence, not by dominance: if a dot
        // is still present on a side, that side has not superseded
        // it, so the entry must survive even when the side's context
        // dominates the dot's counter.
        var otherDots = new HashSet<(string ReplicaId, long Counter)>();
        foreach (var entry in other.Entries)
        {
            otherDots.Add((entry.ReplicaId, entry.Counter));
        }

        var localDots = new HashSet<(string ReplicaId, long Counter)>();
        foreach (var entry in Entries)
        {
            localDots.Add((entry.ReplicaId, entry.Counter));
        }

        var survivors = new List<MvRegisterEntry>(Entries.Count + other.Entries.Count);

        // Keep a local entry iff the other side either still has the
        // same dot (so it has not been superseded there) or has never
        // observed it.
        foreach (var entry in Entries)
        {
            if (otherDots.Contains((entry.ReplicaId, entry.Counter))
                || !IsObserved(entry, otherContext))
            {
                survivors.Add(entry);
            }
        }

        // Add an other-side entry iff we have not already taken its
        // dot from the local side and we (the local side) have not
        // observed-and-superseded it.
        foreach (var entry in other.Entries)
        {
            if (localDots.Contains((entry.ReplicaId, entry.Counter))) continue;
            if (IsObserved(entry, localContext)) continue;
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
}
