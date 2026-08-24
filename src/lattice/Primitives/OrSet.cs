using System.Buffers;

namespace Orleans.Lattice;

/// <summary>
/// An observed-remove (OR) set CRDT. Each call to
/// <see cref="Add(byte[], string, long)"/> tags the element with a unique
/// <see cref="OrSetDot"/>; <see cref="Remove(byte[])"/> drops only the dots
/// currently observed for that element. State-level <see cref="Merge(OrSet, OrSet)"/>
/// is the union of every replica's adds minus the union of every replica's
/// observed-remove dots, making the CRDT commutative, associative, and
/// idempotent under arbitrary delivery order.
/// <para>
/// Element identity is by content (byte equality), encoded internally as a
/// base64 string for serialization stability. Empty arrays are valid
/// elements; <c>null</c> is rejected.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.OrSet)]
public sealed class OrSet : ICrdt<OrSet>
{
    // Below this many already-present dots a linear scan over the small
    // list beats allocating and populating a HashSet for the membership
    // checks. An OR-Set element carries one dot per concurrent add, which
    // is overwhelmingly 1-2 in practice, so the linear path is the common
    // case; the set is only built once a key genuinely accumulates many
    // concurrent dots. Matches the LiveDotCount fast-path threshold.
    private const int DotLinearScanThreshold = 4;

    // Elements whose base64 encoding fits in this many chars are keyed
    // through a stack buffer; larger elements rent from the shared pool.
    // 256 chars covers elements up to 192 bytes with no allocation.
    private const int MaxStackBase64Chars = 256;

    private static int Base64CharCount(int byteCount) => checked((byteCount + 2) / 3 * 4);

    /// <summary>
    /// Per-element live-add dots, keyed by the base64 encoding of the
    /// element bytes. An element is a member of the set if and only if its
    /// dot list (after subtracting <see cref="Tombstones"/>) is non-empty.
    /// </summary>
    [Id(0)]
    public Dictionary<string, List<OrSetDot>> Adds { get; set; }

    /// <summary>
    /// Observed-remove dots, keyed identically to <see cref="Adds"/>. A dot
    /// in this map cancels the matching dot in <see cref="Adds"/> on merge.
    /// </summary>
    [Id(1)]
    public Dictionary<string, List<OrSetDot>> Tombstones { get; set; }

    /// <summary>Creates an empty observed-remove set.</summary>
    public OrSet()
    {
        Adds = [];
        Tombstones = [];
    }

    // Direct-assign constructor for the clone fast path: takes ownership of
    // already-built backing stores so the clone allocates no discarded
    // empty-collection shells from field initializers that an object
    // initializer would immediately overwrite.
    private OrSet(Dictionary<string, List<OrSetDot>> adds, Dictionary<string, List<OrSetDot>> tombstones)
    {
        Adds = adds;
        Tombstones = tombstones;
    }

    /// <summary>Returns <c>true</c> when no element has any live (un-tombstoned) dot.</summary>
    public bool IsEmpty
    {
        get
        {
            // No element has ever been removed: every stored dot is live, so
            // the emptiness check reduces to "does any key hold a dot" without
            // a per-key tombstone probe. Append-only membership sets and
            // secondary indexes stay in this branch for their whole lifetime.
            if (Tombstones.Count == 0)
            {
                foreach (var dots in Adds.Values)
                {
                    if (dots.Count > 0) return false;
                }
                return true;
            }
            foreach (var (key, dots) in Adds)
            {
                Tombstones.TryGetValue(key, out var tomb);
                if (LiveDotCount(dots, tomb) > 0) return false;
            }
            return true;
        }
    }

    /// <inheritdoc />
    /// <remarks>
    /// An <see cref="OrSet"/> is bottom when no element has any live
    /// (un-tombstoned) dot - i.e. <see cref="IsEmpty"/>. Tombstones may
    /// still be present and are preserved for causal-history purposes,
    /// but a containing composite (e.g.
    /// <see cref="OrMap{TKey, TValue}"/>) treats the slot as empty.
    /// </remarks>
    public bool IsBottom => IsEmpty;

    /// <summary>Adds <paramref name="element"/> with a fresh causal dot.</summary>
    public void Add(byte[] element, string replicaId, long counter)
    {
        ArgumentNullException.ThrowIfNull(element);
        ArgumentException.ThrowIfNullOrEmpty(replicaId);

        var charCount = Base64CharCount(element.Length);
        char[]? rented = charCount > MaxStackBase64Chars ? ArrayPool<char>.Shared.Rent(charCount) : null;
        Span<char> buffer = rented ?? stackalloc char[MaxStackBase64Chars];
        try
        {
            Convert.TryToBase64Chars(element, buffer, out var written);
            var key = buffer[..written];
            var adds = Adds.GetAlternateLookup<ReadOnlySpan<char>>();
            if (!adds.TryGetValue(key, out var dots))
            {
                // Only materialise the base64 string when the element is
                // genuinely new; a re-add of an existing element hits the
                // span lookup above with no allocation.
                dots = [];
                Adds[new string(key)] = dots;
            }
            dots.Add(new OrSetDot { ReplicaId = replicaId, Counter = counter });
        }
        finally
        {
            if (rented is not null) ArrayPool<char>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// Removes <paramref name="element"/> by tombstoning every dot currently
    /// observed for it. Concurrent adds on other replicas (with dots not in
    /// the local <see cref="Adds"/> at the time of removal) survive a later
    /// merge because their dots are not tombstoned here.
    /// </summary>
    public bool Remove(byte[] element)
    {
        ArgumentNullException.ThrowIfNull(element);

        var charCount = Base64CharCount(element.Length);
        char[]? rented = charCount > MaxStackBase64Chars ? ArrayPool<char>.Shared.Rent(charCount) : null;
        Span<char> buffer = rented ?? stackalloc char[MaxStackBase64Chars];
        try
        {
            Convert.TryToBase64Chars(element, buffer, out var written);
            var key = buffer[..written];
            var adds = Adds.GetAlternateLookup<ReadOnlySpan<char>>();
            if (!adds.TryGetValue(key, out var dots) || dots.Count == 0) return false;

            // Build a tombstone list once so the membership checks below are O(1)
            // regardless of how many dots have already been tombstoned for this key.
            var tombstones = Tombstones.GetAlternateLookup<ReadOnlySpan<char>>();
            if (!tombstones.TryGetValue(key, out var tomb))
            {
                // Only materialise the base64 string when a tombstone list
                // must be created for this key.
                tomb = [];
                Tombstones[new string(key)] = tomb;
            }
            var anyAdded = false;
            if (tomb.Count <= DotLinearScanThreshold)
            {
                // Tiny tombstone list: linear Contains beats hashing.
                foreach (var dot in dots)
                {
                    if (!tomb.Contains(dot)) { tomb.Add(dot); anyAdded = true; }
                }
                return anyAdded;
            }
            var tombSet = new HashSet<OrSetDot>(tomb);
            foreach (var dot in dots)
            {
                if (tombSet.Add(dot))
                {
                    tomb.Add(dot);
                    anyAdded = true;
                }
            }
            return anyAdded;
        }
        finally
        {
            if (rented is not null) ArrayPool<char>.Shared.Return(rented);
        }
    }

    /// <summary>Returns <c>true</c> when <paramref name="element"/> has any live (un-tombstoned) dot.</summary>
    public bool Contains(byte[] element)
    {
        ArgumentNullException.ThrowIfNull(element);

        var charCount = Base64CharCount(element.Length);
        char[]? rented = charCount > MaxStackBase64Chars ? ArrayPool<char>.Shared.Rent(charCount) : null;
        Span<char> buffer = rented ?? stackalloc char[MaxStackBase64Chars];
        try
        {
            Convert.TryToBase64Chars(element, buffer, out var written);
            var key = buffer[..written];
            var adds = Adds.GetAlternateLookup<ReadOnlySpan<char>>();
            if (!adds.TryGetValue(key, out var dots)) return false;
            // No element has ever been removed: the stored dots are all live,
            // so skip the tombstone alternate-lookup and probe entirely - the
            // common case for append-only membership sets and secondary indexes.
            if (Tombstones.Count == 0) return dots.Count > 0;
            Tombstones.GetAlternateLookup<ReadOnlySpan<char>>().TryGetValue(key, out var tomb);
            return LiveDotCount(dots, tomb) > 0;
        }
        finally
        {
            if (rented is not null) ArrayPool<char>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// Enumerates the live elements of the set in deterministic order.
    /// Order is the ordinal sort of each element's base64 encoding (the
    /// internal key form), which is stable across replicas but is not the
    /// same as ordering by the raw element bytes.
    /// </summary>
    public IEnumerable<byte[]> Elements()
    {
        if (Adds.Count == 0) yield break;

        // Collect the live keys first (single dictionary walk, one
        // LiveDotCount per key) then sort only the survivors, avoiding
        // both the OrderBy allocation over dead keys and the former
        // redundant second Adds[key] lookup in the yield loop.
        var noTombstones = Tombstones.Count == 0;
        var live = new List<string>(Adds.Count);
        foreach (var (key, dots) in Adds)
        {
            // No removes anywhere: every stored dot is live, so skip the
            // per-key tombstone probe and keep any non-empty key.
            if (noTombstones)
            {
                if (dots.Count > 0) live.Add(key);
                continue;
            }
            Tombstones.TryGetValue(key, out var tomb);
            if (LiveDotCount(dots, tomb) > 0) live.Add(key);
        }
        live.Sort(StringComparer.Ordinal);
        foreach (var key in live)
        {
            yield return Convert.FromBase64String(key);
        }
    }

    /// <summary>Returns the number of live elements (those with at least one un-tombstoned dot).</summary>
    public int Count
    {
        get
        {
            var n = 0;
            // No element has ever been removed: every non-empty key is live,
            // so count them without a per-key tombstone probe.
            if (Tombstones.Count == 0)
            {
                foreach (var dots in Adds.Values)
                {
                    if (dots.Count > 0) n++;
                }
                return n;
            }
            foreach (var (key, dots) in Adds)
            {
                Tombstones.TryGetValue(key, out var tomb);
                if (LiveDotCount(dots, tomb) > 0) n++;
            }
            return n;
        }
    }

    /// <summary>
    /// Lattice merge: pointwise union of <see cref="Adds"/> and
    /// <see cref="Tombstones"/>. Commutative, associative, idempotent.
    /// </summary>
    public static OrSet Merge(OrSet left, OrSet right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        var result = left.Clone();
        result.MergeFrom(right);
        return result;
    }

    /// <summary>
    /// In-place lattice merge: applies the pointwise union of
    /// <paramref name="other"/>'s adds and tombstones into this set.
    /// Equivalent to <see cref="Merge(OrSet, OrSet)"/> followed by replacing
    /// the receiver, but avoids the intermediate clone.
    /// </summary>
    public void MergeFrom(OrSet other)
    {
        ArgumentNullException.ThrowIfNull(other);
        MergeMap(Adds, other.Adds);
        MergeMap(Tombstones, other.Tombstones);
    }

    /// <summary>Creates a deep copy of this set.</summary>
    public OrSet Clone()
    {
        // Presize both backing dictionaries to the source key counts so the
        // entry-by-entry fill below never triggers an intermediate rehash
        // grow. Mirrors the OrMap.Clone / VersionVector.Clone presize; the
        // per-key dot-list copies are unchanged. Clone is on the OrSet.Merge
        // hot path (Merge clones the left operand before folding), so the
        // eliminated resize grows are paid on every replicated OR-set reconcile.
        // The direct-assign constructor takes the filled dictionaries as-is, so
        // the clone allocates no discarded empty-collection shells.
        var adds = new Dictionary<string, List<OrSetDot>>(Adds.Count);
        foreach (var (key, dots) in Adds) adds[key] = [.. dots];
        var tombstones = new Dictionary<string, List<OrSetDot>>(Tombstones.Count);
        foreach (var (key, dots) in Tombstones) tombstones[key] = [.. dots];
        return new OrSet(adds, tombstones);
    }

    /// <summary>
    /// Folds an <see cref="OrSetDelta"/> into this set: every entry in
    /// <see cref="OrSetDelta.Adds"/> is unioned into <see cref="Adds"/>,
    /// every entry in <see cref="OrSetDelta.Removes"/> is unioned into
    /// <see cref="Tombstones"/>. The merge is commutative, associative,
    /// and idempotent against arrival order and duplicate delivery -
    /// applying the same delta twice yields the same state because the
    /// per-(element, dot) sets are unions.
    /// </summary>
    /// <param name="delta">
    /// The typed CRDT delta authored by the producing call site. Empty
    /// collections are valid; <c>null</c> collections are treated as
    /// empty.
    /// </param>
    public void MergeDelta(OrSetDelta delta)
    {
        var adds = delta.Adds;
        if (adds is { Count: > 0 })
        {
            // Union each add dot into the per-element list with the same
            // de-duplication the removes path below applies to tombstones.
            // Calling Add here would append an unconditional duplicate dot on
            // every redelivery, so replaying the same delta twice would grow
            // the dot list without bound and break the idempotency this method
            // documents. Hoist a single reusable scratch buffer (CA2014).
            Span<char> scratch = stackalloc char[MaxStackBase64Chars];
            foreach (var dot in adds)
            {
                if (dot.Element is null) continue;
                var element = dot.Element;
                var charCount = Base64CharCount(element.Length);
                char[]? rented = charCount > scratch.Length ? ArrayPool<char>.Shared.Rent(charCount) : null;
                Span<char> buffer = rented ?? scratch;
                try
                {
                    Convert.TryToBase64Chars(element, buffer, out var written);
                    var key = buffer[..written];
                    var addLookup = Adds.GetAlternateLookup<ReadOnlySpan<char>>();
                    if (!addLookup.TryGetValue(key, out var dots))
                    {
                        dots = [];
                        Adds[new string(key)] = dots;
                    }
                    var entry = new OrSetDot { ReplicaId = dot.ReplicaId, Counter = dot.Counter };
                    if (!dots.Contains(entry)) dots.Add(entry);
                }
                finally
                {
                    if (rented is not null) ArrayPool<char>.Shared.Return(rented);
                }
            }
        }
        var removes = delta.Removes;
        if (removes is { Count: > 0 })
        {
            // Hoist a single reusable scratch buffer out of the loop so
            // the stackalloc is not repeated per element (CA2014).
            Span<char> scratch = stackalloc char[MaxStackBase64Chars];
            foreach (var dot in removes)
            {
                if (dot.Element is null) continue;
                var element = dot.Element;
                var charCount = Base64CharCount(element.Length);
                char[]? rented = charCount > scratch.Length ? ArrayPool<char>.Shared.Rent(charCount) : null;
                Span<char> buffer = rented ?? scratch;
                try
                {
                    Convert.TryToBase64Chars(element, buffer, out var written);
                    var key = buffer[..written];
                    var tombstones = Tombstones.GetAlternateLookup<ReadOnlySpan<char>>();
                    if (!tombstones.TryGetValue(key, out var tomb))
                    {
                        tomb = [];
                        Tombstones[new string(key)] = tomb;
                    }
                    var entry = new OrSetDot { ReplicaId = dot.ReplicaId, Counter = dot.Counter };
                    if (!tomb.Contains(entry)) tomb.Add(entry);
                }
                finally
                {
                    if (rented is not null) ArrayPool<char>.Shared.Return(rented);
                }
            }
        }
    }

    private static int LiveDotCount(List<OrSetDot> dots, List<OrSetDot>? tomb)
    {
        if (tomb is null || tomb.Count == 0) return dots.Count;
        if (tomb.Count <= DotLinearScanThreshold)
        {
            // Tiny tombstone list: linear scan beats hashing.
            var live = 0;
            foreach (var d in dots)
            {
                if (!tomb.Contains(d)) live++;
            }
            return live;
        }
        var tombSet = new HashSet<OrSetDot>(tomb);
        var n = 0;
        foreach (var d in dots)
        {
            if (!tombSet.Contains(d)) n++;
        }
        return n;
    }

    private static void MergeMap(Dictionary<string, List<OrSetDot>> target, Dictionary<string, List<OrSetDot>> source)
    {
        foreach (var (key, dots) in source)
        {
            if (!target.TryGetValue(key, out var existing))
            {
                target[key] = [.. dots];
                continue;
            }
            if (existing.Count <= DotLinearScanThreshold && dots.Count <= DotLinearScanThreshold)
            {
                // Tiny dot list (the common 1-2-concurrent-add case): a
                // linear Contains is cheaper than allocating a HashSet.
                foreach (var d in dots)
                {
                    if (!existing.Contains(d)) existing.Add(d);
                }
                continue;
            }
            // O(n+m) dedup via a transient HashSet - replaces the previous
            // O(n*m) List<>.Contains scan that would degrade quadratically
            // when an element accumulates many concurrent adds.
            var seen = new HashSet<OrSetDot>(existing);
            foreach (var d in dots)
            {
                if (seen.Add(d)) existing.Add(d);
            }
        }
    }
}
