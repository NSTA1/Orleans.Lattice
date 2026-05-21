namespace Orleans.Lattice.Primitives;

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
    /// <summary>
    /// Per-element live-add dots, keyed by the base64 encoding of the
    /// element bytes. An element is a member of the set if and only if its
    /// dot list (after subtracting <see cref="Tombstones"/>) is non-empty.
    /// </summary>
    [Id(0)]
    public Dictionary<string, List<OrSetDot>> Adds { get; set; } = [];

    /// <summary>
    /// Observed-remove dots, keyed identically to <see cref="Adds"/>. A dot
    /// in this map cancels the matching dot in <see cref="Adds"/> on merge.
    /// </summary>
    [Id(1)]
    public Dictionary<string, List<OrSetDot>> Tombstones { get; set; } = [];

    /// <summary>Returns <c>true</c> when no element has any live (un-tombstoned) dot.</summary>
    public bool IsEmpty
    {
        get
        {
            foreach (var (key, dots) in Adds)
            {
                if (LiveDotCount(key, dots) > 0) return false;
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
        var key = Convert.ToBase64String(element);
        if (!Adds.TryGetValue(key, out var dots))
        {
            dots = [];
            Adds[key] = dots;
        }
        dots.Add(new OrSetDot { ReplicaId = replicaId, Counter = counter });
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
        var key = Convert.ToBase64String(element);
        if (!Adds.TryGetValue(key, out var dots) || dots.Count == 0) return false;

        // Build a tombstone HashSet once so the membership checks below are O(1)
        // regardless of how many dots have already been tombstoned for this key.
        if (!Tombstones.TryGetValue(key, out var tomb))
        {
            tomb = [];
            Tombstones[key] = tomb;
        }
        var tombSet = new HashSet<OrSetDot>(tomb);
        var anyAdded = false;
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

    /// <summary>Returns <c>true</c> when <paramref name="element"/> has any live (un-tombstoned) dot.</summary>
    public bool Contains(byte[] element)
    {
        ArgumentNullException.ThrowIfNull(element);
        var key = Convert.ToBase64String(element);
        return Adds.TryGetValue(key, out var dots) && LiveDotCount(key, dots) > 0;
    }

    /// <summary>
    /// Enumerates the live elements of the set in deterministic order.
    /// Order is the ordinal sort of each element's base64 encoding (the
    /// internal key form), which is stable across replicas but is not the
    /// same as ordering by the raw element bytes.
    /// </summary>
    public IEnumerable<byte[]> Elements()
    {
        foreach (var key in Adds.Keys.OrderBy(static k => k, StringComparer.Ordinal))
        {
            if (LiveDotCount(key, Adds[key]) > 0)
                yield return Convert.FromBase64String(key);
        }
    }

    /// <summary>Returns the number of live elements (those with at least one un-tombstoned dot).</summary>
    public int Count
    {
        get
        {
            var n = 0;
            foreach (var (key, dots) in Adds)
            {
                if (LiveDotCount(key, dots) > 0) n++;
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
        var copy = new OrSet();
        foreach (var (key, dots) in Adds) copy.Adds[key] = [.. dots];
        foreach (var (key, dots) in Tombstones) copy.Tombstones[key] = [.. dots];
        return copy;
    }

    private int LiveDotCount(string key, List<OrSetDot> dots)
    {
        if (!Tombstones.TryGetValue(key, out var tomb) || tomb.Count == 0) return dots.Count;
        if (tomb.Count <= 4)
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
