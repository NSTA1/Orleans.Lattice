using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// Typed delta record for a multi-value register mutation. Carries
/// the dot-tagged entries the producing write added (a single dot
/// for <see cref="MvRegisterAccessor{T}.SetAsync(string, T, System.Threading.CancellationToken, int)"/>;
/// the union of the merged-in entries for
/// <see cref="MvRegisterAccessor{T}.MergeAsync(MvRegister, System.Threading.CancellationToken, int)"/>),
/// plus the producer's post-write dot context so the receiver can
/// apply the dominance check the same way the
/// <see cref="MvRegister"/> merge does.
/// <para>
/// Apply semantics on the receiver: merge the carried entries into
/// the local <see cref="MvRegister"/> using dot-context dominance -
/// keep a local entry iff the carried context does not dominate it,
/// and keep a carried entry iff the local context does not dominate
/// it - then pointwise-max the two contexts. The result is
/// independent of arrival order, duplicate delivery, and partial
/// overlap with the local state, so the merge is commutative,
/// associative, and idempotent.
/// </para>
/// <para>
/// Emitters always populate both collections (use empty arrays /
/// dictionaries for "no entries" / "empty context"); use
/// <see cref="Empty"/> to author a no-op delta without allocating
/// fresh empty collections. The <see langword="default"/> instance has
/// <c>null</c> collections and is intended only as the zero-value of
/// the struct - consumers should either treat <c>null</c> as empty or
/// assert non-null at the apply boundary.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.MvRegisterDelta)]
[Immutable]
public readonly record struct MvRegisterDelta
{
    /// <summary>The dot-tagged entries added by the producing write.</summary>
    [Id(0)] public IReadOnlyList<MvRegisterEntry> Entries { get; init; }

    /// <summary>
    /// The producer's post-write dot context. Used by the receiver to
    /// determine which prior local dots the producer observed and
    /// must therefore be dropped on merge.
    /// </summary>
    [Id(1)] public IReadOnlyDictionary<string, long> Context { get; init; }

    private static readonly IReadOnlyDictionary<string, long> EmptyContext =
        new Dictionary<string, long>(StringComparer.Ordinal);

    /// <summary>
    /// A reusable no-op delta with empty (but non-null)
    /// <c>Entries</c> and <see cref="Context"/> collections.
    /// Backed by <see cref="Array.Empty{T}"/> and an empty dictionary
    /// so repeated access does not allocate.
    /// </summary>
    public static MvRegisterDelta Empty { get; } = new()
    {
        Entries = Array.Empty<MvRegisterEntry>(),
        Context = EmptyContext,
    };

    /// <summary>
    /// Compares two deltas by value: <see cref="Entries"/> element-by-element
    /// using the value equality of <see cref="MvRegisterEntry"/>, and
    /// <see cref="Context"/> as an order-independent set of key/value pairs.
    /// The compiler-generated record-struct equality compares the collection
    /// references with <see cref="EqualityComparer{T}.Default"/>, so two
    /// structurally identical deltas built from independently allocated
    /// collections - and, in particular, a delta and its post-serialization
    /// self - would otherwise never compare equal.
    /// </summary>
    /// <param name="other">The delta to compare against.</param>
    public bool Equals(MvRegisterDelta other) =>
        EntriesEqual(Entries, other.Entries) && ContextEqual(Context, other.Context);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        if (Entries is { } entries)
        {
            hash.Add(entries.Count);
            foreach (var entry in entries)
            {
                hash.Add(entry);
            }
        }
        else
        {
            hash.Add(0);
        }

        // Order-independent contribution from the context: XOR of per-entry
        // hashes so two dictionaries with the same pairs in any iteration
        // order hash identically.
        var contextHash = 0;
        if (Context is { } context)
        {
            foreach (var (replicaId, counter) in context)
            {
                contextHash ^= HashCode.Combine(StringComparer.Ordinal.GetHashCode(replicaId), counter);
            }

            hash.Add(context.Count);
        }

        hash.Add(contextHash);
        return hash.ToHashCode();
    }

    private static bool EntriesEqual(IReadOnlyList<MvRegisterEntry>? left, IReadOnlyList<MvRegisterEntry>? right)
    {
        if (ReferenceEquals(left, right))
        {
            return true;
        }

        if (left is null || right is null || left.Count != right.Count)
        {
            return false;
        }

        for (var i = 0; i < left.Count; i++)
        {
            if (!left[i].Equals(right[i]))
            {
                return false;
            }
        }

        return true;
    }

    private static bool ContextEqual(IReadOnlyDictionary<string, long>? left, IReadOnlyDictionary<string, long>? right)
    {
        if (ReferenceEquals(left, right))
        {
            return true;
        }

        if (left is null || right is null || left.Count != right.Count)
        {
            return false;
        }

        foreach (var (key, value) in left)
        {
            if (!right.TryGetValue(key, out var otherValue) || otherValue != value)
            {
                return false;
            }
        }

        return true;
    }
}
