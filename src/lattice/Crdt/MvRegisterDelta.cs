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
/// fresh empty collections. The <see cref="default"/> instance has
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
    /// <see cref="Entries"/> and <see cref="Context"/> collections.
    /// Backed by <see cref="Array.Empty{T}"/> and an empty dictionary
    /// so repeated access does not allocate.
    /// </summary>
    public static MvRegisterDelta Empty { get; } = new()
    {
        Entries = Array.Empty<MvRegisterEntry>(),
        Context = EmptyContext,
    };
}
