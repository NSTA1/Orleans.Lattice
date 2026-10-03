namespace Orleans.Lattice;

using System.Runtime.InteropServices;

/// <summary>
/// An observed-remove (enable-wins) flag CRDT. Each call to
/// <see cref="Enable(string, long)"/> tags the flag with a unique
/// <see cref="OrSetDot"/>; <see cref="Disable"/> drops only the dots
/// currently observed as enabled. State-level <see cref="Merge(OrFlag, OrFlag)"/>
/// unions every replica's enable dots and every replica's observed-remove
/// dots, and an enable dot counts only while no same-replica observed-remove
/// dot at an equal or higher counter covers it, making the CRDT commutative,
/// associative, and idempotent under arbitrary delivery order.
/// <para>
/// The flag is the single-element specialisation of <see cref="OrSet"/>:
/// it tracks presence ("enabled") rather than a set of element values, so
/// it carries no element payload. It is the minimal observed-remove
/// primitive for composite-key membership rows - e.g. a tag/key secondary
/// index where the meaningful bit is whether a <c>(tag, member)</c> row is
/// present - giving OR-Set-grade convergence under concurrent active-active
/// enable / disable without storing a singleton set's element bytes.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.OrFlag)]
public sealed class OrFlag : ICrdt<OrFlag>
{
    // Below this many tombstone dots a linear scan beats allocating and
    // populating a HashSet for the membership checks. A flag carries one
    // dot per concurrent enable/disable, overwhelmingly 1-2 in practice,
    // so the linear path is the common case; the set is only built once a
    // flag genuinely accumulates many concurrent dots. Mirrors
    // OrSet.DotLinearScanThreshold.
    private const int DotLinearScanThreshold = 4;

    /// <summary>
    /// Live enable dots. The flag is enabled if and only if at least one
    /// of these dots is not covered by <see cref="Tombstones"/> - that is, no
    /// tombstone dot from the same replica sits at an equal or higher counter.
    /// </summary>
    [Id(0)]
    public List<OrSetDot> Enables { get; set; }

    /// <summary>
    /// Observed-remove (disable) dots. A dot in this list cancels
    /// same-replica enable dots at or below its counter.
    /// </summary>
    [Id(1)]
    public List<OrSetDot> Tombstones { get; set; }

    /// <summary>Creates an empty enable-wins flag.</summary>
    public OrFlag()
    {
        Enables = [];
        Tombstones = [];
    }

    // Direct-assign constructor for the clone fast path: takes ownership of
    // already-built backing stores so the clone allocates no discarded
    // empty-collection shells from field initializers that an object
    // initializer would immediately overwrite. Mirrors OrSet.
    private OrFlag(List<OrSetDot> enables, List<OrSetDot> tombstones)
    {
        Enables = enables;
        Tombstones = tombstones;
    }

    /// <summary>Returns <c>true</c> when at least one enable dot is not tombstoned.</summary>
    public bool IsEnabled => OrSetDotCompaction.AnyLive(Enables, Tombstones);

    /// <inheritdoc />
    /// <remarks>
    /// An <see cref="OrFlag"/> is bottom when it is not enabled - i.e. no
    /// enable dot survives the tombstone set. Tombstones may still be
    /// present and are preserved for causal-history purposes, but a
    /// containing composite (e.g. <see cref="OrMap{TKey, TValue}"/>)
    /// treats the slot as absent.
    /// </remarks>
    public bool IsBottom => !IsEnabled;

    /// <summary>
    /// Enables the flag with a fresh causal dot, then compacts this replica's
    /// dot history so re-enabling an already-enabled flag does not grow state.
    /// <para>
    /// The fresh dot is what preserves add-wins: it is concurrent with, and so
    /// survives, a disable authored elsewhere that never observed it. The
    /// compaction that follows only ever collapses <b>this</b> replica's own
    /// superseded dots, which carry no information the new dot does not - see
    /// <see cref="OrSetDotCompaction"/>.
    /// </para>
    /// </summary>
    /// <param name="replicaId">The replica authoring the enable. Must be non-empty.</param>
    /// <param name="counter">The replica-local monotonic counter for the dot.</param>
    public void Enable(string replicaId, long counter)
    {
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        Enables.Add(new OrSetDot { ReplicaId = replicaId, Counter = counter });
        Compact();
    }

    /// <summary>
    /// Disables the flag by tombstoning every enable dot currently
    /// observed. Concurrent enables on other replicas (with dots not in
    /// the local <see cref="Enables"/> at the time of the disable) survive
    /// a later merge because their dots are not tombstoned here. Returns
    /// <c>true</c> when at least one new dot was tombstoned.
    /// <para>
    /// Tombstoning the observed dots is sufficient even though
    /// <see cref="Enable(string, long)"/> may have compacted a replica's
    /// earlier dots away: cancellation is coverage-based
    /// (<see cref="OrSetDotCompaction.Covers"/>), so a tombstone at a
    /// replica's highest observed counter also cancels every lower dot from
    /// that replica - including one this replica compacted away but a peer
    /// still holds.
    /// </para>
    /// </summary>
    public bool Disable()
    {
        if (Enables.Count == 0) return false;
        var anyAdded = false;
        foreach (var dot in Enables)
        {
            if (!OrSetDotCompaction.Covers(Tombstones, in dot))
            {
                Tombstones.Add(dot);
                anyAdded = true;
            }
        }

        if (anyAdded)
        {
            Compact();
        }

        return anyAdded;
    }

    /// <summary>
    /// Collapses this flag's dot history to its bounded normal form: at most one
    /// enable dot and one tombstone per replica. Idempotent, and never changes
    /// <see cref="IsEnabled"/>.
    /// <para>
    /// A cancelled enable dot is deliberately <b>retained</b> rather than pruned.
    /// Pruning it would save a few bytes, but the durable per-key history view
    /// decodes a flag's state into its add/remove events, and dropping the
    /// cancelled dot would erase the "enabled" half of an enable-then-disable
    /// pair. Keeping one dot per replica on each side already bounds the state
    /// at O(replicas), which is the whole point, so the prune bought nothing the
    /// per-replica collapse had not already banked.
    /// </para>
    /// <para>
    /// Running this on every mutation and every merge is what makes the fix
    /// <b>self-healing</b>: a flag that already accumulated an unbounded dot
    /// history under an older build collapses the first time anything merges
    /// into it or folds a delta onto it, with no re-derivation, migration, or
    /// operator step. Write-ahead-log replay folds deltas through
    /// <see cref="MergeDelta"/>, so a replayed history heals on the way back in
    /// too.
    /// </para>
    /// </summary>
    private void Compact()
    {
        OrSetDotCompaction.CompactMaxPerReplica(Tombstones);
        OrSetDotCompaction.CompactMaxPerReplica(Enables);
    }

    /// <summary>
    /// Lattice merge: pointwise union of <see cref="Enables"/> and
    /// <see cref="Tombstones"/>. Commutative, associative, idempotent.
    /// </summary>
    public static OrFlag Merge(OrFlag left, OrFlag right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        var result = left.Clone();
        result.MergeFrom(right);
        return result;
    }

    /// <summary>
    /// In-place lattice merge: applies the union of <paramref name="other"/>'s
    /// enable and tombstone dots into this flag. Equivalent to
    /// <see cref="Merge(OrFlag, OrFlag)"/> followed by replacing the
    /// receiver, but avoids the intermediate clone.
    /// </summary>
    public void MergeFrom(OrFlag other)
    {
        ArgumentNullException.ThrowIfNull(other);
        UnionInto(Enables, other.Enables);
        UnionInto(Tombstones, other.Tombstones);
        Compact();
    }

    /// <summary>Creates a deep copy of this flag.</summary>
    public OrFlag Clone() => new([.. Enables], [.. Tombstones]);

    /// <summary>
    /// Folds an <see cref="OrFlagDelta"/> into this flag: every dot in
    /// <see cref="OrFlagDelta.Enables"/> is unioned into <see cref="Enables"/>,
    /// every dot in <see cref="OrFlagDelta.Disables"/> is unioned into
    /// <see cref="Tombstones"/>. The merge is commutative, associative,
    /// and idempotent against arrival order and duplicate delivery -
    /// applying the same delta twice yields the same state because the
    /// per-dot sets are unions.
    /// </summary>
    /// <param name="delta">
    /// The typed CRDT delta authored by the producing call site. Empty
    /// collections are valid; <c>null</c> collections are treated as empty.
    /// </param>
    public void MergeDelta(OrFlagDelta delta)
    {
        UnionDots(Enables, delta.Enables);
        UnionDots(Tombstones, delta.Disables);
        Compact();
    }

    private static void UnionInto(List<OrSetDot> target, List<OrSetDot> source)
    {
        if (source.Count == 0) return;

        // A union with itself is the identity, and short-circuiting it is load
        // bearing rather than merely thrifty: the walks below resolve source's
        // backing span once and then append to target, so aliasing the two
        // lists would let an append resize the array out from under a live
        // span. The list enumerator this replaced raised on the same aliasing
        // through its version check, so the guard preserves that safety while
        // turning a throw into the correct answer.
        if (ReferenceEquals(target, source)) return;

        // Walk the resolved span with ref readonly rather than the list's
        // struct enumerator: OrSetDot is a multi-field struct, so the
        // enumerator's Current copies it once before the Contains/Add call
        // copies it again, and the enumerator re-checks the list version on
        // every MoveNext. Flag merges drive this two (OrFlag) or three
        // (RwFlag) times apiece on the replication apply path.
        var span = CollectionsMarshal.AsSpan(source);
        if (source.Count <= DotLinearScanThreshold)
        {
            // Small incoming dot list (the common 1-2-concurrent-dot and
            // steady-state delta-fold case): at most DotLinearScanThreshold
            // appends, so the linear Contains stays O(target) and never grows
            // quadratic. Only the incoming side must be small - the previous
            // guard also required the target to be small, allocating a HashSet
            // over a long-lived flag's accumulated list on every small merge.
            for (var i = 0; i < span.Length; i++)
            {
                ref readonly var dot = ref span[i];
                if (!target.Contains(dot)) target.Add(dot);
            }
            return;
        }
        var seen = OrSetDotSet.Build(target, source.Count);
        for (var i = 0; i < span.Length; i++)
        {
            ref readonly var dot = ref span[i];
            if (seen.Add(dot)) target.Add(dot);
        }
    }

    /// <summary>
    /// Folds a delta-side dot list into <paramref name="target"/>. This is the
    /// delta twin of <see cref="UnionInto"/> and carries the same two trims:
    /// the source is walked through its backing span where its runtime shape
    /// allows it, and each width strategy lives in its own sibling method
    /// rather than in a shared body.
    /// <para>
    /// The span matters more here than on the state path. A delta's collection
    /// is declared <see cref="IReadOnlyList{T}"/> because it is serialised
    /// public surface, so the loop that shipped before paid an interface call
    /// for the indexer <b>and</b> another for the re-read of <c>Count</c> in
    /// the loop condition, on every dot, and <see cref="OrSetDot"/> is returned
    /// whole by value from both. Flag merges drive this twice per applied
    /// delta on the replication apply path.
    /// </para>
    /// <para>
    /// The split is the second trim and is not cosmetic: fusing a second
    /// strategy into one body makes the JIT compile both, which lengthens the
    /// live ranges the narrow arm - the steady-state arm - is compiled under.
    /// </para>
    /// </summary>
    private static void UnionDots(List<OrSetDot> target, IReadOnlyList<OrSetDot>? source)
    {
        if (source is not { Count: > 0 }) return;

        // Aliasing a flag's own dot list into its delta is constructible
        // because Enables and Tombstones are settable, and the span walks below
        // would let an append resize the array out from under a live span. The
        // union of a list with itself is the identity, so returning is both
        // safe and correct.
        if (ReferenceEquals(target, source)) return;

        if (!CrdtDeltaListSpan.TryGetSpan(source, out var span))
        {
            UnionDotsByIndex(target, source);
            return;
        }

        if (span.Length <= DotLinearScanThreshold)
        {
            UnionDotsNarrow(target, span);
            return;
        }

        UnionDotsWide(target, span);
    }

    /// <summary>
    /// Small incoming dot list - the steady-state delta-fold case. At most
    /// <c>DotLinearScanThreshold</c> appends, so the linear probe stays
    /// O(target) and never grows quadratic.
    /// </summary>
    private static void UnionDotsNarrow(List<OrSetDot> target, ReadOnlySpan<OrSetDot> source)
    {
        for (var i = 0; i < source.Length; i++)
        {
            ref readonly var dot = ref source[i];
            if (!target.Contains(dot)) target.Add(dot);
        }
    }

    /// <summary>
    /// Wide incoming dot list: index the accumulated side once so the probe is
    /// O(1) per dot rather than O(target).
    /// </summary>
    private static void UnionDotsWide(List<OrSetDot> target, ReadOnlySpan<OrSetDot> source)
    {
        var seen = OrSetDotSet.Build(target, source.Length);
        for (var i = 0; i < source.Length; i++)
        {
            ref readonly var dot = ref source[i];
            if (seen.Add(dot)) target.Add(dot);
        }
    }

    /// <summary>
    /// Fallback for a delta whose collection is neither an array nor a
    /// <see cref="List{T}"/> - a caller-supplied container, or a deserialiser
    /// that chose another shape. This is the walk that shipped before, kept
    /// verbatim so an unspannable delta is no slower than it used to be.
    /// </summary>
    private static void UnionDotsByIndex(List<OrSetDot> target, IReadOnlyList<OrSetDot> source)
    {
        var count = source.Count;
        if (count <= DotLinearScanThreshold)
        {
            for (var i = 0; i < count; i++)
            {
                var dot = source[i];
                if (!target.Contains(dot)) target.Add(dot);
            }
            return;
        }
        var seen = OrSetDotSet.Build(target, count);
        for (var i = 0; i < count; i++)
        {
            var dot = source[i];
            if (seen.Add(dot)) target.Add(dot);
        }
    }
}
