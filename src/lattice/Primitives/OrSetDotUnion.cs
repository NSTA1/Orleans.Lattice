using System.Runtime.InteropServices;

namespace Orleans.Lattice;

/// <summary>
/// The dot-list unions every observed-remove primitive (<see cref="OrFlag"/>,
/// <see cref="RwFlag"/>, <see cref="OrSet"/>, <see cref="RwSet"/>) folds a
/// state merge or a delta through. A union is commutative, associative and
/// idempotent, which is what lets the owning CRDTs converge under arbitrary
/// arrival order and duplicate delivery.
/// <para>
/// Each width strategy lives in its own method rather than in a shared body:
/// fusing a second strategy into one body makes the JIT compile both, which
/// lengthens the live ranges the narrow arm - the steady-state arm - is
/// compiled under.
/// </para>
/// </summary>
internal static class OrSetDotUnion
{
    /// <summary>
    /// Below this many incoming dots a linear <see cref="List{T}.Contains"/>
    /// probe beats allocating and populating a hash set for the membership
    /// checks. A primitive carries one dot per concurrent add / enable / remove,
    /// overwhelmingly 1-2 in practice, so the linear path is the common case; the
    /// set is only built once an incoming list is genuinely wide. Only the
    /// incoming side must be small: at most this many appends keeps the linear
    /// probe O(target) and never quadratic.
    /// </summary>
    internal const int LinearScanThreshold = 4;

    /// <summary>
    /// Unions <paramref name="source"/>'s dots into <paramref name="target"/>
    /// (a state-level merge of one dot list).
    /// </summary>
    /// <param name="target">The accumulated dot list, appended to in place.</param>
    /// <param name="source">The dot list being merged in.</param>
    internal static void UnionInto(List<OrSetDot> target, List<OrSetDot> source)
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
        if (source.Count <= LinearScanThreshold)
        {
            // Small incoming dot list (the common 1-2-concurrent-dot and
            // steady-state delta-fold case). Only the incoming side must be
            // small - the previous guard also required the target to be small,
            // allocating a HashSet over a long-lived flag's accumulated list on
            // every small merge.
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
    /// delta twin of <see cref="UnionInto"/>: the source is walked through its
    /// backing span where its runtime shape allows it.
    /// <para>
    /// The span matters more here than on the state path. A delta's collection
    /// is declared <see cref="IReadOnlyList{T}"/> because it is serialised
    /// public surface, so the loop that shipped before paid an interface call
    /// for the indexer <b>and</b> another for the re-read of <c>Count</c> in
    /// the loop condition, on every dot, and <see cref="OrSetDot"/> is returned
    /// whole by value from both. Flag merges drive this two (OrFlag) or three
    /// (RwFlag) times per applied delta on the replication apply path.
    /// </para>
    /// </summary>
    /// <param name="target">The accumulated dot list, appended to in place.</param>
    /// <param name="source">The delta's dot list; <see langword="null"/> is treated as empty.</param>
    internal static void UnionDeltaDots(List<OrSetDot> target, IReadOnlyList<OrSetDot>? source)
    {
        if (source is not { Count: > 0 }) return;

        // Aliasing a flag's own dot list into its delta is constructible
        // because the flags' dot lists are settable, and the span walks below
        // would let an append resize the array out from under a live span. The
        // union of a list with itself is the identity, so returning is both
        // safe and correct.
        if (ReferenceEquals(target, source)) return;

        if (!CrdtDeltaListSpan.TryGetSpan(source, out var span))
        {
            UnionDeltaDotsByIndex(target, source);
            return;
        }

        if (span.Length <= LinearScanThreshold)
        {
            UnionDeltaDotsNarrow(target, span);
            return;
        }

        UnionDeltaDotsWide(target, span);
    }

    /// <summary>
    /// Unions every per-key dot list of <paramref name="source"/> into the
    /// matching list of <paramref name="target"/>, copying a key's list when
    /// <paramref name="target"/> has none (a state-level merge of a set's dot map).
    /// </summary>
    /// <param name="target">The accumulated dot map, updated in place.</param>
    /// <param name="source">The dot map being merged in.</param>
    internal static void MergeDotMaps(Dictionary<string, List<OrSetDot>> target, Dictionary<string, List<OrSetDot>> source)
    {
        foreach (var (key, dots) in source)
        {
            if (!target.TryGetValue(key, out var existing))
            {
                target[key] = [.. dots];
                continue;
            }
            if (dots.Count <= LinearScanThreshold)
            {
                // Small incoming dot list (the steady-state delta / replication
                // fold case). Only the incoming side must be small - the previous
                // guard also required the accumulated list to be small,
                // allocating a HashSet over the whole existing list every time a
                // churned key with a long dot history absorbed even a 1-2-dot
                // delta.
                foreach (var d in dots)
                {
                    if (!existing.Contains(d)) existing.Add(d);
                }
                continue;
            }
            // O(n+m) dedup via a transient HashSet - replaces the previous
            // O(n*m) List<>.Contains scan that would degrade quadratically
            // when an element accumulates many concurrent adds.
            var seen = OrSetDotSet.Build(existing, dots.Count);
            foreach (var d in dots)
            {
                if (seen.Add(d)) existing.Add(d);
            }
        }
    }

    /// <summary>
    /// Small incoming dot list - the steady-state delta-fold case. At most
    /// <see cref="LinearScanThreshold"/> appends, so the linear probe stays
    /// O(target) and never grows quadratic.
    /// </summary>
    private static void UnionDeltaDotsNarrow(List<OrSetDot> target, ReadOnlySpan<OrSetDot> source)
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
    private static void UnionDeltaDotsWide(List<OrSetDot> target, ReadOnlySpan<OrSetDot> source)
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
    private static void UnionDeltaDotsByIndex(List<OrSetDot> target, IReadOnlyList<OrSetDot> source)
    {
        var count = source.Count;
        if (count <= LinearScanThreshold)
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
