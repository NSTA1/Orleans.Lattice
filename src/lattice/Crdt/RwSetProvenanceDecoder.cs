using System.Collections.Generic;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// <see cref="ICrdtProvenanceDecoder"/> for the remove-wins observed-remove set
/// shape (<see cref="LatticeMergeMode.RwSet"/>) - the set-granularity
/// counterpart of <see cref="RwFlagProvenanceDecoder"/>. Turns a
/// <see cref="RwSet"/>'s stored state or a sequence of <see cref="RwSetDelta"/>
/// author deltas into ordered <see cref="CrdtMemberChange"/> events.
/// <para>
/// A remove-wins set retains full element-level provenance durably: every add
/// carries a unique <c>(replica, counter)</c> dot, and every remove carries its
/// own dot too (a remove mints a fresh surviving dot rather than tombstoning an
/// add). This decoder reads that dot context back out as events - each add dot
/// maps to an <see cref="CrdtMemberChangeKind.Added"/> event and each remove dot
/// to a <see cref="CrdtMemberChangeKind.Removed"/> event, preserving the causal
/// dot context. The observed-add tombstones (remove dots an add has cancelled)
/// are bookkeeping for the remove-wins tie-break and are not surfaced as
/// separate events: the remove they cancel already appears as its own
/// <see cref="CrdtMemberChangeKind.Removed"/> event.
/// </para>
/// </summary>
public sealed class RwSetProvenanceDecoder : ICrdtProvenanceDecoder
{
    /// <summary>A shared, stateless instance. The decoder holds no per-call state.</summary>
    public static RwSetProvenanceDecoder Instance { get; } = new();

    /// <inheritdoc />
    public LatticeMergeMode Mode => LatticeMergeMode.RwSet;

    /// <summary>
    /// Decodes an ordered <see cref="RwSetDelta"/> sequence into member-change
    /// events in operation order. Within a single delta, adds precede removes
    /// (the delta records the two as separate dot lists, so there is no finer
    /// intra-delta operation order to preserve); across deltas, the supplied
    /// order is the causal order. The observed-add tombstones are not emitted
    /// (see the type remarks). Each event carries the originating delta's
    /// wall-clock stamp when one was supplied.
    /// </summary>
    /// <param name="deltas">
    /// The ordered author-delta sequence; each entry's <c>Delta</c> must be an
    /// <see cref="RwSetDelta"/>.
    /// </param>
    /// <returns>The decoded member-change events, in operation order.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="deltas"/> is <see langword="null"/>.</exception>
    public IReadOnlyList<CrdtMemberChange> DecodeDeltas(IReadOnlyList<CrdtProvenanceDelta> deltas)
    {
        ArgumentNullException.ThrowIfNull(deltas);
        if (deltas.Count == 0) return Array.Empty<CrdtMemberChange>();

        var total = 0;
        for (var i = 0; i < deltas.Count; i++)
        {
            var delta = (RwSetDelta)deltas[i].Delta;
            if (delta.Adds is { Count: > 0 } adds) total += adds.Count;
            if (delta.Removes is { Count: > 0 } removes) total += removes.Count;
        }
        if (total == 0) return Array.Empty<CrdtMemberChange>();

        var result = new List<CrdtMemberChange>(total);
        for (var i = 0; i < deltas.Count; i++)
        {
            var entry = deltas[i];
            var delta = (RwSetDelta)entry.Delta;
            EmitDots(result, delta.Adds, CrdtMemberChangeKind.Added, entry.WallClock);
            EmitDots(result, delta.Removes, CrdtMemberChangeKind.Removed, entry.WallClock);
        }
        return result;
    }

    /// <summary>
    /// Reconstructs member-change events from a folded <see cref="RwSet"/>: each
    /// add dot yields an <see cref="CrdtMemberChangeKind.Added"/> event and each
    /// remove dot a <see cref="CrdtMemberChangeKind.Removed"/> event. Cross-element
    /// order is the ordinal order of the elements' internal keys; within an
    /// element, events are ordered by causal ordinal then replica then kind. The
    /// observed-add tombstones are not surfaced separately (the remove they cancel
    /// is already emitted from <see cref="RwSet.Removes"/>). Because no owning
    /// mutation is available, <see cref="CrdtMemberChange.WallClock"/> is always
    /// <see langword="null"/>.
    /// </summary>
    /// <param name="state">The <see cref="RwSet"/> to decode.</param>
    /// <returns>The reconstructed member-change events.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="state"/> is <see langword="null"/>.</exception>
    public IReadOnlyList<CrdtMemberChange> DecodeState(object state)
    {
        ArgumentNullException.ThrowIfNull(state);
        var set = (RwSet)state;
        var adds = set.Adds;
        var removes = set.Removes;

        var keys = new List<string>(adds.Count + removes.Count);
        var total = 0;
        foreach (var (key, dots) in adds)
        {
            keys.Add(key);
            total += dots.Count;
        }
        foreach (var (key, dots) in removes)
        {
            total += dots.Count;
            if (!adds.ContainsKey(key)) keys.Add(key);
        }
        if (total == 0) return Array.Empty<CrdtMemberChange>();

        keys.Sort(StringComparer.Ordinal);

        var result = new List<CrdtMemberChange>(total);
        foreach (var key in keys)
        {
            var element = Convert.FromBase64String(key);
            var start = result.Count;

            if (adds.TryGetValue(key, out var addDots))
            {
                for (var i = 0; i < addDots.Count; i++)
                {
                    var dot = addDots[i];
                    result.Add(new CrdtMemberChange
                    {
                        Element = element,
                        Kind = CrdtMemberChangeKind.Added,
                        ReplicaId = dot.ReplicaId,
                        Ordinal = dot.Counter,
                        WallClock = null,
                    });
                }
            }

            if (removes.TryGetValue(key, out var removeDots))
            {
                for (var i = 0; i < removeDots.Count; i++)
                {
                    var dot = removeDots[i];
                    result.Add(new CrdtMemberChange
                    {
                        Element = element,
                        Kind = CrdtMemberChangeKind.Removed,
                        ReplicaId = dot.ReplicaId,
                        Ordinal = dot.Counter,
                        WallClock = null,
                    });
                }
            }

            result.Sort(start, result.Count - start, CausalOrderComparer.Instance);
        }
        return result;
    }

    /// <summary>
    /// Projects a folded <see cref="RwSet"/> into its live members only. Each
    /// element that carries an add dot and no surviving remove dot yields one
    /// <see cref="CrdtMemberValue"/> carrying the element bytes and the provenance
    /// of its add dot with the highest causal ordinal (tie-broken by replica id).
    /// A removed element - one whose remove dots are not all cancelled by
    /// observed-add tombstones - is excluded (remove-wins), which is the key
    /// behavioural difference from <see cref="DecodeState(object)"/>: the current
    /// value contains only what is presently in the set. Members are ordered by
    /// the ordinal sort of each element's internal base64 key, matching
    /// <see cref="RwSet.Elements"/>.
    /// </summary>
    /// <param name="state">The <see cref="RwSet"/> to project.</param>
    /// <returns>The live elements as current-state members.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="state"/> is <see langword="null"/>.</exception>
    public IReadOnlyList<CrdtMemberValue> DecodeCurrentValue(object state)
    {
        ArgumentNullException.ThrowIfNull(state);
        var set = (RwSet)state;
        var adds = set.Adds;
        if (adds.Count == 0) return Array.Empty<CrdtMemberValue>();

        var keys = new List<string>(adds.Count);
        foreach (var key in adds.Keys) keys.Add(key);
        keys.Sort(StringComparer.Ordinal);

        var result = new List<CrdtMemberValue>(keys.Count);
        foreach (var key in keys)
        {
            var addDots = adds[key];
            if (addDots.Count == 0) continue;

            // Remove-wins: the element is present only when no remove dot
            // survives (every remove dot has been cancelled by an observed-add
            // tombstone).
            if (HasLiveRemove(set, key)) continue;

            var bestReplica = string.Empty;
            var bestCounter = long.MinValue;
            var hasLive = false;
            for (var i = 0; i < addDots.Count; i++)
            {
                var dot = addDots[i];
                if (!hasLive
                    || dot.Counter > bestCounter
                    || (dot.Counter == bestCounter && string.CompareOrdinal(dot.ReplicaId, bestReplica) > 0))
                {
                    hasLive = true;
                    bestReplica = dot.ReplicaId;
                    bestCounter = dot.Counter;
                }
            }

            if (!hasLive) continue;
            result.Add(new CrdtMemberValue
            {
                Element = Convert.FromBase64String(key),
                ReplicaId = bestReplica,
                Ordinal = bestCounter,
            });
        }

        return result.Count == 0 ? Array.Empty<CrdtMemberValue>() : result;
    }

    /// <summary>
    /// Whether any of <paramref name="key"/>'s remove dots survives its
    /// observed-add tombstones - the remove-wins exclusion test.
    /// <para>
    /// Cancellation is coverage-based, not exact-match: a remove dot is
    /// cancelled when the same replica tombstoned any counter at or above it.
    /// So when an element's tombstones all carry one replica id - which is what
    /// they overwhelmingly do - the whole list collapses to that replica's
    /// highest counter and the per-dot test becomes a single comparison,
    /// reducing the element from O(removes x tombstones) to O(T + R) with no
    /// allocation. An element whose tombstones span several replicas, or whose
    /// list is short, keeps the scan. The shared-replica check is a
    /// <b>precondition</b>: a counter-only test would wrongly cancel a remove
    /// dot on replica B whose counter equals a tombstoned counter on replica A.
    /// </para>
    /// <para>
    /// The caller only asks whether any remove survives, so the walk stops at
    /// the first one rather than counting them all.
    /// </para>
    /// </summary>
    private static bool HasLiveRemove(RwSet set, string key)
    {
        if (!set.Removes.TryGetValue(key, out var removeDots) || removeDots.Count == 0) return false;
        set.Tombstones.TryGetValue(key, out var tomb);
        if (tomb is null || tomb.Count == 0) return true;

        string? sharedReplica = null;
        var coverCounter = long.MinValue;
        if (tomb.Count > TombstoneIndexThreshold && removeDots.Count > 1)
        {
            sharedReplica = SingleReplica(tomb);
            if (sharedReplica is not null)
            {
                for (var i = 0; i < tomb.Count; i++)
                {
                    if (tomb[i].Counter > coverCounter) coverCounter = tomb[i].Counter;
                }
            }
        }

        for (var i = 0; i < removeDots.Count; i++)
        {
            var dot = removeDots[i];
            var covered = sharedReplica is not null
                ? dot.Counter <= coverCounter
                    && string.Equals(dot.ReplicaId, sharedReplica, StringComparison.Ordinal)
                : OrSetDotCompaction.Covers(tomb, in dot);
            if (!covered) return true;
        }

        return false;
    }

    /// <summary>
    /// Tombstone-list length above which the remove-wins test switches from a
    /// linear coverage scan to the collapsed replica-plus-highest-counter test.
    /// Below it the scan wins: the precondition pass has a fixed cost that a
    /// handful of counter-first comparisons does not repay.
    /// </summary>
    private const int TombstoneIndexThreshold = 8;

    /// <summary>
    /// The single replica id every dot in <paramref name="dots"/> carries, or
    /// <see langword="null"/> when the list spans more than one replica (or is
    /// empty). One pass, comparing ordinally and short-circuiting on the
    /// reference the list overwhelmingly repeats.
    /// </summary>
    private static string? SingleReplica(List<OrSetDot> dots)
    {
        if (dots.Count == 0) return null;
        var first = dots[0].ReplicaId;
        for (var i = 1; i < dots.Count; i++)
        {
            var candidate = dots[i].ReplicaId;
            if (!ReferenceEquals(candidate, first)
                && !string.Equals(candidate, first, StringComparison.Ordinal))
            {
                return null;
            }
        }

        return first;
    }

    private static void EmitDots(
        List<CrdtMemberChange> result,
        IReadOnlyList<OrSetDeltaDot>? dots,
        CrdtMemberChangeKind kind,
        HybridLogicalClock? wallClock)
    {
        if (dots is not { Count: > 0 }) return;
        for (var i = 0; i < dots.Count; i++)
        {
            var dot = dots[i];
            if (dot.Element is null) continue;
            result.Add(new CrdtMemberChange
            {
                Element = dot.Element,
                Kind = kind,
                ReplicaId = dot.ReplicaId,
                Ordinal = dot.Counter,
                WallClock = wallClock,
            });
        }
    }

    /// <summary>
    /// Orders two member-change events for the same element by causal ordinal,
    /// then replica id, then kind (an add sorts before the remove that observed
    /// its own dot). Cached as a single shared instance so the per-element sort
    /// never allocates a comparison delegate.
    /// </summary>
    private sealed class CausalOrderComparer : IComparer<CrdtMemberChange>
    {
        public static CausalOrderComparer Instance { get; } = new();

        public int Compare(CrdtMemberChange x, CrdtMemberChange y)
        {
            var byOrdinal = x.Ordinal.CompareTo(y.Ordinal);
            if (byOrdinal != 0) return byOrdinal;
            var byReplica = string.CompareOrdinal(x.ReplicaId, y.ReplicaId);
            if (byReplica != 0) return byReplica;
            return ((int)x.Kind).CompareTo((int)y.Kind);
        }
    }
}
