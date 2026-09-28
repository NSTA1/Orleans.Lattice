using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// <see cref="ICrdtProvenanceDecoder"/> for the observed-remove map shape
/// (<see cref="LatticeMergeMode.OrMap"/>). Turns an
/// <see cref="OrMap{TKey, TValue}"/>'s stored state or a sequence of
/// <see cref="OrMapDelta{TKey, TValue}"/> author deltas into key-level
/// <see cref="CrdtMemberChange"/> events.
/// <para>
/// <strong>Key membership only.</strong> The "member" of an OR-map is a key:
/// an added key maps to an <see cref="CrdtMemberChangeKind.Added"/> event and a
/// tombstoned key to a <see cref="CrdtMemberChangeKind.Removed"/> event, each
/// carrying the authoring dot. The decoder does not recurse into the per-key
/// value CRDTs (their own provenance is out of scope here); only key
/// add/remove membership is surfaced.
/// </para>
/// <para>
/// <strong>Generic shape, reflection-bound once per type.</strong> The map's
/// wire shape is open over the host-supplied <c>(TKey, TValue)</c> pair, which
/// the non-generic decoder contract cannot name at compile time. The decoder
/// binds a strongly-typed emitter per closed <c>(TKey, TValue)</c> the first
/// time it sees one and caches it, so steady-state decode is allocation-light
/// (no per-item boxing); only the one-time per-type delegate creation pays
/// reflection.
/// </para>
/// <para>
/// <strong>Key-to-bytes limitation.</strong> Because the key type is not known
/// to the wire-facing <see cref="CrdtMemberChange.Element"/> (a
/// <see cref="byte"/> array), each key is rendered to its invariant-culture
/// string form and encoded as UTF-8. For <see cref="string"/> keys this is the
/// key verbatim; for other key types it is a stable surrogate that is only as
/// injective as the key's <see cref="object.ToString()"/>. The element is a
/// presentation/identity surrogate for the key, not a round-trippable encoding
/// of it.
/// </para>
/// <para>
/// <strong>Why every dot scan here walks a span.</strong> A key's add and
/// tombstone dots live in a <see cref="List{T}"/> of
/// <see cref="OrSetDot"/>, a struct, so reading one through the list indexer
/// copies the whole struct, bounds-checks the access and re-reads the list's
/// mutable <c>Count</c> on every iteration of the loop condition. Taking
/// <see cref="CollectionsMarshal.AsSpan{T}(List{T})"/> once removes the
/// <c>Count</c> re-read and lets the bounds check hoist. The invariant that
/// licenses it: no scan in this type may change a scanned list's length while
/// its span is alive - every one of them is read-only, appending only to the
/// caller's sink.
/// </para>
/// <para>
/// <strong>Why only some of them hold a <c>ref readonly</c>.</strong> Reading
/// the element through <c>ref readonly</c> also removes the struct copy, but
/// only pays where the loop body makes no call. A byref into the span that is
/// live across a call is an interior pointer the JIT must report to the GC, so
/// it is pinned to a tracked stack slot instead of being enregistered. Measured
/// on the benchmark suite that arbitrated this change, holding one across the
/// emit loops' <c>sink.Add</c> cost more than the 16-byte copy it saved. So the
/// pure scans (<see cref="SingleReplica"/>, <see cref="IsEntryTombstoned"/>,
/// the counter-index fill) read through <c>ref readonly</c>, and every loop
/// whose body calls out copies the element instead.
/// </para>
/// </summary>
public sealed class OrMapProvenanceDecoder : ICrdtProvenanceDecoder
{
    private delegate void DeltaEmitter(object boxed, HybridLogicalClock? wallClock, List<CrdtMemberChange> sink);

    private delegate void StateEmitter(object boxed, List<CrdtMemberChange> sink);

    private delegate void CurrentValueEmitter(object boxed, List<CrdtMemberValue> sink);

    private static readonly ConcurrentDictionary<Type, DeltaEmitter> DeltaEmitters = new();
    private static readonly ConcurrentDictionary<Type, StateEmitter> StateEmitters = new();
    private static readonly ConcurrentDictionary<Type, CurrentValueEmitter> CurrentValueEmitters = new();

    /// <summary>A shared, stateless instance. The decoder holds no per-call state.</summary>
    public static OrMapProvenanceDecoder Instance { get; } = new();

    /// <inheritdoc />
    public LatticeMergeMode Mode => LatticeMergeMode.OrMap;

    /// <summary>
    /// Decodes an ordered <see cref="OrMapDelta{TKey, TValue}"/> sequence into
    /// key-level member-change events in operation order: added keys before
    /// tombstoned keys within a delta, the supplied order across deltas, so a
    /// removed-then-re-added key surfaces both events in causal order. Each
    /// event carries the originating delta's wall-clock stamp when one was
    /// supplied.
    /// </summary>
    /// <param name="deltas">
    /// The ordered author-delta sequence; each entry's <c>Delta</c> must be an
    /// <see cref="OrMapDelta{TKey, TValue}"/> of a single closed
    /// <c>(TKey, TValue)</c> shape.
    /// </param>
    /// <returns>The decoded member-change events, in operation order.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="deltas"/> is <see langword="null"/>.</exception>
    public IReadOnlyList<CrdtMemberChange> DecodeDeltas(IReadOnlyList<CrdtProvenanceDelta> deltas)
    {
        ArgumentNullException.ThrowIfNull(deltas);
        if (deltas.Count == 0) return Array.Empty<CrdtMemberChange>();

        var result = new List<CrdtMemberChange>();
        for (var i = 0; i < deltas.Count; i++)
        {
            var entry = deltas[i];
            var boxed = entry.Delta;
            var emitter = DeltaEmitters.GetOrAdd(boxed.GetType(), CreateDeltaEmitter);
            emitter(boxed, entry.WallClock, result);
        }
        return result.Count == 0 ? Array.Empty<CrdtMemberChange>() : result;
    }

    /// <summary>
    /// Reconstructs key-level member-change events from a folded
    /// <see cref="OrMap{TKey, TValue}"/>: each live per-key dot yields an
    /// <see cref="CrdtMemberChangeKind.Added"/> event and each tombstone dot a
    /// <see cref="CrdtMemberChangeKind.Removed"/> event, ordered
    /// deterministically by key surrogate then replica then causal ordinal then
    /// kind. Because no owning mutation is available,
    /// <see cref="CrdtMemberChange.WallClock"/> is always
    /// <see langword="null"/>.
    /// </summary>
    /// <param name="state">The <see cref="OrMap{TKey, TValue}"/> to decode.</param>
    /// <returns>The reconstructed member-change events.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="state"/> is <see langword="null"/>.</exception>
    public IReadOnlyList<CrdtMemberChange> DecodeState(object state)
    {
        ArgumentNullException.ThrowIfNull(state);
        var emitter = StateEmitters.GetOrAdd(state.GetType(), CreateStateEmitter);
        var result = new List<CrdtMemberChange>();
        emitter(state, result);
        if (result.Count == 0) return Array.Empty<CrdtMemberChange>();
        result.Sort(ElementOrderComparer.Instance);
        return result;
    }

    /// <summary>
    /// Projects a folded <see cref="OrMap{TKey, TValue}"/> into its live keys: one
    /// <see cref="CrdtMemberValue"/> per key that has at least one un-tombstoned
    /// dot, carrying the key surrogate bytes (see the type remarks on the
    /// key-to-bytes limitation) and the provenance of that key's surviving dot
    /// with the highest causal ordinal. Tombstoned (removed) keys are excluded, so
    /// unlike <see cref="DecodeState(object)"/> the projection is exactly the map's
    /// current key set. The decoder does not recurse into the per-key value CRDTs.
    /// Members are ordered by key surrogate bytes.
    /// </summary>
    /// <param name="state">The <see cref="OrMap{TKey, TValue}"/> to project.</param>
    /// <returns>The live keys as current-state members.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="state"/> is <see langword="null"/>.</exception>
    public IReadOnlyList<CrdtMemberValue> DecodeCurrentValue(object state)
    {
        ArgumentNullException.ThrowIfNull(state);
        var emitter = CurrentValueEmitters.GetOrAdd(state.GetType(), CreateCurrentValueEmitter);
        var result = new List<CrdtMemberValue>();
        emitter(state, result);
        if (result.Count == 0) return Array.Empty<CrdtMemberValue>();
        result.Sort(static (x, y) => CompareElementBytes(x.Element, y.Element));
        return result;
    }

    private static CurrentValueEmitter CreateCurrentValueEmitter(Type closedType)
    {
        var args = closedType.GetGenericArguments();
        var method = typeof(OrMapProvenanceDecoder)
            .GetMethod(nameof(EmitCurrentValueTyped), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(args);
        return (CurrentValueEmitter)method.CreateDelegate(typeof(CurrentValueEmitter));
    }

    private static void EmitCurrentValueTyped<TKey, TValue>(object boxed, List<CrdtMemberValue> sink)
        where TKey : notnull
        where TValue : ICrdt<TValue>, new()
    {
        var map = (OrMap<TKey, TValue>)boxed;

        // At most one member per key, so the add-map's free O(1) count is an
        // exact upper bound - taking it up front means the sink never grows.
        sink.EnsureCapacity(sink.Count + map.Adds.Count);

        // A key that has churned accumulates tombstones, and testing each of
        // its live dots by linear scan over that list is O(adds x tombstones)
        // per key. Within one key's tombstone list the dots overwhelmingly
        // share a replica id (the same observation OrSetDot.Equals is ordered
        // around), and when they do the membership test reduces to a counter
        // lookup: a dot whose replica differs from the shared one cannot be in
        // the list at all, and one that matches is in it exactly when its
        // counter is. Above a threshold that lookup is served by sorting the
        // key's counters into a scratch buffer once and binary-searching it,
        // making the test O(T log T + A log T) with no hashing and no
        // allocation. Hashing is deliberately avoided: an index over OrSetDot
        // hashes its replica id, which costs far more than the counter
        // comparison the linear scan leads with, and measured slower than the
        // scan it replaced. A key whose tombstones span several replicas or
        // whose list is short keeps the scan.
        //
        // The buffer is rented lazily, on the first key that qualifies, rather
        // than taken up front: a quiet map qualifies no key at all, and an
        // unconditional stack buffer charges every such call for zeroing a
        // scratch it never reads.
        long[]? rented = null;

        foreach (var (key, entries) in map.Adds)
        {
            if (entries.Count == 0) continue;
            map.Tombstones.TryGetValue(key, out var tomb);

            string? sharedReplica = null;
            var counters = Span<long>.Empty;
            if (tomb is not null
                && tomb.Count > TombstoneIndexThreshold
                && entries.Count > 1)
            {
                sharedReplica = SingleReplica(tomb);
                if (sharedReplica is not null)
                {
                    if (rented is null || rented.Length < tomb.Count)
                    {
                        if (rented is not null) ArrayPool<long>.Shared.Return(rented);
                        rented = ArrayPool<long>.Shared.Rent(tomb.Count);
                    }

                    counters = rented.AsSpan(0, tomb.Count);
                    // Span walk - see the type remarks on why every dot scan
                    // here takes one. The gate above puts this list above the
                    // index threshold and the body only reads.
                    var tombSpan = CollectionsMarshal.AsSpan(tomb);
                    for (var i = 0; i < tombSpan.Length; i++) counters[i] = tombSpan[i].Counter;
                    counters.Sort();
                }
            }

            var hasLive = false;
            var bestReplica = string.Empty;
            var bestCounter = long.MinValue;
            // Span walk: the body only reads, but it can call into
            // BinarySearch or IsEntryTombstoned, so the element is copied
            // rather than held by reference (a byref into the span live across
            // a call is pinned to a GC-tracked stack slot).
            var entrySpan = CollectionsMarshal.AsSpan(entries);
            for (var i = 0; i < entrySpan.Length; i++)
            {
                var entry = entrySpan[i];
                var tombstoned = sharedReplica is not null
                    ? string.Equals(entry.ReplicaId, sharedReplica, StringComparison.Ordinal)
                        && counters.BinarySearch(entry.Counter) >= 0
                    : IsEntryTombstoned(tomb, entry.ReplicaId, entry.Counter);
                if (tombstoned) continue;
                if (!hasLive
                    || entry.Counter > bestCounter
                    || (entry.Counter == bestCounter && string.CompareOrdinal(entry.ReplicaId, bestReplica) > 0))
                {
                    hasLive = true;
                    bestReplica = entry.ReplicaId;
                    bestCounter = entry.Counter;
                }
            }

            if (!hasLive) continue;
            sink.Add(new CrdtMemberValue
            {
                Element = KeyToBytes(key),
                ReplicaId = bestReplica,
                Ordinal = bestCounter,
            });
        }

        if (rented is not null) ArrayPool<long>.Shared.Return(rented);
    }

    /// <summary>
    /// The single replica id every dot in <paramref name="tombstones"/> carries,
    /// or <see langword="null"/> when the list spans more than one replica (or
    /// is empty). One pass, comparing ordinally and short-circuiting on the
    /// reference the list overwhelmingly repeats.
    /// <para>
    /// Internal rather than private only so the benchmark host can A/B the
    /// shipped scan against its pre-span baseline directly.
    /// </para>
    /// </summary>
    internal static string? SingleReplica(List<OrSetDot> tombstones)
    {
        if (tombstones.Count == 0) return null;
        // Span walk - see the type remarks. Callers gate this on a list longer
        // than TombstoneIndexThreshold, and the body only reads.
        var span = CollectionsMarshal.AsSpan(tombstones);
        var first = span[0].ReplicaId;
        for (var i = 1; i < span.Length; i++)
        {
            ref readonly var dot = ref span[i];
            var candidate = dot.ReplicaId;
            if (!ReferenceEquals(candidate, first)
                && !string.Equals(candidate, first, StringComparison.Ordinal))
            {
                return null;
            }
        }

        return first;
    }

    /// <summary>
    /// Tombstone count above which a key's membership test switches from a
    /// linear scan to a sorted counter index. Below it the scan wins: sorting
    /// and binary-searching has a fixed cost that a handful of counter-first
    /// comparisons does not repay.
    /// </summary>
    private const int TombstoneIndexThreshold = 8;

    /// <summary>
    /// Whether <paramref name="tombstones"/> contains the exact
    /// <c>(replicaId, counter)</c> dot. The inner scan of the per-key
    /// membership test, so it runs once per add dot on every key whose
    /// tombstone list misses the counter index.
    /// <para>
    /// Internal rather than private only so the benchmark host can A/B the
    /// shipped scan against its pre-span baseline directly.
    /// </para>
    /// </summary>
    internal static bool IsEntryTombstoned(List<OrSetDot>? tombstones, string replicaId, long counter)
    {
        if (tombstones is null) return false;
        // Span walk - this is the inner scan of the per-key membership test, so
        // it runs once per add dot on every key that misses the counter index.
        // The body only reads.
        var span = CollectionsMarshal.AsSpan(tombstones);
        for (var i = 0; i < span.Length; i++)
        {
            ref readonly var t = ref span[i];
            if (t.Counter == counter && string.Equals(t.ReplicaId, replicaId, StringComparison.Ordinal))
            {
                return true;
            }
        }
        return false;
    }

    private static int CompareElementBytes(byte[] a, byte[] b)
    {
        var min = Math.Min(a.Length, b.Length);
        for (var i = 0; i < min; i++)
        {
            var c = a[i].CompareTo(b[i]);
            if (c != 0) return c;
        }
        return a.Length.CompareTo(b.Length);
    }

    private static DeltaEmitter CreateDeltaEmitter(Type closedType)
    {
        var args = closedType.GetGenericArguments();
        var method = typeof(OrMapProvenanceDecoder)
            .GetMethod(nameof(EmitDeltaTyped), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(args);
        return (DeltaEmitter)method.CreateDelegate(typeof(DeltaEmitter));
    }

    private static StateEmitter CreateStateEmitter(Type closedType)
    {
        var args = closedType.GetGenericArguments();
        var method = typeof(OrMapProvenanceDecoder)
            .GetMethod(nameof(EmitStateTyped), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(args);
        return (StateEmitter)method.CreateDelegate(typeof(StateEmitter));
    }

    private static void EmitDeltaTyped<TKey, TValue>(
        object boxed,
        HybridLogicalClock? wallClock,
        List<CrdtMemberChange> sink)
        where TKey : notnull
        where TValue : ICrdt<TValue>, new()
    {
        var delta = (OrMapDelta<TKey, TValue>)boxed;
        var adds = delta.Adds;
        var tombstones = delta.Tombstones;
        var addCount = adds is null ? 0 : adds.Count;
        var tombCount = tombstones is null ? 0 : tombstones.Count;
        if (addCount + tombCount == 0) return;
        sink.EnsureCapacity(sink.Count + addCount + tombCount);

        if (addCount > 0)
        {
            // A delta's dots arrive grouped by key - a single mutation ships
            // every dot it authored for one key together - so encoding the key
            // surrogate per dot re-runs the same UTF-8 encode (and mints the
            // same array) for every dot after the first in a group. A one-slot
            // memo over the previous key collapses a group to one encode and
            // one array, and degrades to the prior cost when no two adjacent
            // dots share a key. EmitStateTyped already hoists per key; this is
            // the same hoist expressed for a flat, key-carrying list.
            TKey? memoKey = default;
            byte[]? memoBytes = null;
            var comparer = EqualityComparer<TKey>.Default;

            for (var i = 0; i < adds!.Count; i++)
            {
                var add = adds[i];
                if (memoBytes is null || !comparer.Equals(memoKey!, add.Key))
                {
                    memoKey = add.Key;
                    memoBytes = KeyToBytes(add.Key);
                }

                sink.Add(new CrdtMemberChange
                {
                    Element = memoBytes,
                    Kind = CrdtMemberChangeKind.Added,
                    ReplicaId = add.ReplicaId,
                    Ordinal = add.Counter,
                    WallClock = wallClock,
                });
            }
        }

        if (tombCount > 0)
        {
            TKey? memoKey = default;
            byte[]? memoBytes = null;
            var comparer = EqualityComparer<TKey>.Default;

            for (var i = 0; i < tombstones!.Count; i++)
            {
                var tomb = tombstones[i];
                if (memoBytes is null || !comparer.Equals(memoKey!, tomb.Key))
                {
                    memoKey = tomb.Key;
                    memoBytes = KeyToBytes(tomb.Key);
                }

                sink.Add(new CrdtMemberChange
                {
                    Element = memoBytes,
                    Kind = CrdtMemberChangeKind.Removed,
                    ReplicaId = tomb.ReplicaId,
                    Ordinal = tomb.Counter,
                    WallClock = wallClock,
                });
            }
        }
    }

    private static void EmitStateTyped<TKey, TValue>(object boxed, List<CrdtMemberChange> sink)
        where TKey : notnull
        where TValue : ICrdt<TValue>, new()
    {
        var map = (OrMap<TKey, TValue>)boxed;

        // The two dictionary counts are free (an O(1) field read, not a re-scan
        // of their contents) and every non-empty key contributes at least one
        // event, so they are a sound lower bound on the event count. Taking it
        // up front removes the doubling chain the sink would otherwise climb
        // from capacity zero, which for a map of any size is several array
        // allocations and copies before the first useful append.
        sink.EnsureCapacity(sink.Count + map.Adds.Count + map.Tombstones.Count);

        foreach (var (key, entries) in map.Adds)
        {
            if (entries.Count == 0) continue;
            var element = KeyToBytes(key);
            // Span walk: the loop appends to sink, never to entries, so the
            // scanned list's length cannot change while the span is alive. The
            // element is copied rather than held by reference because the body
            // calls into sink.Add.
            var entrySpan = CollectionsMarshal.AsSpan(entries);
            for (var i = 0; i < entrySpan.Length; i++)
            {
                var e = entrySpan[i];
                sink.Add(new CrdtMemberChange
                {
                    Element = element,
                    Kind = CrdtMemberChangeKind.Added,
                    ReplicaId = e.ReplicaId,
                    Ordinal = e.Counter,
                    WallClock = null,
                });
            }
        }

        foreach (var (key, dots) in map.Tombstones)
        {
            if (dots.Count == 0) continue;
            var element = KeyToBytes(key);
            var dotSpan = CollectionsMarshal.AsSpan(dots);
            for (var i = 0; i < dotSpan.Length; i++)
            {
                var dot = dotSpan[i];
                sink.Add(new CrdtMemberChange
                {
                    Element = element,
                    Kind = CrdtMemberChangeKind.Removed,
                    ReplicaId = dot.ReplicaId,
                    Ordinal = dot.Counter,
                    WallClock = null,
                });
            }
        }
    }

    private static byte[] KeyToBytes<TKey>(TKey key)
    {
        var text = key as string ?? Convert.ToString(key, CultureInfo.InvariantCulture) ?? string.Empty;
        return text.Length == 0 ? Array.Empty<byte>() : Encoding.UTF8.GetBytes(text);
    }

    /// <summary>
    /// Orders OR-map member-change events deterministically by key surrogate
    /// (the decoded <see cref="CrdtMemberChange.Element"/> bytes) first, then by
    /// replica, causal ordinal, and kind, so the folded-state projection is
    /// grouped per key and stable across replicas.
    /// </summary>
    private sealed class ElementOrderComparer : IComparer<CrdtMemberChange>
    {
        public static ElementOrderComparer Instance { get; } = new();

        public int Compare(CrdtMemberChange x, CrdtMemberChange y)
        {
            var byElement = CompareBytes(x.Element, y.Element);
            if (byElement != 0) return byElement;
            return CrdtMemberChangeCausalComparer.Instance.Compare(x, y);
        }

        private static int CompareBytes(byte[] a, byte[] b)
        {
            var min = Math.Min(a.Length, b.Length);
            for (var i = 0; i < min; i++)
            {
                var c = a[i].CompareTo(b[i]);
                if (c != 0) return c;
            }
            return a.Length.CompareTo(b.Length);
        }
    }
}
