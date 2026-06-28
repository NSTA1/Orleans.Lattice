using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Reflection;
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
/// </summary>
public sealed class OrMapProvenanceDecoder : ICrdtProvenanceDecoder
{
    private delegate void DeltaEmitter(object boxed, HybridLogicalClock? wallClock, List<CrdtMemberChange> sink);

    private delegate void StateEmitter(object boxed, List<CrdtMemberChange> sink);

    private static readonly ConcurrentDictionary<Type, DeltaEmitter> DeltaEmitters = new();
    private static readonly ConcurrentDictionary<Type, StateEmitter> StateEmitters = new();

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
            for (var i = 0; i < adds!.Count; i++)
            {
                var add = adds[i];
                sink.Add(new CrdtMemberChange
                {
                    Element = KeyToBytes(add.Key),
                    Kind = CrdtMemberChangeKind.Added,
                    ReplicaId = add.ReplicaId,
                    Ordinal = add.Counter,
                    WallClock = wallClock,
                });
            }
        }

        if (tombCount > 0)
        {
            for (var i = 0; i < tombstones!.Count; i++)
            {
                var tomb = tombstones[i];
                sink.Add(new CrdtMemberChange
                {
                    Element = KeyToBytes(tomb.Key),
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

        foreach (var (key, entries) in map.Adds)
        {
            if (entries.Count == 0) continue;
            var element = KeyToBytes(key);
            for (var i = 0; i < entries.Count; i++)
            {
                var e = entries[i];
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
            for (var i = 0; i < dots.Count; i++)
            {
                var dot = dots[i];
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
