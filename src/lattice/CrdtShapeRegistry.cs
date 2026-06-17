using System.Buffers;
using System.Collections.Concurrent;
using System.Text.Json;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// Type-erased descriptor for one concrete CRDT shape. Carries the
/// bytes-to-state and bytes-to-delta deserialisers plus a merge action
/// that folds the typed delta into the loaded state, the empty-state
/// constructor used when a key has not yet been observed, and the
/// state serialiser used by snapshot capture. Constructed by the
/// per-mode factory helpers (such as
/// <see cref="OrSet"/>, <see cref="PnCounter"/>, <see cref="VersionVector"/>,
/// <see cref="MvRegister"/> on the descriptor itself, or the generic
/// <c>OrMap&lt;TKey, TValue&gt;</c> branch); not intended for direct
/// host construction. One descriptor lives per <c>(treeId, mode)</c>
/// slot in the <see cref="CrdtShapeRegistry"/>.
/// </summary>
public sealed class CrdtShape
{
    /// <summary>The CRDT mode this descriptor applies to.</summary>
    public LatticeMergeMode Mode { get; }

    /// <summary>Deserialises full-state bytes into a typed primitive instance.</summary>
    public Func<byte[], object> DeserializeState { get; }

    /// <summary>Deserialises typed delta DTO bytes into a typed delta instance.</summary>
    public Func<byte[], object> DeserializeDelta { get; }

    /// <summary>Folds a deserialised typed delta into a deserialised typed state.</summary>
    public Action<object, object> MergeDelta { get; }

    /// <summary>Folds another deserialised typed state into a deserialised typed state.</summary>
    public Action<object, object> MergeStates { get; }

    /// <summary>Constructs an empty typed state instance for the "key absent" case.</summary>
    public Func<object> CreateEmpty { get; }

    /// <summary>Serialises a typed state instance back to bytes for snapshot capture.</summary>
    public Func<object, byte[]> SerializeState { get; }

    /// <summary>
    /// Streams the full-state serialisation of a typed instance into a
    /// caller-supplied <see cref="IBufferWriter{T}"/>, producing bytes
    /// byte-identical to <see cref="SerializeState"/> but without
    /// allocating an intermediate <c>byte[]</c>. <see langword="null"/>
    /// for shapes whose serialiser does not expose a streaming lane
    /// (currently the reflection-based <see cref="LatticeMergeMode.Sequence"/>
    /// and <see cref="LatticeMergeMode.OrMap"/> shapes); callers fall back
    /// to <see cref="SerializeState"/> in that case. Used by the leaf
    /// grain's deferred CRDT-apply path to feed the projection-digest fold
    /// from a reused buffer so the per-apply allocation stays flat in the
    /// post-merge state size instead of scaling with it.
    /// </summary>
    internal Action<object, IBufferWriter<byte>>? SerializeStateInto { get; init; }

    /// <summary>
    /// Serialises a typed delta instance back to bytes, the inverse of
    /// <see cref="DeserializeDelta"/>. <see langword="null"/> for shapes
    /// that do not support pre-ship delta coalescing (those whose
    /// <see cref="CombineDeltas"/> is also <see langword="null"/>). Used
    /// by the sender-side coalescing pass to re-encode a combined delta
    /// onto the wire.
    /// </summary>
    public Func<object, byte[]>? SerializeDelta { get; }

    /// <summary>
    /// Associatively folds two deserialised typed deltas into a single
    /// combined delta whose receiver-side apply effect is identical to
    /// applying the two source deltas in sequence. The operation mirrors
    /// the primitive's own join semilattice (union for observed-remove
    /// adds / removes, pointwise-max for counters and version vectors,
    /// dot-dominance merge for the multi-value register, union of the dot-
    /// tagged adds / tombstones with same-dot value snapshots lattice-
    /// merged through the value CRDT for the OR-Map), so it is
    /// commutative, associative, and idempotent. <see langword="null"/>
    /// for shapes that do not support pre-ship delta coalescing, in which
    /// case the sender ships the source deltas individually rather than
    /// combining them.
    /// </summary>
    public Func<object, object, object>? CombineDeltas { get; }

    /// <summary>Initialises a new <see cref="CrdtShape"/>.</summary>
    public CrdtShape(
        LatticeMergeMode mode,
        Func<byte[], object> deserializeState,
        Func<byte[], object> deserializeDelta,
        Action<object, object> mergeDelta,
        Action<object, object> mergeStates,
        Func<object> createEmpty,
        Func<object, byte[]> serializeState,
        Func<object, byte[]>? serializeDelta = null,
        Func<object, object, object>? combineDeltas = null)
    {
        ArgumentNullException.ThrowIfNull(deserializeState);
        ArgumentNullException.ThrowIfNull(deserializeDelta);
        ArgumentNullException.ThrowIfNull(mergeDelta);
        ArgumentNullException.ThrowIfNull(mergeStates);
        ArgumentNullException.ThrowIfNull(createEmpty);
        ArgumentNullException.ThrowIfNull(serializeState);
        Mode = mode;
        DeserializeState = deserializeState;
        DeserializeDelta = deserializeDelta;
        MergeDelta = mergeDelta;
        MergeStates = mergeStates;
        CreateEmpty = createEmpty;
        SerializeState = serializeState;
        SerializeDelta = serializeDelta;
        CombineDeltas = combineDeltas;
    }

    /// <summary>Factory for the <see cref="LatticeMergeMode.OrSet"/> shape.</summary>
    public static CrdtShape ForOrSet()
    {
        var ctx = CrdtJsonSerializerContext.Default;
        return new CrdtShape(
            LatticeMergeMode.OrSet,
            bytes => JsonSerializer.Deserialize(bytes, ctx.OrSet)!,
            bytes => JsonSerializer.Deserialize(bytes, ctx.OrSetDelta),
            (state, delta) => ((OrSet)state).MergeDelta((OrSetDelta)delta),
            (state, other) => ((OrSet)state).MergeFrom((OrSet)other),
            () => new OrSet(),
            state => JsonSerializer.SerializeToUtf8Bytes((OrSet)state, ctx.OrSet),
            delta => JsonSerializer.SerializeToUtf8Bytes((OrSetDelta)delta, ctx.OrSetDelta),
            static (a, b) => CombineOrSetDelta((OrSetDelta)a, (OrSetDelta)b))
        {
            SerializeStateInto = static (state, writer) =>
            {
                using var w = new Utf8JsonWriter(writer);
                JsonSerializer.Serialize(w, (OrSet)state, CrdtJsonSerializerContext.Default.OrSet);
            },
        };
    }

    /// <summary>Factory for the <see cref="LatticeMergeMode.PnCounter"/> shape.</summary>
    public static CrdtShape ForPnCounter()
    {
        var ctx = CrdtJsonSerializerContext.Default;
        return new CrdtShape(
            LatticeMergeMode.PnCounter,
            bytes => JsonSerializer.Deserialize(bytes, ctx.PnCounter)!,
            bytes => JsonSerializer.Deserialize(bytes, ctx.PnCounterDelta),
            (state, delta) => ((PnCounter)state).MergeDelta((PnCounterDelta)delta),
            (state, other) => ((PnCounter)state).MergeFrom((PnCounter)other),
            () => new PnCounter(),
            state => JsonSerializer.SerializeToUtf8Bytes((PnCounter)state, ctx.PnCounter),
            delta => JsonSerializer.SerializeToUtf8Bytes((PnCounterDelta)delta, ctx.PnCounterDelta),
            static (a, b) => CombinePnCounterDelta((PnCounterDelta)a, (PnCounterDelta)b))
        {
            SerializeStateInto = static (state, writer) =>
            {
                using var w = new Utf8JsonWriter(writer);
                JsonSerializer.Serialize(w, (PnCounter)state, CrdtJsonSerializerContext.Default.PnCounter);
            },
        };
    }

    /// <summary>Factory for the <see cref="LatticeMergeMode.VersionVector"/> shape.</summary>
    public static CrdtShape ForVersionVector()
    {
        var ctx = CrdtJsonSerializerContext.Default;
        return new CrdtShape(
            LatticeMergeMode.VersionVector,
            bytes => JsonSerializer.Deserialize(bytes, ctx.VersionVector)!,
            bytes => JsonSerializer.Deserialize(bytes, ctx.VersionVectorDelta),
            (state, delta) => ((VersionVector)state).MergeDelta((VersionVectorDelta)delta),
            (state, other) => ((VersionVector)state).MergeFrom((VersionVector)other),
            () => new VersionVector(),
            state => JsonSerializer.SerializeToUtf8Bytes((VersionVector)state, ctx.VersionVector),
            delta => JsonSerializer.SerializeToUtf8Bytes((VersionVectorDelta)delta, ctx.VersionVectorDelta),
            static (a, b) => CombineVersionVectorDelta((VersionVectorDelta)a, (VersionVectorDelta)b))
        {
            SerializeStateInto = static (state, writer) =>
            {
                using var w = new Utf8JsonWriter(writer);
                JsonSerializer.Serialize(w, (VersionVector)state, CrdtJsonSerializerContext.Default.VersionVector);
            },
        };
    }

    /// <summary>Factory for the <see cref="LatticeMergeMode.MvRegister"/> shape.</summary>
    public static CrdtShape ForMvRegister()
    {
        var ctx = CrdtJsonSerializerContext.Default;
        return new CrdtShape(
            LatticeMergeMode.MvRegister,
            bytes => JsonSerializer.Deserialize(bytes, ctx.MvRegister)!,
            bytes => JsonSerializer.Deserialize(bytes, ctx.MvRegisterDelta),
            (state, delta) => ((MvRegister)state).MergeDelta((MvRegisterDelta)delta),
            (state, other) => ((MvRegister)state).MergeFrom((MvRegister)other),
            () => new MvRegister(),
            state => JsonSerializer.SerializeToUtf8Bytes((MvRegister)state, ctx.MvRegister),
            delta => JsonSerializer.SerializeToUtf8Bytes((MvRegisterDelta)delta, ctx.MvRegisterDelta),
            static (a, b) => CombineMvRegisterDelta((MvRegisterDelta)a, (MvRegisterDelta)b))
        {
            SerializeStateInto = static (state, writer) =>
            {
                using var w = new Utf8JsonWriter(writer);
                JsonSerializer.Serialize(w, (MvRegister)state, CrdtJsonSerializerContext.Default.MvRegister);
            },
        };
    }

    /// <summary>Factory for the <see cref="LatticeMergeMode.OrFlag"/> shape.</summary>
    public static CrdtShape ForOrFlag()
    {
        var ctx = CrdtJsonSerializerContext.Default;
        return new CrdtShape(
            LatticeMergeMode.OrFlag,
            bytes => JsonSerializer.Deserialize(bytes, ctx.OrFlag)!,
            bytes => JsonSerializer.Deserialize(bytes, ctx.OrFlagDelta),
            (state, delta) => ((OrFlag)state).MergeDelta((OrFlagDelta)delta),
            (state, other) => ((OrFlag)state).MergeFrom((OrFlag)other),
            () => new OrFlag(),
            state => JsonSerializer.SerializeToUtf8Bytes((OrFlag)state, ctx.OrFlag),
            delta => JsonSerializer.SerializeToUtf8Bytes((OrFlagDelta)delta, ctx.OrFlagDelta),
            static (a, b) => CombineOrFlagDelta((OrFlagDelta)a, (OrFlagDelta)b))
        {
            SerializeStateInto = static (state, writer) =>
            {
                using var w = new Utf8JsonWriter(writer);
                JsonSerializer.Serialize(w, (OrFlag)state, CrdtJsonSerializerContext.Default.OrFlag);
            },
        };
    }

    /// <summary>Factory for the <see cref="LatticeMergeMode.RwFlag"/> shape.</summary>
    public static CrdtShape ForRwFlag()
    {
        var ctx = CrdtJsonSerializerContext.Default;
        return new CrdtShape(
            LatticeMergeMode.RwFlag,
            bytes => JsonSerializer.Deserialize(bytes, ctx.RwFlag)!,
            bytes => JsonSerializer.Deserialize(bytes, ctx.RwFlagDelta),
            (state, delta) => ((RwFlag)state).MergeDelta((RwFlagDelta)delta),
            (state, other) => ((RwFlag)state).MergeFrom((RwFlag)other),
            () => new RwFlag(),
            state => JsonSerializer.SerializeToUtf8Bytes((RwFlag)state, ctx.RwFlag),
            delta => JsonSerializer.SerializeToUtf8Bytes((RwFlagDelta)delta, ctx.RwFlagDelta),
            static (a, b) => CombineRwFlagDelta((RwFlagDelta)a, (RwFlagDelta)b))
        {
            SerializeStateInto = static (state, writer) =>
            {
                using var w = new Utf8JsonWriter(writer);
                JsonSerializer.Serialize(w, (RwFlag)state, CrdtJsonSerializerContext.Default.RwFlag);
            },
        };
    }

    /// <summary>
    /// Factory for the <see cref="LatticeMergeMode.Sequence"/> shape. Uses
    /// the reflection serialiser (matching the receiver-side typed-delta
    /// path and the <see cref="RgaAccessor{T}"/> state read/write seam)
    /// rather than the source-generated context, so the persisted byte[]
    /// row and the wire delta stay on a single (de)serialisation lane.
    /// </summary>
    public static CrdtShape ForRga()
    {
        var s = JsonLatticeSerializer<Rga>.Default;
        var d = JsonLatticeSerializer<RgaDelta>.Default;
        return new CrdtShape(
            LatticeMergeMode.Sequence,
            bytes => s.Deserialize(bytes),
            bytes => d.Deserialize(bytes),
            (state, delta) => ((Rga)state).MergeDelta((RgaDelta)delta),
            (state, other) => ((Rga)state).MergeFrom((Rga)other),
            () => new Rga(),
            state => s.Serialize((Rga)state),
            delta => d.Serialize((RgaDelta)delta),
            static (a, b) => CombineRgaDelta((RgaDelta)a, (RgaDelta)b));
    }

    /// <summary>
    /// Factory for the generic <see cref="LatticeMergeMode.OrMap"/> shape
    /// over a concrete <c>(TKey, TValue)</c> pair. Hosts that configure a
    /// tree for <see cref="LatticeMergeMode.OrMap"/> register the matching
    /// pair via
    /// <see cref="LatticeServiceCollectionExtensions.AddOrMapShape{TKey, TValue}(ISiloBuilder, string)"/>.
    /// <para>
    /// The OR-Map shape now folds same-key delta runs the same way the
    /// closed shapes do: it unions the dot-tagged adds and tombstones and
    /// lattice-merges any same-dot value snapshots through the value CRDT's
    /// own <see cref="ICrdt{TSelf}.MergeFrom(TSelf)"/>. Because the concrete
    /// generic parameters are bound here at construction time, the
    /// <see cref="CombineDeltas"/> lambda can recurse into <c>TValue</c>'s
    /// own join, so registered OR-Map trees get the same pre-ship coalescing
    /// bandwidth saving as the closed primitives. The loss-free ship-
    /// individually fall-back still applies when a tree's OR-Map shape is
    /// unregistered (the registry returns no descriptor) or an entry carries
    /// an opaque (null) delta.
    /// </para>
    /// </summary>
    public static CrdtShape ForOrMap<TKey, TValue>()
        where TKey : notnull
        where TValue : ICrdt<TValue>, new()
    {
        var s = JsonLatticeSerializer<OrMap<TKey, TValue>>.Default;
        var d = JsonLatticeSerializer<OrMapDelta<TKey, TValue>>.Default;
        return new CrdtShape(
            LatticeMergeMode.OrMap,
            bytes => s.Deserialize(bytes),
            bytes => d.Deserialize(bytes),
            (state, delta) => ((OrMap<TKey, TValue>)state).MergeDelta((OrMapDelta<TKey, TValue>)delta),
            (state, other) => ((OrMap<TKey, TValue>)state).MergeFrom((OrMap<TKey, TValue>)other),
            () => new OrMap<TKey, TValue>(),
            state => s.Serialize((OrMap<TKey, TValue>)state),
            delta => d.Serialize((OrMapDelta<TKey, TValue>)delta),
            combineDeltas: static (a, b) =>
                CombineOrMapDelta<TKey, TValue>((OrMapDelta<TKey, TValue>)a, (OrMapDelta<TKey, TValue>)b));
    }

    // --- Delta-combine helpers (pre-ship coalescing) ----------------------------
    //
    // Each helper folds two source deltas into a single combined delta whose
    // receiver-side apply effect equals applying the two in sequence. The
    // operations mirror each primitive's join semilattice, so they are
    // commutative, associative, and idempotent: the sender may combine an
    // arbitrary same-key run in any order and ship the result once.

    private static OrSetDelta CombineOrSetDelta(OrSetDelta a, OrSetDelta b) => new()
    {
        // Observed-remove adds / removes are grow-only dot sets; the union
        // of the two deltas' dot sets reproduces applying both in sequence.
        Adds = UnionOrSetDeltaDots(a.Adds, b.Adds),
        Removes = UnionOrSetDeltaDots(a.Removes, b.Removes),
    };

    private static PnCounterDelta CombinePnCounterDelta(PnCounterDelta a, PnCounterDelta b) => new()
    {
        // Per-replica cumulative components merge by pointwise-max: each
        // delta carries the highest count observed from a replica, never an
        // increment to sum.
        Increments = PointwiseMaxLong(a.Increments, b.Increments),
        Decrements = PointwiseMaxLong(a.Decrements, b.Decrements),
    };

    private static VersionVectorDelta CombineVersionVectorDelta(VersionVectorDelta a, VersionVectorDelta b) => new()
    {
        // Version vectors merge by pointwise-max per replica entry.
        Entries = PointwiseMaxHlc(a.Entries, b.Entries),
    };

    private static MvRegisterDelta CombineMvRegisterDelta(MvRegisterDelta a, MvRegisterDelta b)
    {
        // The multi-value register's merge resolves dot dominance (a later
        // write supersedes the earlier entries its context observed), which
        // a naive entry concat would get wrong. Reuse the primitive's own
        // MergeFrom against transient registers built from the two deltas,
        // then read the post-merge live entries + dot context back out as
        // the combined delta - structurally identical to the delta shape.
        var left = ToMvRegister(a);
        left.MergeFrom(ToMvRegister(b));
        return new MvRegisterDelta
        {
            Entries = left.Entries.ToArray(),
            Context = new Dictionary<string, long>(left.Context, StringComparer.Ordinal),
        };
    }

    private static RgaDelta CombineRgaDelta(RgaDelta a, RgaDelta b) => new()
    {
        // RGA inserts (keyed by dot) and tombstones (dots) are both grow-
        // only sets; the union of the two deltas reproduces applying both.
        Inserts = UnionRgaInserts(a.Inserts, b.Inserts),
        Tombstones = UnionOrSetDots(a.Tombstones, b.Tombstones),
    };

    private static OrFlagDelta CombineOrFlagDelta(OrFlagDelta a, OrFlagDelta b) => new()
    {
        // Enable / disable dot sets are grow-only; the union of the two
        // deltas' dot sets reproduces applying both in sequence.
        Enables = UnionOrSetDots(a.Enables, b.Enables),
        Disables = UnionOrSetDots(a.Disables, b.Disables),
    };

    private static RwFlagDelta CombineRwFlagDelta(RwFlagDelta a, RwFlagDelta b) => new()
    {
        // Enable / disable / tombstone dot sets are grow-only; the union of
        // the two deltas' dot sets reproduces applying both in sequence.
        Enables = UnionOrSetDots(a.Enables, b.Enables),
        Disables = UnionOrSetDots(a.Disables, b.Disables),
        Tombstones = UnionOrSetDots(a.Tombstones, b.Tombstones),
    };

    private static OrMapDelta<TKey, TValue> CombineOrMapDelta<TKey, TValue>(
        OrMapDelta<TKey, TValue> a, OrMapDelta<TKey, TValue> b)
        where TKey : notnull
        where TValue : ICrdt<TValue>, new()
    {
        // Mirror OrMap.MergeFrom on the delta shape: union the dot-tagged
        // adds keyed by the (key, replicaId, counter) dot, lattice-merging
        // same-dot value snapshots through the value CRDT's own join, then
        // union the tombstones deduped by the same dot. The value join is
        // commutative, associative, and idempotent, and dot union is set
        // union, so the combined delta's receiver-side apply effect equals
        // applying the two source deltas in sequence.
        var addsByDot = new Dictionary<(TKey Key, string ReplicaId, long Counter), OrMapDeltaEntry<TKey, TValue>>();
        var addsOrder = new List<(TKey Key, string ReplicaId, long Counter)>();
        AppendOrMapAdds(a.Adds, addsByDot, addsOrder);
        AppendOrMapAdds(b.Adds, addsByDot, addsOrder);

        var adds = addsOrder.Count == 0
            ? System.Array.Empty<OrMapDeltaEntry<TKey, TValue>>()
            : BuildOrderedAdds(addsByDot, addsOrder);

        var tombstoneSeen = new HashSet<(TKey Key, string ReplicaId, long Counter)>();
        var tombstones = new List<OrMapDeltaTombstone<TKey>>(
            (a.Tombstones?.Count ?? 0) + (b.Tombstones?.Count ?? 0));
        AppendOrMapTombstones(a.Tombstones, tombstones, tombstoneSeen);
        AppendOrMapTombstones(b.Tombstones, tombstones, tombstoneSeen);

        return new OrMapDelta<TKey, TValue>
        {
            Adds = adds,
            Tombstones = tombstones.Count == 0
                ? System.Array.Empty<OrMapDeltaTombstone<TKey>>()
                : tombstones,
        };
    }

    private static void AppendOrMapAdds<TKey, TValue>(
        IReadOnlyList<OrMapDeltaEntry<TKey, TValue>>? source,
        Dictionary<(TKey Key, string ReplicaId, long Counter), OrMapDeltaEntry<TKey, TValue>> byDot,
        List<(TKey Key, string ReplicaId, long Counter)> order)
        where TKey : notnull
        where TValue : ICrdt<TValue>, new()
    {
        if (source is null)
        {
            return;
        }
        foreach (var entry in source)
        {
            var replicaId = entry.ReplicaId ?? string.Empty;
            var dot = (entry.Key, replicaId, entry.Counter);
            if (byDot.TryGetValue(dot, out var stored))
            {
                // Same dot from both sides: lattice-merge the incoming
                // value into the stored clone. The clone is a reference,
                // so mutating it in place is sufficient - the dictionary
                // already holds the merged entry.
                stored.Value.MergeFrom(entry.Value);
            }
            else
            {
                // First occurrence: store a CLONED value so a later same-
                // dot merge never mutates the source delta's snapshot.
                var clone = new TValue();
                clone.MergeFrom(entry.Value);
                byDot[dot] = new OrMapDeltaEntry<TKey, TValue>
                {
                    Key = entry.Key,
                    ReplicaId = replicaId,
                    Counter = entry.Counter,
                    Value = clone,
                };
                order.Add(dot);
            }
        }
    }

    private static OrMapDeltaEntry<TKey, TValue>[] BuildOrderedAdds<TKey, TValue>(
        Dictionary<(TKey Key, string ReplicaId, long Counter), OrMapDeltaEntry<TKey, TValue>> byDot,
        List<(TKey Key, string ReplicaId, long Counter)> order)
        where TKey : notnull
        where TValue : ICrdt<TValue>, new()
    {
        var adds = new OrMapDeltaEntry<TKey, TValue>[order.Count];
        for (var i = 0; i < order.Count; i++)
        {
            adds[i] = byDot[order[i]];
        }
        return adds;
    }

    private static void AppendOrMapTombstones<TKey>(
        IReadOnlyList<OrMapDeltaTombstone<TKey>>? source,
        List<OrMapDeltaTombstone<TKey>> result,
        HashSet<(TKey Key, string ReplicaId, long Counter)> seen)
        where TKey : notnull
    {
        if (source is null)
        {
            return;
        }
        foreach (var tombstone in source)
        {
            if (seen.Add((tombstone.Key, tombstone.ReplicaId ?? string.Empty, tombstone.Counter)))
            {
                result.Add(tombstone);
            }
        }
    }

    private static MvRegister ToMvRegister(MvRegisterDelta delta)
    {
        var register = new MvRegister();
        var entries = delta.Entries;
        if (entries is { Count: > 0 })
        {
            register.Entries.Capacity = entries.Count;
            foreach (var entry in entries)
            {
                register.Entries.Add(entry);
            }
        }
        var context = delta.Context;
        if (context is { Count: > 0 })
        {
            foreach (var (replicaId, counter) in context)
            {
                register.Context[replicaId] = counter;
            }
        }
        return register;
    }

    private static IReadOnlyList<OrSetDeltaDot> UnionOrSetDeltaDots(
        IReadOnlyList<OrSetDeltaDot>? a,
        IReadOnlyList<OrSetDeltaDot>? b)
    {
        var result = new List<OrSetDeltaDot>((a?.Count ?? 0) + (b?.Count ?? 0));
        var seen = new HashSet<(string ReplicaId, long Counter, string Element)>();
        AppendOrSetDeltaDots(a, result, seen);
        AppendOrSetDeltaDots(b, result, seen);
        return result;
    }

    private static void AppendOrSetDeltaDots(
        IReadOnlyList<OrSetDeltaDot>? source,
        List<OrSetDeltaDot> result,
        HashSet<(string ReplicaId, long Counter, string Element)> seen)
    {
        if (source is null)
        {
            return;
        }
        foreach (var dot in source)
        {
            var element = dot.Element is null ? string.Empty : Convert.ToBase64String(dot.Element);
            if (seen.Add((dot.ReplicaId ?? string.Empty, dot.Counter, element)))
            {
                result.Add(dot);
            }
        }
    }

    private static IReadOnlyList<RgaDeltaNode> UnionRgaInserts(
        IReadOnlyList<RgaDeltaNode>? a,
        IReadOnlyList<RgaDeltaNode>? b)
    {
        var result = new List<RgaDeltaNode>((a?.Count ?? 0) + (b?.Count ?? 0));
        var seen = new HashSet<OrSetDot>();
        AppendRgaInserts(a, result, seen);
        AppendRgaInserts(b, result, seen);
        return result;
    }

    private static void AppendRgaInserts(
        IReadOnlyList<RgaDeltaNode>? source,
        List<RgaDeltaNode> result,
        HashSet<OrSetDot> seen)
    {
        if (source is null)
        {
            return;
        }
        foreach (var node in source)
        {
            if (seen.Add(node.Dot))
            {
                result.Add(node);
            }
        }
    }

    private static IReadOnlyList<OrSetDot> UnionOrSetDots(
        IReadOnlyList<OrSetDot>? a,
        IReadOnlyList<OrSetDot>? b)
    {
        var result = new List<OrSetDot>((a?.Count ?? 0) + (b?.Count ?? 0));
        var seen = new HashSet<OrSetDot>();
        if (a is not null)
        {
            foreach (var dot in a)
            {
                if (seen.Add(dot))
                {
                    result.Add(dot);
                }
            }
        }
        if (b is not null)
        {
            foreach (var dot in b)
            {
                if (seen.Add(dot))
                {
                    result.Add(dot);
                }
            }
        }
        return result;
    }

    private static Dictionary<string, long> PointwiseMaxLong(
        Dictionary<string, long>? a,
        Dictionary<string, long>? b)
    {
        var result = a is null
            ? new Dictionary<string, long>(b?.Count ?? 0, StringComparer.Ordinal)
            : new Dictionary<string, long>(a, StringComparer.Ordinal);
        if (b is not null)
        {
            foreach (var (key, value) in b)
            {
                if (!result.TryGetValue(key, out var existing) || value > existing)
                {
                    result[key] = value;
                }
            }
        }
        return result;
    }

    private static Dictionary<string, HybridLogicalClock> PointwiseMaxHlc(
        Dictionary<string, HybridLogicalClock>? a,
        Dictionary<string, HybridLogicalClock>? b)
    {
        var result = a is null
            ? new Dictionary<string, HybridLogicalClock>(b?.Count ?? 0, StringComparer.Ordinal)
            : new Dictionary<string, HybridLogicalClock>(a, StringComparer.Ordinal);
        if (b is not null)
        {
            foreach (var (key, value) in b)
            {
                if (!result.TryGetValue(key, out var existing) || value.CompareTo(existing) > 0)
                {
                    result[key] = value;
                }
            }
        }
        return result;
    }
}

/// <summary>
/// Registry of typed CRDT shapes keyed by <c>(treeId, mode)</c>, with
/// a per-mode global fallback for the closed-shape modes
/// (<see cref="LatticeMergeMode.OrSet"/>, <see cref="LatticeMergeMode.PnCounter"/>,
/// <see cref="LatticeMergeMode.VersionVector"/>, <see cref="LatticeMergeMode.MvRegister"/>,
/// <see cref="LatticeMergeMode.Sequence"/>)
/// whose descriptor is unambiguous and does not depend on host generics.
/// Only <see cref="LatticeMergeMode.OrMap"/> requires per-tree
/// registration because it is generic over <c>(TKey, TValue)</c>.
/// Producer-side accessors and receiver-side appliers both consult the
/// same registry so the typed deserialise + merge runs through a single
/// type-erased seam.
/// </summary>
public sealed class CrdtShapeRegistry
{
    private readonly ConcurrentDictionary<(string TreeId, LatticeMergeMode Mode), CrdtShape> _perTree =
        new();
    private readonly ConcurrentDictionary<LatticeMergeMode, CrdtShape> _global = new();

    /// <summary>
    /// Initialises a new registry pre-populated with the closed-shape
    /// descriptors (<see cref="LatticeMergeMode.OrSet"/>,
    /// <see cref="LatticeMergeMode.PnCounter"/>,
    /// <see cref="LatticeMergeMode.VersionVector"/>,
    /// <see cref="LatticeMergeMode.MvRegister"/>,
    /// <see cref="LatticeMergeMode.Sequence"/>). Hosts add their
    /// <see cref="LatticeMergeMode.OrMap"/> descriptors per tree.
    /// </summary>
    public CrdtShapeRegistry()
    {
        _global[LatticeMergeMode.OrSet] = CrdtShape.ForOrSet();
        _global[LatticeMergeMode.PnCounter] = CrdtShape.ForPnCounter();
        _global[LatticeMergeMode.VersionVector] = CrdtShape.ForVersionVector();
        _global[LatticeMergeMode.MvRegister] = CrdtShape.ForMvRegister();
        _global[LatticeMergeMode.Sequence] = CrdtShape.ForRga();
        _global[LatticeMergeMode.OrFlag] = CrdtShape.ForOrFlag();
        _global[LatticeMergeMode.RwFlag] = CrdtShape.ForRwFlag();
    }

    /// <summary>
    /// Registers <paramref name="shape"/> as the descriptor for the
    /// tree identified by <paramref name="treeId"/> at the
    /// <paramref name="shape"/>'s declared mode. Throws when a
    /// different shape is already registered for the same slot.
    /// </summary>
    public void Register(string treeId, CrdtShape shape)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(shape);
        var key = (treeId, shape.Mode);
        if (!_perTree.TryAdd(key, shape))
        {
            var existing = _perTree[key];
            if (!ReferenceEquals(existing, shape))
            {
                throw new InvalidOperationException(
                    $"A CrdtShape for mode {shape.Mode} is already registered for tree '{treeId}'. " +
                    "Each (tree, mode) slot may carry at most one descriptor; remove the duplicate registration.");
            }
        }
    }

    /// <summary>
    /// Resolves the descriptor for <paramref name="treeId"/> at
    /// <paramref name="mode"/>. Per-tree registrations win over the
    /// per-mode global defaults so an OR-Map registration for the
    /// same tree id never collides with the global closed-shape
    /// descriptors. Returns <c>null</c> when no descriptor is
    /// available (only possible for <see cref="LatticeMergeMode.OrMap"/>
    /// on a tree that has not been registered). Callers fault the
    /// producer / apply path on <c>null</c> so the misconfiguration
    /// surfaces rather than silently dropping the entry.
    /// </summary>
    public CrdtShape? TryGet(string treeId, LatticeMergeMode mode)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        if (_perTree.TryGetValue((treeId, mode), out var perTree))
        {
            return perTree;
        }
        return _global.TryGetValue(mode, out var global) ? global : null;
    }
}
