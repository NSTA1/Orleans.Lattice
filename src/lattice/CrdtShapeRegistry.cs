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

    /// <summary>Initialises a new <see cref="CrdtShape"/>.</summary>
    public CrdtShape(
        LatticeMergeMode mode,
        Func<byte[], object> deserializeState,
        Func<byte[], object> deserializeDelta,
        Action<object, object> mergeDelta,
        Action<object, object> mergeStates,
        Func<object> createEmpty,
        Func<object, byte[]> serializeState)
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
            state => JsonSerializer.SerializeToUtf8Bytes((OrSet)state, ctx.OrSet));
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
            state => JsonSerializer.SerializeToUtf8Bytes((PnCounter)state, ctx.PnCounter));
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
            state => JsonSerializer.SerializeToUtf8Bytes((VersionVector)state, ctx.VersionVector));
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
            state => JsonSerializer.SerializeToUtf8Bytes((MvRegister)state, ctx.MvRegister));
    }

    /// <summary>
    /// Factory for the generic <see cref="LatticeMergeMode.OrMap"/> shape
    /// over a concrete <c>(TKey, TValue)</c> pair. Hosts that configure a
    /// tree for <see cref="LatticeMergeMode.OrMap"/> register the matching
    /// pair via
    /// <see cref="LatticeServiceCollectionExtensions.AddOrMapShape{TKey, TValue}(ISiloBuilder, string)"/>.
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
            state => s.Serialize((OrMap<TKey, TValue>)state));
    }
}

/// <summary>
/// Registry of typed CRDT shapes keyed by <c>(treeId, mode)</c>, with
/// a per-mode global fallback for the closed-shape modes
/// (<see cref="LatticeMergeMode.OrSet"/>, <see cref="LatticeMergeMode.PnCounter"/>,
/// <see cref="LatticeMergeMode.VersionVector"/>, <see cref="LatticeMergeMode.MvRegister"/>)
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
    /// <see cref="LatticeMergeMode.MvRegister"/>). Hosts add their
    /// <see cref="LatticeMergeMode.OrMap"/> descriptors per tree.
    /// </summary>
    public CrdtShapeRegistry()
    {
        _global[LatticeMergeMode.OrSet] = CrdtShape.ForOrSet();
        _global[LatticeMergeMode.PnCounter] = CrdtShape.ForPnCounter();
        _global[LatticeMergeMode.VersionVector] = CrdtShape.ForVersionVector();
        _global[LatticeMergeMode.MvRegister] = CrdtShape.ForMvRegister();
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
