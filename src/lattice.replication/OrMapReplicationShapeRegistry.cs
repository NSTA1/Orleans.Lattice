using System.Collections.Concurrent;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Receiver-side registry of typed OR-Map shapes. Hosts that
/// configure a tree for <see cref="LatticeMergeMode.OrMap"/> must
/// register the concrete <c>(TKey, TValue)</c> pair on the silo
/// service collection via
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddOrMapReplicationShape{TKey, TValue}(ISiloBuilder, string)"/>
/// so the <see cref="ReplicationApplier"/> can deserialise the
/// generic <see cref="OrMapDelta{TKey, TValue}"/> carried in
/// <see cref="WalRecord.Delta"/> and route it through the matching
/// <see cref="OrMap{TKey, TValue}.MergeDelta(OrMapDelta{TKey, TValue})"/>
/// call.
/// </summary>
public sealed class OrMapReplicationShapeRegistry
{
    private readonly ConcurrentDictionary<string, OrMapReplicationShape> _shapes =
        new(StringComparer.Ordinal);

    /// <summary>
    /// Registers <paramref name="shape"/> as the apply descriptor for
    /// the tree identified by <paramref name="treeId"/>. Throws when
    /// a different shape is already registered for the same tree.
    /// </summary>
    public void Register(string treeId, OrMapReplicationShape shape)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(shape);
        if (!_shapes.TryAdd(treeId, shape))
        {
            var existing = _shapes[treeId];
            if (!ReferenceEquals(existing, shape))
            {
                throw new InvalidOperationException(
                    $"An OrMapReplicationShape is already registered for tree '{treeId}'. " +
                    "Each tree may carry at most one (TKey, TValue) shape; remove the duplicate registration.");
            }
        }
    }

    /// <summary>
    /// Resolves the apply descriptor for <paramref name="treeId"/>, or
    /// <c>null</c> when none has been registered. Callers fault the
    /// apply path on <c>null</c> so the misconfiguration surfaces
    /// rather than silently dropping the entry.
    /// </summary>
    public OrMapReplicationShape? TryGet(string treeId)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return _shapes.TryGetValue(treeId, out var shape) ? shape : null;
    }
}

/// <summary>
/// Receiver-side apply descriptor for one concrete
/// <c>(TKey, TValue)</c> OR-Map shape. Carries the bytes-to-state
/// and bytes-to-delta deserialisers plus a merge action that folds
/// the typed delta into the loaded state. Constructed by the
/// <c>AddOrMapReplicationShape&lt;TKey, TValue&gt;</c> extension; not
/// intended for direct host construction.
/// </summary>
public sealed class OrMapReplicationShape
{
    /// <summary>Deserialises the full-state bytes into a typed primitive instance.</summary>
    public Func<byte[], object> DeserializeState { get; }

    /// <summary>Deserialises the typed delta DTO bytes into a typed delta instance.</summary>
    public Func<byte[], object> DeserializeDelta { get; }

    /// <summary>Folds a deserialised typed delta into a deserialised typed state.</summary>
    public Action<object, object> MergeDelta { get; }

    /// <summary>Constructs an empty typed state instance for the "key absent" case.</summary>
    public Func<object> CreateEmpty { get; }

    /// <summary>Serialises a typed state instance back to bytes for CAS write-back.</summary>
    public Func<object, byte[]> SerializeState { get; }

    /// <summary>Initialises a new <see cref="OrMapReplicationShape"/>.</summary>
    public OrMapReplicationShape(
        Func<byte[], object> deserializeState,
        Func<byte[], object> deserializeDelta,
        Action<object, object> mergeDelta,
        Func<object> createEmpty,
        Func<object, byte[]> serializeState)
    {
        ArgumentNullException.ThrowIfNull(deserializeState);
        ArgumentNullException.ThrowIfNull(deserializeDelta);
        ArgumentNullException.ThrowIfNull(mergeDelta);
        ArgumentNullException.ThrowIfNull(createEmpty);
        ArgumentNullException.ThrowIfNull(serializeState);
        DeserializeState = deserializeState;
        DeserializeDelta = deserializeDelta;
        MergeDelta = mergeDelta;
        CreateEmpty = createEmpty;
        SerializeState = serializeState;
    }

    /// <summary>
    /// Factory for the typed <c>(TKey, TValue)</c> shape descriptor. Constructs
    /// a shape that deserialises <see cref="OrMap{TKey, TValue}"/> and
    /// <see cref="OrMapDelta{TKey, TValue}"/> via the default JSON serialiser
    /// and folds the delta in via the primitive's instance
    /// <see cref="OrMap{TKey, TValue}.MergeDelta(OrMapDelta{TKey, TValue})"/>
    /// method.
    /// </summary>
    public static OrMapReplicationShape For<TKey, TValue>()
        where TKey : notnull
        where TValue : Primitives.ICrdt<TValue>, new()
    {
        var stateSerializer = JsonLatticeSerializer<OrMap<TKey, TValue>>.Default;
        var deltaSerializer = JsonLatticeSerializer<OrMapDelta<TKey, TValue>>.Default;
        return new OrMapReplicationShape(
            deserializeState: bytes => stateSerializer.Deserialize(bytes),
            deserializeDelta: bytes => deltaSerializer.Deserialize(bytes),
            mergeDelta: (state, delta) => ((OrMap<TKey, TValue>)state).MergeDelta((OrMapDelta<TKey, TValue>)delta),
            createEmpty: () => new OrMap<TKey, TValue>(),
            serializeState: state => stateSerializer.Serialize((OrMap<TKey, TValue>)state));
    }
}