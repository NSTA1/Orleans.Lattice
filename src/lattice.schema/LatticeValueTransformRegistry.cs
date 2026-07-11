namespace Orleans.Lattice.Schema;

/// <summary>
/// Default <see cref="ILatticeValueTransformRegistry"/>: indexes every
/// registered <see cref="ILatticeValueTransform"/> by its stable id. Constructed
/// from the DI-registered collection, it fails fast on a duplicate id so a
/// misconfigured host is caught at first resolution rather than silently
/// shadowing one transform with another.
/// </summary>
internal sealed class LatticeValueTransformRegistry : ILatticeValueTransformRegistry
{
    private readonly Dictionary<string, ILatticeValueTransform> _byId;

    /// <summary>
    /// Creates the registry over <paramref name="transforms"/>.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="transforms"/> is <c>null</c>.</exception>
    /// <exception cref="InvalidOperationException">Two transforms share the same id.</exception>
    public LatticeValueTransformRegistry(IEnumerable<ILatticeValueTransform> transforms)
    {
        ArgumentNullException.ThrowIfNull(transforms);
        _byId = new Dictionary<string, ILatticeValueTransform>(StringComparer.Ordinal);
        foreach (var transform in transforms)
        {
            if (!_byId.TryAdd(transform.Id, transform))
            {
                throw new InvalidOperationException(
                    $"Two ILatticeValueTransform instances are registered under the id '{transform.Id}'. " +
                    "Transform ids must be unique across the host.");
            }
        }
    }

    /// <inheritdoc />
    public bool TryGet(string id, out ILatticeValueTransform? transform)
    {
        ArgumentNullException.ThrowIfNull(id);
        return _byId.TryGetValue(id, out transform);
    }

    /// <inheritdoc />
    public ILatticeValueTransform Get(string id)
    {
        ArgumentNullException.ThrowIfNull(id);
        if (_byId.TryGetValue(id, out var transform))
            return transform;
        throw new KeyNotFoundException($"No ILatticeValueTransform is registered under the id '{id}'.");
    }
}
