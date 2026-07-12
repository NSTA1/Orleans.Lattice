namespace Orleans.Lattice.Schema;

/// <summary>
/// A single registered upcaster hop that rewrites a value from
/// <see cref="FromVersion"/> to <see cref="ToVersion"/> within a schema family.
/// The rewrite is backed by <b>one</b> of two mechanisms, mirroring the shared
/// value-transform primitive: a declarative, serializable
/// <see cref="LatticeValueTransform"/> IR (the common case, evaluated against the
/// value's JSON document), or a host-supplied <see cref="ILatticeValueTransform"/>
/// resolved by its stable id (the DI escape hatch for logic the IR cannot express
/// or for opaque / plain-text values).
/// <para>
/// An upcaster must be a <b>total, deterministic</b> function over the values it is
/// applied to: the same input always yields the same output, and it throws a clear
/// exception rather than corrupting a value it cannot handle, so a reader or a
/// background migration can surface (or abort on) the offending value.
/// </para>
/// </summary>
public sealed class LatticeSchemaUpcaster
{
    private readonly LatticeValueTransform? _transform;
    private readonly string? _transformId;

    private LatticeSchemaUpcaster(
        uint schemaId, uint fromVersion, uint toVersion, LatticeValueTransform? transform, string? transformId)
    {
        SchemaId = schemaId;
        FromVersion = fromVersion;
        ToVersion = toVersion;
        _transform = transform;
        _transformId = transformId;
    }

    /// <summary>The schema-family id this upcaster belongs to.</summary>
    public uint SchemaId { get; }

    /// <summary>The version this upcaster reads from.</summary>
    public uint FromVersion { get; }

    /// <summary>The version this upcaster produces.</summary>
    public uint ToVersion { get; }

    /// <summary>
    /// The stable id of the host-supplied <see cref="ILatticeValueTransform"/>
    /// backing this upcaster, or <c>null</c> when it is backed by a
    /// <see cref="LatticeValueTransform"/> IR.
    /// </summary>
    public string? TransformId => _transformId;

    /// <summary>
    /// Creates an upcaster backed by a declarative <see cref="LatticeValueTransform"/>
    /// IR.
    /// </summary>
    /// <param name="schemaId">The schema-family id.</param>
    /// <param name="fromVersion">The version this hop reads from.</param>
    /// <param name="toVersion">The version this hop produces. Must be greater than <paramref name="fromVersion"/>.</param>
    /// <param name="transform">The transform IR applied to each value's JSON document.</param>
    /// <returns>The upcaster.</returns>
    /// <exception cref="ArgumentException"><paramref name="toVersion"/> is not greater than <paramref name="fromVersion"/>.</exception>
    public static LatticeSchemaUpcaster FromTransform(
        uint schemaId, uint fromVersion, uint toVersion, LatticeValueTransform transform)
    {
        ThrowIfNotAscending(fromVersion, toVersion);
        return new LatticeSchemaUpcaster(schemaId, fromVersion, toVersion, transform, transformId: null);
    }

    /// <summary>
    /// Creates an upcaster backed by a host-supplied
    /// <see cref="ILatticeValueTransform"/> resolved by its stable id.
    /// </summary>
    /// <param name="schemaId">The schema-family id.</param>
    /// <param name="fromVersion">The version this hop reads from.</param>
    /// <param name="toVersion">The version this hop produces. Must be greater than <paramref name="fromVersion"/>.</param>
    /// <param name="transformId">The stable id of the registered transform. Must not be <c>null</c> or empty.</param>
    /// <returns>The upcaster.</returns>
    /// <exception cref="ArgumentException"><paramref name="transformId"/> is <c>null</c> or empty, or <paramref name="toVersion"/> is not greater than <paramref name="fromVersion"/>.</exception>
    public static LatticeSchemaUpcaster FromTransformId(
        uint schemaId, uint fromVersion, uint toVersion, string transformId)
    {
        ArgumentException.ThrowIfNullOrEmpty(transformId);
        ThrowIfNotAscending(fromVersion, toVersion);
        return new LatticeSchemaUpcaster(schemaId, fromVersion, toVersion, transform: null, transformId);
    }

    /// <summary>
    /// Applies this upcaster to <paramref name="body"/>, returning the rewritten
    /// value. A DI-backed upcaster is resolved through
    /// <paramref name="transformRegistry"/>.
    /// </summary>
    /// <param name="body">The plain value body to rewrite.</param>
    /// <param name="transformRegistry">The registry resolving a DI transform id, or <c>null</c> when only IR upcasters are used.</param>
    /// <returns>The rewritten value.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="body"/> is <c>null</c>.</exception>
    /// <exception cref="InvalidOperationException">
    /// The DI transform id cannot be resolved (no registry, or no transform registered under the id).
    /// </exception>
    public byte[] Apply(byte[] body, ILatticeValueTransformRegistry? transformRegistry)
    {
        ArgumentNullException.ThrowIfNull(body);

        if (_transformId is not null)
        {
            if (transformRegistry is null || !transformRegistry.TryGet(_transformId, out var transform) || transform is null)
            {
                throw new InvalidOperationException(
                    $"Upcaster for schema {SchemaId} v{FromVersion}->v{ToVersion} references transform id " +
                    $"'{_transformId}', which is not registered. Register it with AddLatticeValueTransform(...).");
            }

            return transform.Transform(body);
        }

        return LatticeValueTransformEvaluation.Evaluate(body, _transform!.Value);
    }

    private static void ThrowIfNotAscending(uint fromVersion, uint toVersion)
    {
        if (toVersion <= fromVersion)
        {
            throw new ArgumentException(
                $"An upcaster must advance the version: toVersion ({toVersion}) must be greater than fromVersion ({fromVersion}).",
                nameof(toVersion));
        }
    }
}
