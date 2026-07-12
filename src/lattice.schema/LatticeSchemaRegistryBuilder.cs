namespace Orleans.Lattice.Schema;

/// <summary>
/// Builds an immutable <see cref="ILatticeSchemaRegistry"/> from a set of schema
/// descriptors and upcasters. A host configures the registry through
/// <c>AddLatticeSchemaVersioning(...)</c>: it declares each version's descriptor
/// and the upcaster hop that evolves each version to the next.
/// </summary>
/// <remarks>
/// Registration is validated eagerly: a duplicate descriptor for the same
/// <c>(schemaId, version)</c> or a duplicate upcaster for the same
/// <c>(schemaId, fromVersion)</c> throws, so a misconfiguration fails fast at
/// startup rather than producing a mis-resolved read.
/// </remarks>
public sealed class LatticeSchemaRegistryBuilder
{
    private readonly Dictionary<(uint, uint), LatticeSchemaDescriptor> _descriptors = new();
    private readonly Dictionary<(uint, uint), LatticeSchemaUpcaster> _upcasters = new();

    /// <summary>
    /// Registers a schema descriptor. A stamped value whose <c>(schemaId, version)</c>
    /// has no descriptor is still readable (the reader dispatches on the stamped
    /// version), but registering descriptors lets a reader detect a value newer than
    /// it recognizes and lets diagnostics name the version.
    /// </summary>
    /// <param name="schemaId">The schema-family id.</param>
    /// <param name="version">The schema version.</param>
    /// <param name="name">A human-readable name for diagnostics. Must not be <c>null</c> or empty.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentException"><paramref name="name"/> is <c>null</c> or empty, or a descriptor for the same <c>(schemaId, version)</c> is already registered.</exception>
    public LatticeSchemaRegistryBuilder AddSchema(uint schemaId, uint version, string name)
    {
        var descriptor = new LatticeSchemaDescriptor(schemaId, version, name);
        if (!_descriptors.TryAdd((schemaId, version), descriptor))
        {
            throw new ArgumentException(
                $"A descriptor for schema {schemaId} v{version} is already registered.", nameof(version));
        }

        return this;
    }

    /// <summary>
    /// Registers an upcaster hop backed by a declarative
    /// <see cref="LatticeValueTransform"/> IR.
    /// </summary>
    /// <param name="schemaId">The schema-family id.</param>
    /// <param name="fromVersion">The version this hop reads from.</param>
    /// <param name="toVersion">The version this hop produces. Must be greater than <paramref name="fromVersion"/>.</param>
    /// <param name="transform">The transform IR applied to each value's JSON document.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentException">An upcaster for the same <c>(schemaId, fromVersion)</c> is already registered, or <paramref name="toVersion"/> is not greater than <paramref name="fromVersion"/>.</exception>
    public LatticeSchemaRegistryBuilder AddUpcaster(
        uint schemaId, uint fromVersion, uint toVersion, LatticeValueTransform transform) =>
        AddUpcaster(LatticeSchemaUpcaster.FromTransform(schemaId, fromVersion, toVersion, transform));

    /// <summary>
    /// Registers an upcaster hop backed by a host-supplied
    /// <see cref="ILatticeValueTransform"/> resolved by its stable id.
    /// </summary>
    /// <param name="schemaId">The schema-family id.</param>
    /// <param name="fromVersion">The version this hop reads from.</param>
    /// <param name="toVersion">The version this hop produces. Must be greater than <paramref name="fromVersion"/>.</param>
    /// <param name="transformId">The stable id of the registered transform. Must not be <c>null</c> or empty.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentException"><paramref name="transformId"/> is <c>null</c> or empty, an upcaster for the same <c>(schemaId, fromVersion)</c> is already registered, or <paramref name="toVersion"/> is not greater than <paramref name="fromVersion"/>.</exception>
    public LatticeSchemaRegistryBuilder AddUpcaster(
        uint schemaId, uint fromVersion, uint toVersion, string transformId) =>
        AddUpcaster(LatticeSchemaUpcaster.FromTransformId(schemaId, fromVersion, toVersion, transformId));

    /// <summary>Registers a pre-built upcaster hop.</summary>
    /// <param name="upcaster">The upcaster to register. Must not be <c>null</c>.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="upcaster"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException">An upcaster for the same <c>(schemaId, fromVersion)</c> is already registered.</exception>
    public LatticeSchemaRegistryBuilder AddUpcaster(LatticeSchemaUpcaster upcaster)
    {
        ArgumentNullException.ThrowIfNull(upcaster);
        if (!_upcasters.TryAdd((upcaster.SchemaId, upcaster.FromVersion), upcaster))
        {
            throw new ArgumentException(
                $"An upcaster for schema {upcaster.SchemaId} from v{upcaster.FromVersion} is already registered.",
                nameof(upcaster));
        }

        return this;
    }

    /// <summary>
    /// Builds the immutable registry.
    /// </summary>
    /// <param name="transformRegistry">
    /// The registry resolving DI transform ids used by any
    /// <see cref="LatticeSchemaUpcaster.FromTransformId"/> upcaster, or <c>null</c>
    /// when only IR upcasters are registered.
    /// </param>
    /// <returns>The built registry.</returns>
    public ILatticeSchemaRegistry Build(ILatticeValueTransformRegistry? transformRegistry = null) =>
        new LatticeSchemaRegistry(
            new Dictionary<(uint, uint), LatticeSchemaDescriptor>(_descriptors),
            new Dictionary<(uint, uint), LatticeSchemaUpcaster>(_upcasters),
            transformRegistry);
}
