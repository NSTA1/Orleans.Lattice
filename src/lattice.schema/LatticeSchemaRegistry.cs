namespace Orleans.Lattice.Schema;

/// <summary>
/// The default <see cref="ILatticeSchemaRegistry"/>: an immutable catalog built by
/// <see cref="LatticeSchemaRegistryBuilder"/>. Descriptors are keyed by
/// <c>(schemaId, version)</c>; upcasters are keyed by <c>(schemaId, fromVersion)</c>
/// and each advances the version by an arbitrary positive step, so
/// <see cref="Upcast"/> walks the chain from the stored version up to the target,
/// following the unique next hop at each step.
/// </summary>
internal sealed class LatticeSchemaRegistry : ILatticeSchemaRegistry
{
    private readonly IReadOnlyDictionary<(uint SchemaId, uint Version), LatticeSchemaDescriptor> _descriptors;
    private readonly IReadOnlyDictionary<(uint SchemaId, uint FromVersion), LatticeSchemaUpcaster> _upcasters;
    private readonly ILatticeValueTransformRegistry? _transformRegistry;

    internal LatticeSchemaRegistry(
        IReadOnlyDictionary<(uint, uint), LatticeSchemaDescriptor> descriptors,
        IReadOnlyDictionary<(uint, uint), LatticeSchemaUpcaster> upcasters,
        ILatticeValueTransformRegistry? transformRegistry)
    {
        _descriptors = descriptors;
        _upcasters = upcasters;
        _transformRegistry = transformRegistry;
    }

    /// <inheritdoc />
    public bool TryGetDescriptor(uint schemaId, uint version, out LatticeSchemaDescriptor descriptor) =>
        _descriptors.TryGetValue((schemaId, version), out descriptor);

    /// <inheritdoc />
    public bool CanUpcast(uint schemaId, uint fromVersion, uint toVersion)
    {
        if (toVersion == fromVersion)
        {
            return true;
        }

        if (toVersion < fromVersion)
        {
            return false;
        }

        var current = fromVersion;
        while (current < toVersion)
        {
            if (!_upcasters.TryGetValue((schemaId, current), out var hop) || hop.ToVersion > toVersion)
            {
                return false;
            }

            current = hop.ToVersion;
        }

        return current == toVersion;
    }

    /// <inheritdoc />
    public byte[] Upcast(uint schemaId, uint fromVersion, uint toVersion, byte[] body)
    {
        ArgumentNullException.ThrowIfNull(body);

        if (toVersion == fromVersion)
        {
            return body;
        }

        if (toVersion < fromVersion)
        {
            throw new NotSupportedException(
                $"Cannot downcast schema {schemaId} from v{fromVersion} to v{toVersion}: schema versions are monotonic.");
        }

        var current = fromVersion;
        var result = body;
        while (current < toVersion)
        {
            if (!_upcasters.TryGetValue((schemaId, current), out var hop) || hop.ToVersion > toVersion)
            {
                throw new NotSupportedException(
                    $"No upcaster registered for schema {schemaId} from v{current} toward v{toVersion}. " +
                    "Register the missing hop with the schema-versioning registry.");
            }

            result = hop.Apply(result, _transformRegistry);
            current = hop.ToVersion;
        }

        return result;
    }
}
