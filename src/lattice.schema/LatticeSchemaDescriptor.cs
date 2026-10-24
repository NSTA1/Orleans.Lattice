namespace Orleans.Lattice.Schema;

/// <summary>
/// Describes one registered schema version: which schema family it belongs to, its
/// version number, and a human-readable name for diagnostics. Held in the
/// host-supplied <see cref="ILatticeSchemaRegistry"/> so a reader can confirm a
/// stamped <c>(schemaId, version)</c> is recognized before deciphering the body.
/// Runtime registrations persist this descriptor and expose it through control APIs.
/// </summary>
[GenerateSerializer]
[Alias(SchemaTypeAliases.LatticeSchemaDescriptor)]
[Immutable]
public readonly record struct LatticeSchemaDescriptor
{
    /// <summary>Initializes a new <see cref="LatticeSchemaDescriptor"/>.</summary>
    /// <param name="schemaId">The schema-family id.</param>
    /// <param name="version">The schema version.</param>
    /// <param name="name">A human-readable name for diagnostics. Must not be <c>null</c> or empty.</param>
    /// <exception cref="ArgumentException"><paramref name="name"/> is <c>null</c> or empty.</exception>
    [System.Text.Json.Serialization.JsonConstructor]
    public LatticeSchemaDescriptor(uint schemaId, uint version, string name)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        SchemaId = schemaId;
        Version = version;
        Name = name;
    }

    /// <summary>The schema-family id.</summary>
    [Id(0)] public uint SchemaId { get; }

    /// <summary>The schema version.</summary>
    [Id(1)] public uint Version { get; }

    /// <summary>A human-readable name for diagnostics.</summary>
    [Id(2)] public string Name { get; }
}
