namespace Orleans.Lattice.Schema;

/// <summary>
/// Describes one registered schema version: which schema family it belongs to, its
/// version number, and a human-readable name for diagnostics. Held in the
/// host-supplied <see cref="ILatticeSchemaRegistry"/> so a reader can confirm a
/// stamped <c>(schemaId, version)</c> is recognized before deciphering the body.
/// This is an in-process descriptor; it is never persisted or sent on the wire.
/// </summary>
public readonly record struct LatticeSchemaDescriptor
{
    /// <summary>Initializes a new <see cref="LatticeSchemaDescriptor"/>.</summary>
    /// <param name="schemaId">The schema-family id.</param>
    /// <param name="version">The schema version.</param>
    /// <param name="name">A human-readable name for diagnostics. Must not be <c>null</c> or empty.</param>
    /// <exception cref="ArgumentException"><paramref name="name"/> is <c>null</c> or empty.</exception>
    public LatticeSchemaDescriptor(uint schemaId, uint version, string name)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        SchemaId = schemaId;
        Version = version;
        Name = name;
    }

    /// <summary>The schema-family id.</summary>
    public uint SchemaId { get; }

    /// <summary>The schema version.</summary>
    public uint Version { get; }

    /// <summary>A human-readable name for diagnostics.</summary>
    public string Name { get; }
}
