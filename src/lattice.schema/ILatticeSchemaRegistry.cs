namespace Orleans.Lattice.Schema;

/// <summary>
/// The host-supplied catalog of schema versions and the upcasters that evolve a
/// value from one version to the next, keyed by <c>(schemaId, version)</c>. It is
/// the versioning analogue of the compressor registry: the core stays
/// format-agnostic and dispatches on the stamped envelope tag, while the host
/// declares which versions exist and how to move between them.
/// <para>
/// A read of a value stamped at a <b>newer</b> version than the reader's registry
/// recognizes surfaces <see cref="NotSupportedException"/>, mirroring the
/// unknown-compressor case: new schema versions ship without a coordinated wire
/// bump, and a lagging reader fails loudly rather than returning a mis-decoded
/// value.
/// </para>
/// </summary>
public interface ILatticeSchemaRegistry
{
    /// <summary>
    /// Attempts to resolve the descriptor registered for <paramref name="schemaId"/>
    /// at <paramref name="version"/>.
    /// </summary>
    /// <param name="schemaId">The schema-family id.</param>
    /// <param name="version">The schema version.</param>
    /// <param name="descriptor">The resolved descriptor when found.</param>
    /// <returns><c>true</c> when a descriptor is registered; otherwise <c>false</c>.</returns>
    bool TryGetDescriptor(uint schemaId, uint version, out LatticeSchemaDescriptor descriptor);

    /// <summary>
    /// Returns <c>true</c> when a contiguous upcaster chain from
    /// <paramref name="fromVersion"/> to <paramref name="toVersion"/> exists for
    /// <paramref name="schemaId"/> (trivially <c>true</c> when the versions are
    /// equal).
    /// </summary>
    /// <param name="schemaId">The schema-family id.</param>
    /// <param name="fromVersion">The stored version.</param>
    /// <param name="toVersion">The target version.</param>
    /// <returns><c>true</c> when the value can be upcast; otherwise <c>false</c>.</returns>
    bool CanUpcast(uint schemaId, uint fromVersion, uint toVersion);

    /// <summary>
    /// Upcasts <paramref name="body"/> from <paramref name="fromVersion"/> to
    /// <paramref name="toVersion"/> by applying the registered upcaster chain in
    /// order. Returns <paramref name="body"/> unchanged when the versions are equal.
    /// </summary>
    /// <param name="schemaId">The schema-family id.</param>
    /// <param name="fromVersion">The stored version.</param>
    /// <param name="toVersion">The target version.</param>
    /// <param name="body">The plain value body to upcast.</param>
    /// <returns>The upcast value body.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="body"/> is <c>null</c>.</exception>
    /// <exception cref="NotSupportedException">
    /// <paramref name="toVersion"/> is less than <paramref name="fromVersion"/>
    /// (versions are monotonic), or no contiguous upcaster chain exists for the
    /// requested hop.
    /// </exception>
    byte[] Upcast(uint schemaId, uint fromVersion, uint toVersion, byte[] body);
}
