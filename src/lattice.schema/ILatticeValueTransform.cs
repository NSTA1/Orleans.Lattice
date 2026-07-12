namespace Orleans.Lattice.Schema;

/// <summary>
/// A host-supplied, imperative value transform identified by a stable id. This is
/// the DI escape hatch for logic the declarative <see cref="LatticeValueTransform"/>
/// IR cannot express - and for opaque or plain-text values the JSON IR cannot
/// navigate. A consumer persists only the stable <see cref="Id"/> and resolves the
/// implementation through <see cref="ILatticeValueTransformRegistry"/> at
/// evaluation time.
/// <para>
/// An implementation must be <b>deterministic</b> and <b>total</b> for the values
/// it is applied to: the same input bytes must always yield the same output bytes,
/// and it must throw a clear exception (rather than corrupt or truncate) on input
/// it cannot handle, so the driving consumer can abort a shadow build. The
/// schema-versioning registry delivered by a later release specialises this seam
/// to an upcaster keyed on <c>(fromVersion, toVersion)</c>.
/// </para>
/// </summary>
public interface ILatticeValueTransform
{
    /// <summary>
    /// The stable identifier under which this transform is registered and
    /// resolved. Must be unique across all registered transforms and stable
    /// across process restarts, because a consumer persists it.
    /// </summary>
    string Id { get; }

    /// <summary>
    /// Transforms <paramref name="value"/> into a new value.
    /// </summary>
    /// <param name="value">The input value bytes.</param>
    /// <returns>The transformed value bytes.</returns>
    byte[] Transform(byte[] value);
}
