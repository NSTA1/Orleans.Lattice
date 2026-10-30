namespace Orleans.Lattice.Schema;

/// <summary>
/// A stored value decoded for a transform remediation that must write its result
/// back self-describing (issue #5243): the body the transform runs on - exactly the
/// body a client read returns - and the schema-version stamp that body is at.
/// </summary>
/// <param name="Body">
/// The decoded body. For a value already stamped at the tree's target it is a view
/// over the stored array past the envelope header, so no copy is made.
/// </param>
/// <param name="Stamped">
/// <c>true</c> when the rewritten body must be re-enveloped at
/// <paramref name="SchemaId"/> / <paramref name="Version"/>; <c>false</c> when the
/// tree is unversioned and the rewritten body is written plain.
/// </param>
/// <param name="SchemaId">The schema-family id to stamp, when <paramref name="Stamped"/>.</param>
/// <param name="Version">The schema version <paramref name="Body"/> conforms to, when <paramref name="Stamped"/>.</param>
internal readonly record struct SchemaRestampInput(ReadOnlyMemory<byte> Body, bool Stamped, uint SchemaId, uint Version)
{
    /// <summary>
    /// Returns <paramref name="rewritten"/> ready to store: enveloped at the stamp
    /// when <see cref="Stamped"/>, otherwise the plain bytes unchanged.
    /// </summary>
    /// <param name="rewritten">The plain rewritten body.</param>
    /// <returns>The bytes to write.</returns>
    public byte[] Restamp(byte[] rewritten) =>
        Stamped ? LatticeSchemaEnvelope.Encode(SchemaId, Version, rewritten) : rewritten;
}
