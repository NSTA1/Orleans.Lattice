namespace Orleans.Lattice;

/// <summary>
/// The core no-op <see cref="ILatticeEnvelopeCodec"/>: never active, reports every
/// value as unversioned, and strips nothing. Registered by <c>AddLattice</c> as the
/// safe default so a consumer of the seam always resolves an instance even when no
/// schema / versioning add-on is registered, preserving "zero cost when
/// unregistered".
/// </summary>
/// <remarks>
/// <see cref="IsActive"/> returns <c>false</c> for every tree, so the merge / apply
/// wiring caches an inactive flag per activation and never calls
/// <see cref="ReadVersion"/> or <see cref="StripForFold"/> - the fold path stays
/// byte-for-byte identical to the pre-seam behaviour with no per-fold allocation. A
/// schema / versioning add-on replaces this with a real, envelope-aware codec.
/// </remarks>
internal sealed class NullLatticeEnvelopeCodec : ILatticeEnvelopeCodec
{
    /// <inheritdoc />
    public bool IsActive(string treeId) => false;

    /// <inheritdoc />
    public uint ReadVersion(byte[]? value) => 0;

    /// <inheritdoc />
    public byte[] StripForFold(byte[] delta) => delta;
}
