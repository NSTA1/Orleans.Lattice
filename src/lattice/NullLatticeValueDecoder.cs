namespace Orleans.Lattice;

/// <summary>
/// The core no-op <see cref="ILatticeValueDecoder"/>: never active, so no
/// value is ever decoded. Registered by <c>AddLattice</c> as the safe default
/// so a consumer of the seam always resolves an instance even when no schema
/// add-on is registered, preserving "zero cost when unregistered".
/// </summary>
/// <remarks>
/// <see cref="IsActive"/> returns <c>false</c> for every tree, so the read-path
/// wiring caches an inactive flag per activation and never calls
/// <see cref="DecodeAsync"/> - the read path stays byte-for-byte identical to
/// the pre-seam behaviour with no per-read allocation. A schema/versioning
/// add-on replaces this with a real, envelope-stripping decoder.
/// </remarks>
internal sealed class NullLatticeValueDecoder : ILatticeValueDecoder
{
    /// <inheritdoc />
    public bool IsActive(string treeId) => false;

    /// <inheritdoc />
    public ValueTask<byte[]> DecodeAsync(string treeId, byte[] storedValue, CancellationToken ct) =>
        new(storedValue);
}
