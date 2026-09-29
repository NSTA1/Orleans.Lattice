namespace Orleans.Lattice.Explorer.UI.Framing;

/// <summary>One verified bundle asset: its path, media type, pinned digest and bytes.</summary>
/// <param name="Path">The normalised bundle path.</param>
/// <param name="MediaType">The declared media type.</param>
/// <param name="Digest">The SHA-256 the manifest pins and the bytes were verified against.</param>
/// <param name="Bytes">The verified bytes; never mutated.</param>
internal sealed record AppFrameBundleAsset(string Path, string MediaType, string Digest, ReadOnlyMemory<byte> Bytes);
