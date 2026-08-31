namespace Orleans.Lattice.Vector;

/// <summary>
/// The on-the-wire constants of the chunked <see cref="VectorIndex"/> snapshot
/// format. A durable consumer stamps <see cref="Version"/> into every record it
/// writes and refuses a persisted form it does not understand rather than
/// misreading one.
/// </summary>
public static class VectorIndexFormat
{
    /// <summary>The four-byte marker that opens a snapshot header: <c>LVIX</c>.</summary>
    public const uint HeaderMagic = 0x5849564CU;

    /// <summary>The four-byte marker that opens a snapshot chunk: <c>LVIC</c>.</summary>
    public const uint ChunkMagic = 0x4349564CU;

    /// <summary>
    /// The current snapshot format version. Increment it for any change to the
    /// header or chunk layout; a reader rejects a version it does not support
    /// rather than decoding it as the current one.
    /// </summary>
    public const int Version = 1;

    /// <summary>The exact byte length of a snapshot header.</summary>
    public const int HeaderSize = 56;

    /// <summary>The exact byte length of the fixed preamble on every chunk.</summary>
    public const int ChunkHeaderSize = 24;

    /// <summary>
    /// Whether this build can read a snapshot stamped with the given version.
    /// </summary>
    /// <param name="version">The format version read from a persisted header or chunk.</param>
    public static bool IsSupported(int version) => version == Version;
}
