namespace Orleans.Lattice.Vector.Persistence;

/// <summary>
/// The on-disk constants of the durable index: the record markers, the format
/// version, and the fixed sizes a reader needs before it can trust a byte.
/// <para>
/// Every durable record - manifest, chunk, partition state, build state, key
/// mapping - is wrapped in the same <see cref="VectorIndexRecord"/> envelope, so
/// there is exactly one place that decides whether a persisted byte sequence is
/// admissible. Because the index is a derived projection, a record that fails
/// any of those checks is discarded and recomputed rather than repaired.
/// </para>
/// </summary>
public static class VectorIndexPersistenceFormat
{
    /// <summary>The marker every durable record opens with: the ASCII bytes <c>LVID</c>.</summary>
    public const uint RecordMagic = 0x4449564CU;

    /// <summary>
    /// The version of the durable layout this build writes and reads. A record
    /// carrying any other version is treated as "rebuild from source" rather than
    /// as a fault, exactly as an unreadable snapshot header is.
    /// </summary>
    public const int Version = 1;

    /// <summary>The number of bytes <see cref="VectorIndexRecord"/> prepends to a payload.</summary>
    public const int RecordHeaderSize = 24;

    /// <summary>The number of bytes a <see cref="VectorIndexManifest"/> payload occupies.</summary>
    public const int ManifestPayloadSize = 32 + VectorIndexFormat.HeaderSize;

    /// <summary>The number of bytes a <see cref="VectorIndexPartitionState"/> payload occupies.</summary>
    public const int PartitionStatePayloadSize = 24;

    /// <summary>
    /// Whether a persisted record's declared version is one this build reads.
    /// </summary>
    /// <param name="version">The version read from a record.</param>
    public static bool IsSupported(int version) => version == Version;
}
