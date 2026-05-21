namespace Orleans.Lattice;

/// <summary>
/// Compression algorithm tag shared by every Orleans.Lattice layer
/// that routes byte payloads through <see cref="ILatticeCompressor"/>.
/// The replication batch framing layer carries this value in the
/// fixed plaintext header so receivers can read the algorithm tag
/// before allocating an inflate buffer; future layers (WAL segment
/// compression, snapshot compression) will reuse the same enum so
/// host-side DI registrations are not duplicated.
/// <para>
/// The on-wire representation is a single <see cref="byte"/>. The
/// tag-space is partitioned by range:
/// </para>
/// <list type="bullet">
///   <item>
///     <description>
///       <c>0x00</c> - <c>0x7F</c> - core-reserved tags. Only the
///       members of this enum are guaranteed-stable wire values
///       (<see cref="None"/> = <c>0x00</c>, <see cref="Zstd"/> =
///       <c>0x01</c>). Only types declared in the core
///       <c>Orleans.Lattice</c> assembly may claim a tag in this
///       range; host-defined compressors (including alternative
///       implementations of a core algorithm) are rejected at
///       registration time and must use the host-defined range
///       below.
///     </description>
///   </item>
///   <item>
///     <description>
///       <c>0x80</c> - <c>0xFF</c> - host-defined tags. Hosts that
///       ship their own compressor register a singleton
///       <see cref="ILatticeCompressor"/> whose
///       <see cref="ILatticeCompressor.Algorithm"/> casts a byte
///       from this range into <see cref="LatticeCompression"/>. The
///       framing dispatch keys on the raw byte so the host's tag
///       round-trips through encode/decode even though it is not a
///       named enum member. See
///       <c>docs/lattice/compression.md</c> for the full
///       registration walk-through.
///     </description>
///   </item>
/// </list>
/// <para>
/// An encoded batch whose tag has no registered compressor at the
/// receiver surfaces as <see cref="NotSupportedException"/> from
/// the consuming decoder; this is the wire-version-free way new
/// algorithms ship without coordinated upgrades.
/// </para>
/// </summary>
public enum LatticeCompression : byte
{
    /// <summary>
    /// Payload bytes are written verbatim; no compression layer is
    /// applied. The default value when compression is left at its
    /// default option setting and the historical wire-compatible
    /// shape for layers that pre-date this seam.
    /// </summary>
    None = 0,

    /// <summary>
    /// Payload bytes are compressed with the Zstandard algorithm
    /// (RFC 8478). The canonical implementation is
    /// <see cref="ZstdLatticeCompressor"/> in
    /// <c>Orleans.Lattice</c>; hosts that want a different
    /// algorithm cast a byte in <c>[0x80, 0xFF]</c> into this enum
    /// and register a matching <see cref="ILatticeCompressor"/>
    /// via <c>AddLatticeCompressor</c>.
    /// </summary>
    Zstd = 1,
}
