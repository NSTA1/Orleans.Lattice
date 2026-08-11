namespace Orleans.Lattice.Storage.File;

/// <summary>
/// A single prepared WAL entry ready to be framed onto disk: its
/// caller-assigned offset paired with the encoded payload bytes the
/// provider will store verbatim. The payload is borrowed for the
/// duration of the append call and is never retained past it (only its
/// on-disk position and length are indexed).
/// </summary>
internal readonly record struct PreparedWalRecord(long Offset, ReadOnlyMemory<byte> Payload);
