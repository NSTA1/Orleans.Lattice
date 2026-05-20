namespace Orleans.Lattice;

/// <summary>
/// Bytes-shaped read result returned by
/// <see cref="IWalStorageProvider.ReadEncodedAsync"/>. Mirrors the
/// shape of <see cref="IWalStorageProvider.AppendEncodedBatchAsync"/>'s
/// input: a parallel pair of pre-encoded entry segments and their
/// offsets, plus the highest offset the page witnesses. The shipper
/// drain consumes these segments verbatim without ever materialising
/// the strongly-typed <see cref="WalRecord"/> values on its hot path
/// (one fewer encode per shipped entry).
/// <para>
/// The page is a transient value returned synchronously to the
/// caller; it is not an Orleans-serialized wire type. Providers own
/// the underlying byte arrays for the duration of the returned task
/// and may pool them, so callers must not retain references to the
/// segments past the call that consumed the page.
/// </para>
/// </summary>
public readonly record struct WalShardEncodedPage
{
    /// <summary>
    /// Per-entry pre-encoded payload bytes, in ascending offset order
    /// parallel to <see cref="Offsets"/>. Each segment is the exact
    /// byte sequence the registered
    /// <see cref="IWalRecordEncoder"/> emitted on the producer side,
    /// so a caller can either (a) hand the segments straight through
    /// to an outbound framing encoder (the gRPC marshaller's
    /// one-encode shipper path) or (b) decode them element-wise via
    /// <see cref="IWalRecordEncoder.Decode"/>.
    /// </summary>
    public ReadOnlyMemory<ArraySegment<byte>> EncodedEntries { get; init; }

    /// <summary>
    /// Dense ascending offsets parallel to
    /// <see cref="EncodedEntries"/>. The length of this slice equals
    /// the length of <see cref="EncodedEntries"/>.
    /// </summary>
    public ReadOnlyMemory<long> Offsets { get; init; }

    /// <summary>
    /// The highest <see cref="WalEntry.Offset"/> witnessed by the
    /// scan that produced this page. Equals
    /// <c>Offsets.Span[Offsets.Length - 1]</c> when the page is
    /// non-empty; <c>-1</c> when the page is empty (the scan reached
    /// the tail of the log without yielding any entries beyond the
    /// caller-supplied lower bound).
    /// </summary>
    public long HighestOffsetInclusive { get; init; }
}
