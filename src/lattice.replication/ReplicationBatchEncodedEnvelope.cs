namespace Orleans.Lattice.Replication;

/// <summary>
/// In-process counterpart to <see cref="ReplicationBatchEnvelope"/>
/// for the framing-only fast path. Carries an <see cref="EncodedBatchHeader"/>
/// alongside a memory of pre-encoded entry byte segments, so the
/// shipper can hand a ready-to-frame batch to the transport without
/// ever materialising the strongly-typed
/// <see cref="BPlusTree.Grains.WalRecord"/> list.
/// <para>
/// Like <see cref="ReplicationBatch"/> itself, this value type is
/// intentionally not Orleans-serialisable: it is the in-process call
/// shape, not the on-the-wire envelope. The on-the-wire bytes are
/// produced by
/// <see cref="IReplicationBatchEncoder.EncodeFraming(in EncodedBatchHeader, System.ReadOnlyMemory{System.ArraySegment{byte}}, System.Buffers.IBufferWriter{byte})"/>
/// and are framed by
/// <see cref="EncodedBatchHeader"/>'s fixed wire layout followed by
/// length-prefixed segments.
/// </para>
/// <para>
/// The segments memory is borrowed from the WAL provider's read page
/// for the lifetime of the surrounding
/// <see cref="IReplicationTransport.SendAsync(ReplicationBatch, System.Threading.CancellationToken)"/>
/// call; the Orleans single-threaded grain turn model makes the
/// reference safe for synchronous consumption inside that call.
/// Transports that need to retain the segments past the returned
/// <see cref="System.Threading.Tasks.Task"/>'s completion must copy
/// them.
/// </para>
/// </summary>
public readonly record struct ReplicationBatchEncodedEnvelope
{
    /// <summary>
    /// Fixed-shape framing header authored by the shipper. Stamped
    /// with the current framing wire version, the entry count, the
    /// monotonic batch sequence, and the FNV-1a hash of the surrounding
    /// <see cref="ReplicationBatch.OriginClusterId"/>.
    /// </summary>
    public EncodedBatchHeader Header { get; init; }

    /// <summary>
    /// Pre-encoded entry segments in commit order. Each segment is the
    /// exact byte output of
    /// <see cref="IWalRecordEncoder.Encode(in BPlusTree.Grains.WalRecord, System.Buffers.IBufferWriter{byte})"/>
    /// for one <see cref="BPlusTree.Grains.WalRecord"/>; the framing
    /// encoder writes each segment verbatim, preceded only by its
    /// 4-byte little-endian length prefix.
    /// </summary>
    public System.ReadOnlyMemory<System.ArraySegment<byte>> EncodedEntries { get; init; }
}