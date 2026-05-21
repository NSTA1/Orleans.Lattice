using System.Buffers;
using Orleans.Serialization;

namespace Orleans.Lattice;

/// <summary>
/// Default <see cref="IWalRecordEncoder"/> implementation that writes
/// <see cref="WalRecord"/> bytes through the canonical
/// <see cref="Serializer{T}"/> from <c>Orleans.Serialization</c>. Has
/// no per-call state beyond the bytes the caller supplies through the
/// destination <see cref="IBufferWriter{T}"/>; the underlying
/// serializer is itself thread-safe and stateless.
/// <para>
/// Registered as a singleton from
/// <see cref="LatticeServiceCollectionExtensions.AddLattice"/> so the
/// codec stays warm across every WAL append. Hosts that wish to
/// substitute a different wire format register their own
/// implementation before that call (the default registration uses
/// <c>TryAddSingleton</c>).
/// </para>
/// </summary>
public sealed class OrleansBinaryWalRecordEncoder(Serializer<WalRecord> serializer) : IWalRecordEncoder
{
    private readonly Serializer<WalRecord> _serializer = serializer
        ?? throw new ArgumentNullException(nameof(serializer));

    /// <inheritdoc />
    public void Encode(in WalRecord record, IBufferWriter<byte> writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        // Strip the redundant TreeId slot before serialisation: every
        // storage and transport seam recovers the tree id from
        // surrounding context (storage partition key, framing header
        // TreeName tail, shipper grain key), so persisting it on every
        // entry duplicates ~25-35 bytes per entry for production tree
        // names. Decoders re-stamp via Decode(span, treeId).
        if (record.TreeId.Length == 0)
        {
            _serializer.Serialize(record, writer);
            return;
        }
        var stripped = record with { TreeId = string.Empty };
        _serializer.Serialize(stripped, writer);
    }

    /// <inheritdoc />
    public WalRecord Decode(ReadOnlySpan<byte> encoded)
    {
        // Serializer<T> exposes a span overload of Deserialize that
        // avoids copying the bytes; we delegate directly. The returned
        // record carries TreeId == string.Empty when the producer used
        // Encode (which strips the slot); forensic tooling that calls
        // this single-argument overload accepts that invariant. Call
        // sites with the tree id in hand should call the
        // Decode(span, treeId) overload instead.
        return _serializer.Deserialize(encoded);
    }

    /// <inheritdoc />
    public WalRecord Decode(ReadOnlySpan<byte> encoded, string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        var record = _serializer.Deserialize(encoded);
        // Re-stamp TreeId from the caller-supplied context. The
        // producer's Encode stripped this slot; this overload is the
        // single seam where it is restored.
        return record with { TreeId = treeId };
    }
}
