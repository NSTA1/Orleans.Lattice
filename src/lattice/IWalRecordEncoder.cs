using System.Buffers;

namespace Orleans.Lattice;

/// <summary>
/// Single-pass codec for the WAL's per-entry payload bytes. Captures
/// both the encode side - producing the bytes a
/// <see cref="WalRecord"/> serialises to under the WAL's chosen wire
/// format, emitted into a caller-supplied
/// <see cref="IBufferWriter{T}"/> so the result can be measured
/// against <see cref="LatticeOptions.WalMaxBatchBytes"/> and handed
/// verbatim to
/// <see cref="IWalStorageProvider.AppendEncodedBatchAsync"/> without a
/// second encode - and the decode side, used by the in-memory
/// provider on read and by the default
/// <see cref="IWalStorageProvider.AppendEncodedBatchAsync"/> fallback
/// to round-trip pre-encoded payloads back to
/// <see cref="WalRecord"/> values for third-party providers that did
/// not override the zero-copy overload.
/// <para>
/// Replaces the historical per-entry size heuristic
/// (<c>key.Length * 2 + value.Length + 128</c>). Producing the bytes
/// once at append time and handing the same bytes to the storage
/// provider closes the "WAL append re-encodes what the grain already
/// encoded" gap in the commit hot path; backends that natively store
/// binary payloads (Azure Table Storage, file-backed providers)
/// accept the segments straight through to the persistence row, and
/// the WAL grain accumulates the per-batch byte budget directly from
/// the bytes it just wrote rather than from a heuristic that lost
/// fidelity around vector-clock cardinality.
/// </para>
/// <para>
/// The encoder's subject type is <see cref="WalRecord"/>, not
/// <see cref="LatticeMutation"/>: the WAL grain already has a
/// <see cref="WalRecord"/> in hand at append time (the
/// observer-stamped durability shape), so the encoder consumes it
/// directly without the producer-side
/// <see cref="LatticeMutation"/> round-trip. The public
/// observer-payload surface (<see cref="LatticeMutation"/>) is
/// unchanged; the durability shape is an implementation detail of the
/// WAL append path.
/// </para>
/// <para>
/// Implementations must be safe for concurrent invocation from
/// multiple threads. The default
/// <see cref="OrleansBinaryWalRecordEncoder"/> wraps the thread-safe
/// <c>Serializer&lt;WalRecord&gt;</c> from
/// <c>Orleans.Serialization</c>; replacement implementations are
/// expected to be similarly cheap (no per-call heap allocation
/// beyond the bytes claimed via
/// <see cref="IBufferWriter{T}.GetSpan"/> /
/// <see cref="IBufferWriter{T}.GetMemory"/>, plus whatever the
/// decode path requires).
/// </para>
/// </summary>
public interface IWalRecordEncoder
{
    /// <summary>
    /// Encodes <paramref name="record"/> into <paramref name="writer"/>
    /// under the WAL's canonical wire format. Callers compute the
    /// number of bytes written by snapshotting the writer's running
    /// count before and after the call (or by inspecting
    /// <c>WrittenSpan.Length</c> on an
    /// <see cref="ArrayBufferWriter{T}"/>); the encoded bytes can then
    /// participate in the per-batch byte budget
    /// (<see cref="LatticeOptions.WalMaxBatchBytes"/>) and be passed
    /// directly to
    /// <see cref="IWalStorageProvider.AppendEncodedBatchAsync"/>.
    /// </summary>
    /// <param name="record">The WAL record to encode. Captured state is read-only; the encoder must not mutate it.</param>
    /// <param name="writer">Destination buffer writer. The encoder calls <see cref="IBufferWriter{T}.GetSpan"/> / <see cref="IBufferWriter{T}.Advance"/> exactly as <c>Orleans.Serialization</c> does. Must not be <see langword="null"/>.</param>
    void Encode(in WalRecord record, IBufferWriter<byte> writer);

    /// <summary>
    /// Decodes a previously-<see cref="Encode"/>d payload back to a
    /// <see cref="WalRecord"/>. The supplied segment is the exact byte
    /// sequence the encoder wrote on the producer side (the in-memory
    /// provider stores it verbatim; the Azure Table provider reads it
    /// back from the row's <c>Payload</c> column).
    /// </summary>
    /// <param name="encoded">The encoded payload bytes. Borrowed for the duration of the call; the implementation must not retain a reference to the underlying array.</param>
    /// <remarks>
    /// The encoder strips the redundant <see cref="WalRecord.TreeId"/>
    /// slot at encode time because every storage and transport seam
    /// already supplies the tree id from surrounding context (the
    /// storage partition key, the framing header's <c>TreeName</c>
    /// tail, the shipper's owning grain key). The single-argument
    /// <c>Decode</c> therefore returns a record whose
    /// <see cref="WalRecord.TreeId"/> is the empty string; call sites
    /// that have the tree id in hand should prefer the
    /// <see cref="Decode(ReadOnlySpan{byte}, string)"/> overload so the
    /// returned record is field-complete. The single-argument overload
    /// remains useful for forensic tooling that inspects raw WAL
    /// bytes without surrounding context.
    /// </remarks>
    WalRecord Decode(ReadOnlySpan<byte> encoded);

    /// <summary>
    /// Decodes a previously-<see cref="Encode"/>d payload back to a
    /// <see cref="WalRecord"/> and re-stamps
    /// <see cref="WalRecord.TreeId"/> from the supplied
    /// <paramref name="treeId"/> context. Producer-side
    /// <see cref="Encode"/> elides the tree-id slot from the encoded
    /// bytes because every storage and transport seam recovers it from
    /// surrounding context (the storage partition key, the framing
    /// header's <c>TreeName</c> tail, the shipper's owning grain key);
    /// this overload is the seam where the re-stamp happens.
    /// </summary>
    /// <param name="encoded">The encoded payload bytes. Borrowed for the duration of the call; the implementation must not retain a reference to the underlying array.</param>
    /// <param name="treeId">The tree id recovered from surrounding context. Stamped onto <see cref="WalRecord.TreeId"/> on the returned value. Must not be <see langword="null"/>.</param>
    WalRecord Decode(ReadOnlySpan<byte> encoded, string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        var record = Decode(encoded);
        return record with { TreeId = treeId };
    }

    /// <summary>
    /// Decodes a previously-<see cref="Encode"/>d payload back to a
    /// <see cref="WalRecord"/> and re-stamps both
    /// <see cref="WalRecord.TreeId"/> and <see cref="WalRecord.Mode"/>
    /// from the supplied <paramref name="treeId"/> and
    /// <paramref name="mode"/> batch-level context. Producer-side
    /// <see cref="Encode"/> elides both slots from the encoded bytes
    /// because they are constant within a single shipped batch (tree
    /// id from the framing header / grain key, merge mode from the
    /// framing header's <c>Mode</c> field hoisted by the producer-side
    /// resolver). This overload is the seam where both fields are
    /// restored on the receiver-side replication apply path.
    /// </summary>
    /// <param name="encoded">The encoded payload bytes. Borrowed for the duration of the call; the implementation must not retain a reference to the underlying array.</param>
    /// <param name="treeId">The tree id recovered from surrounding context. Stamped onto <see cref="WalRecord.TreeId"/> on the returned value. Must not be <see langword="null"/>.</param>
    /// <param name="mode">The per-tree merge mode recovered from the framing header. Stamped onto <see cref="WalRecord.Mode"/> on the returned value.</param>
    WalRecord Decode(ReadOnlySpan<byte> encoded, string treeId, LatticeMergeMode mode)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        var record = Decode(encoded);
        return record with { TreeId = treeId, Mode = mode };
    }
}
