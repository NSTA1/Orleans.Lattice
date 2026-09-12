using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The single read/write plane for the agent-memory tree. Every memory entry is
/// stored as an <see cref="MvRegister"/> whose concurrent values are the
/// Orleans-serialized <see cref="MemoryRecord"/> bytes of each replica's write, so
/// two clusters' concurrent writes to the same key both survive (each mints its own
/// dot) instead of one whole record being lost to last-writer-wins. A read folds the
/// conflict set back into a single record through the record model's own CRDT
/// <see cref="MemoryRecord.Merge(MemoryRecord, MemoryRecord)"/>, which is
/// commutative, associative, and idempotent, so the fold order does not matter.
/// <para>
/// This is authored unconditionally - whether or not the replication companion is
/// enabled - so a single-cluster deployment stores and reads the identical
/// <see cref="MvRegister"/> shape a replicated one does, and enabling replication is
/// a pure configuration change that needs no data migration.
/// </para>
/// </summary>
internal static class RepoContextMemoryCodec
{
    /// <summary>
    /// The identity serializer the memory <see cref="MvRegister"/> accessor uses:
    /// the register's per-replica values are already the Orleans-serialized
    /// <see cref="MemoryRecord"/> bytes, so no second encoding is applied (avoiding
    /// the base64 blow-up a JSON serializer would add over a byte payload).
    /// </summary>
    internal static readonly ILatticeSerializer<byte[]> ByteIdentity = new IdentityByteSerializer();

    /// <summary>
    /// Binds an <see cref="MvRegisterAccessor{T}"/> to the memory entry at
    /// <paramref name="key"/> in <paramref name="tree"/>, authored through
    /// <see cref="ByteIdentity"/> so the stored per-replica values are the raw
    /// <see cref="MemoryRecord"/> bytes.
    /// </summary>
    /// <param name="tree">The memory tree. Must not be <see langword="null"/>.</param>
    /// <param name="key">The full memory key.</param>
    /// <returns>The bound multi-value-register accessor.</returns>
    internal static MvRegisterAccessor<byte[]> Accessor(ILattice tree, string key) =>
        tree.MvRegister(key, ByteIdentity);

    /// <summary>
    /// Folds a stored memory value - the whole-key <see cref="MvRegister"/> blob as
    /// returned by <see cref="ILattice.GetAsync(string, System.Threading.CancellationToken)"/>
    /// or <c>GetWithVersionAsync(...).Value</c> - into a single
    /// <see cref="MemoryRecord"/> by deserializing each concurrent value and
    /// reducing through <see cref="MemoryRecord.Merge(MemoryRecord, MemoryRecord)"/>.
    /// </summary>
    /// <param name="stored">The stored register blob, or <see langword="null"/> when the key is absent or expired.</param>
    /// <param name="serializer">The Orleans serializer used to decode each concurrent record. Must not be <see langword="null"/>.</param>
    /// <param name="key">
    /// The full memory key the blob was read from, named in the failure when a
    /// decode throws. Optional only so that call sites with no key in hand still
    /// compile; supply it wherever the key is known, because it is the one fact the
    /// underlying decoders cannot report.
    /// </param>
    /// <returns>
    /// The merged record, or <see langword="null"/> when <paramref name="stored"/>
    /// is <see langword="null"/> or the register carries no live value.
    /// </returns>
    /// <exception cref="RepoContextRecordDecodeException">
    /// The stored blob, or one of the concurrent records inside it, is malformed.
    /// </exception>
    internal static MemoryRecord? Fold(byte[]? stored, Serializer serializer, string? key = null)
    {
        ArgumentNullException.ThrowIfNull(serializer);

        if (stored is null)
        {
            return null;
        }

        var register = DecodeRegister(stored, key);
        if (register.IsEmpty)
        {
            return null;
        }

        var values = register.Values();
        if (values.Count == 0)
        {
            return null;
        }

        var folded = DecodeRecord(values[0], serializer, stored, key);
        for (var i = 1; i < values.Count; i++)
        {
            folded = MemoryRecord.Merge(folded, DecodeRecord(values[i], serializer, stored, key));
        }

        return folded;
    }

    /// <summary>
    /// Folds a stored memory value the way <see cref="Fold(byte[], Serializer, string)"/>
    /// does, but reports an undecodable value by returning <see langword="false"/>
    /// instead of throwing.
    /// <para>
    /// This exists for the one caller whose correct response to a malformed record
    /// is to proceed rather than to fail: retiring the record. A retirement that
    /// cannot decode what it is retiring is still a valid retirement, whereas a
    /// mutation that cannot decode what it is mutating is not, so this must never
    /// be reached for by a read-modify-write that is not a retirement. Widening its
    /// use would convert "this record could not be decoded" from a loud failure
    /// into a silent one.
    /// </para>
    /// </summary>
    /// <param name="stored">The stored register blob, or <see langword="null"/> when the key is absent or expired.</param>
    /// <param name="serializer">The Orleans serializer used to decode each concurrent record. Must not be <see langword="null"/>.</param>
    /// <param name="key">The full memory key the blob was read from.</param>
    /// <param name="folded">
    /// The merged record on success, which is <see langword="null"/> when the key
    /// is absent or the register carries no live value; <see langword="null"/> when
    /// the value is undecodable.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the value decoded (including the absent and
    /// empty cases); <see langword="false"/> when it is malformed.
    /// </returns>
    internal static bool TryFold(
        byte[]? stored,
        Serializer serializer,
        string key,
        out MemoryRecord? folded)
    {
        ArgumentNullException.ThrowIfNull(serializer);

        try
        {
            folded = Fold(stored, serializer, key);
            return true;
        }
        catch (RepoContextRecordDecodeException)
        {
            folded = null;
            return false;
        }
    }

    /// <summary>
    /// Decodes one concurrent value of the register into a <see cref="MemoryRecord"/>,
    /// attributing a malformed record to the key that holds it.
    /// </summary>
    /// <param name="value">The concurrent value's Orleans-serialized record bytes.</param>
    /// <param name="serializer">The Orleans serializer.</param>
    /// <param name="stored">The whole stored blob, used only to size the failure report.</param>
    /// <param name="key">The full memory key, or <see langword="null"/> when not supplied.</param>
    /// <returns>The decoded record.</returns>
    private static MemoryRecord DecodeRecord(
        byte[] value,
        Serializer serializer,
        byte[] stored,
        string? key)
    {
        try
        {
            return serializer.Deserialize<MemoryRecord>(value);
        }
        catch (Exception ex) when (ex is not RepoContextRecordDecodeException)
        {
            // The window is anchored over the concurrent value rather than the whole
            // blob: that is the payload the decoder actually rejected, and reporting
            // the enclosing blob's bytes would point a reader at framing that decoded
            // perfectly well.
            _ = stored;
            throw new RepoContextRecordDecodeException(
                key ?? UnknownKey,
                "memory record",
                value,
                ex);
        }
    }

    /// <summary>
    /// Decodes the whole-key register blob with the same JSON codec the
    /// <see cref="MvRegisterAccessor{T}"/> writes it with, so a direct read of the
    /// stored bytes (bulk scan, keyword search) unwraps the identical shape the
    /// accessor round-trips.
    /// </summary>
    /// <param name="stored">The stored register blob. Must not be <see langword="null"/>.</param>
    /// <param name="key">
    /// The full memory key the blob was read from, named in the failure when the
    /// decode throws.
    /// </param>
    /// <returns>The decoded register.</returns>
    /// <exception cref="RepoContextRecordDecodeException">The stored blob is malformed.</exception>
    internal static MvRegister DecodeRegister(byte[] stored, string? key = null)
    {
        try
        {
            return JsonLatticeSerializer<MvRegister>.Default.Deserialize(stored);
        }
        catch (Exception ex) when (ex is not RepoContextRecordDecodeException)
        {
            throw new RepoContextRecordDecodeException(
                key ?? UnknownKey,
                "register envelope",
                stored,
                ex);
        }
    }

    /// <summary>
    /// The placeholder named in a decode failure raised from a call site that had
    /// no key in hand. It is deliberately conspicuous: a failure reporting it is
    /// still more actionable than the bare positional error it replaced, and it
    /// marks the call site as one that should thread its key through.
    /// </summary>
    private const string UnknownKey = "(key not supplied by caller)";

    /// <summary>The no-op <see cref="byte"/>[] serializer backing <see cref="ByteIdentity"/>.</summary>
    private sealed class IdentityByteSerializer : ILatticeSerializer<byte[]>
    {
        public byte[] Serialize(byte[] value) => value;

        public byte[] Deserialize(byte[] bytes) => bytes;
    }
}
