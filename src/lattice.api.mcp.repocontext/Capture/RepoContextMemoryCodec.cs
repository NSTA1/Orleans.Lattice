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
    /// <returns>
    /// The merged record, or <see langword="null"/> when <paramref name="stored"/>
    /// is <see langword="null"/> or the register carries no live value.
    /// </returns>
    internal static MemoryRecord? Fold(byte[]? stored, Serializer serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);

        if (stored is null)
        {
            return null;
        }

        var register = DecodeRegister(stored);
        if (register.IsEmpty)
        {
            return null;
        }

        var values = register.Values();
        if (values.Count == 0)
        {
            return null;
        }

        var folded = serializer.Deserialize<MemoryRecord>(values[0]);
        for (var i = 1; i < values.Count; i++)
        {
            folded = MemoryRecord.Merge(folded, serializer.Deserialize<MemoryRecord>(values[i]));
        }

        return folded;
    }

    /// <summary>
    /// Decodes the whole-key register blob with the same JSON codec the
    /// <see cref="MvRegisterAccessor{T}"/> writes it with, so a direct read of the
    /// stored bytes (bulk scan, keyword search) unwraps the identical shape the
    /// accessor round-trips.
    /// </summary>
    /// <param name="stored">The stored register blob. Must not be <see langword="null"/>.</param>
    /// <returns>The decoded register.</returns>
    internal static MvRegister DecodeRegister(byte[] stored) =>
        JsonLatticeSerializer<MvRegister>.Default.Deserialize(stored);

    /// <summary>The no-op <see cref="byte"/>[] serializer backing <see cref="ByteIdentity"/>.</summary>
    private sealed class IdentityByteSerializer : ILatticeSerializer<byte[]>
    {
        public byte[] Serialize(byte[] value) => value;

        public byte[] Deserialize(byte[] bytes) => bytes;
    }
}
