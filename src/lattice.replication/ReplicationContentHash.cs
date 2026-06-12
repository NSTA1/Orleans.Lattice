using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Stable, allocation-light content hash over the content-defining
/// fields of a <see cref="WalRecord"/>: the mutation kind, the key,
/// the (optional) exclusive range end, and the committed value bytes.
/// The hash is the 64-bit FNV-1a digest of those fields concatenated
/// with field separators so distinct shapes cannot alias. It is the
/// building block of the sender-side content-hash dedup measurement
/// (<see cref="LatticeReplicationOptions.ContentHashDedupEnabled"/>):
/// two captured writes that re-set byte-identical content for the same
/// key produce the same digest, which is what lets the shipper count
/// the redundant payload re-send rate that idempotent upstream retry
/// logic generates.
/// <para>
/// FNV-1a is <b>not</b> a cryptographic hash and the digest never
/// travels on the wire; it is an in-process change-token only, matching
/// the convention already used by
/// <see cref="EncodedBatchHeader.HashClusterId(string)"/> and the
/// secret-set change token. A hash collision can at worst mis-classify
/// one distinct payload as a redundant re-send on the measurement
/// counter; it never elides a payload or alters apply semantics,
/// because the measurement is observability-only and every entry is
/// still shipped verbatim.
/// </para>
/// </summary>
internal static class ReplicationContentHash
{
    private const ulong FnvOffsetBasis = 14695981039346656037UL;
    private const ulong FnvPrime = 1099511628211UL;

    /// <summary>
    /// Computes the stable content hash of the supplied record from its
    /// <see cref="WalRecord.Op"/>, <see cref="WalRecord.Key"/>,
    /// <see cref="WalRecord.EndExclusiveKey"/>, and
    /// <see cref="WalRecord.Value"/>.
    /// </summary>
    public static ulong Compute(in WalRecord record) =>
        Compute(record.Op, record.Key, record.EndExclusiveKey, record.Value);

    /// <summary>
    /// Computes the stable content hash from the individual
    /// content-defining fields. Exposed as an overload so unit tests
    /// can exercise the digest without constructing a full
    /// <see cref="WalRecord"/>. <paramref name="value"/> may be empty
    /// (deletes carry no payload).
    /// </summary>
    public static ulong Compute(MutationKind op, string? key, string? endExclusiveKey, ReadOnlySpan<byte> value)
    {
        var state = FnvOffsetBasis;
        state = (state ^ (byte)op) * FnvPrime;
        state = HashSeparator(state);
        state = HashString(state, key);
        state = HashSeparator(state);
        state = HashString(state, endExclusiveKey);
        state = HashSeparator(state);
        for (var i = 0; i < value.Length; i++)
        {
            state = (state ^ value[i]) * FnvPrime;
        }
        return state;
    }

    private static ulong HashString(ulong state, string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return state;
        }
        // Hash the UTF-16 code units directly (low byte then high byte)
        // so the digest is stable across runtimes without allocating a
        // UTF-8 byte buffer, matching StableSecretSetHash.
        for (var i = 0; i < value.Length; i++)
        {
            var ch = value[i];
            state = (state ^ (byte)(ch & 0xFF)) * FnvPrime;
            state = (state ^ (byte)((ch >> 8) & 0xFF)) * FnvPrime;
        }
        return state;
    }

    private static ulong HashSeparator(ulong state) => (state ^ 0x00) * FnvPrime;
}
