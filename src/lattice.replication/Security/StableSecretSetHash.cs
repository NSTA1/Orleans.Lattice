namespace Orleans.Lattice.Replication;

/// <summary>
/// Deterministic 64-bit FNV-1a hash over the ordered, length-prefixed
/// concatenation of a secret set's UTF-16 code units. The output is stable
/// across processes and runtime restarts, unlike
/// <see cref="string.GetHashCode()"/> which is randomised per process.
/// </summary>
/// <remarks>
/// <para>
/// Used by <see cref="ILatticeReplicationSecretSource"/> implementations
/// to derive a <see cref="LatticeReplicationAcceptedSecrets.Version"/>
/// token. Two secret-source snapshots whose
/// <see cref="LatticeReplicationAcceptedSecrets.Version"/> values are
/// equal must represent the same ordered set; consumers may rely on
/// the equality to skip a no-op rebuild.
/// </para>
/// <para>
/// FNV-1a is <b>not</b> a cryptographic hash. The output never travels
/// on the wire and is never compared against a secret-derived value;
/// it is an in-memory change-token only.
/// </para>
/// </remarks>
internal static class StableSecretSetHash
{
    private const ulong FnvOffsetBasis = 14695981039346656037UL;
    private const ulong FnvPrime = 1099511628211UL;

    /// <summary>
    /// Hashes the supplied ordered sequence of strings. Each string is
    /// length-prefixed and then fed through the running FNV-1a state so
    /// distinct partitions cannot alias (e.g. <c>{"ab", "c"}</c> and
    /// <c>{"a", "bc"}</c> hash differently). Null entries are treated as
    /// empty.
    /// </summary>
    public static string Compute(IReadOnlyList<string?> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);

        var state = FnvOffsetBasis;
        for (var i = 0; i < entries.Count; i++)
        {
            var s = entries[i];
            var length = s?.Length ?? 0;

            // Length-prefix the entry so its extent frames its content in the
            // digest before its bytes. A fixed NUL separator byte cannot
            // delimit entries reliably because it is indistinguishable from a
            // NUL that occurs inside an entry - both an embedded '\0' and the
            // high byte of any ASCII code unit hash as 0x00 - so e.g.
            // {"a\0\0c"} and {"a", "\0", "c"} (and {"\0"} and {null, null,
            // null}) produced an identical separator-delimited byte stream and
            // collided, violating the "distinct partitions cannot alias"
            // contract. Framing each entry by its length instead makes the
            // partition unambiguous, matching the sibling
            // ReplicationContentHash.
            state = HashLength(state, length);

            // Avoid Encoding.UTF8.GetBytes allocation by hashing the UTF-16
            // code units directly. Determinism only requires a stable
            // byte-stream view; UTF-16 little-endian as exposed by string
            // indexing is stable across runtimes.
            for (var j = 0; j < length; j++)
            {
                var ch = s![j];
                state = (state ^ (byte)(ch & 0xFF)) * FnvPrime;
                state = (state ^ (byte)((ch >> 8) & 0xFF)) * FnvPrime;
            }
        }
        return state.ToString("x16");
    }

    private static ulong HashLength(ulong state, int length)
    {
        // Fold the 32-bit entry length low byte first so an entry's extent
        // frames its content in the digest.
        var bits = (uint)length;
        for (var shift = 0; shift < 32; shift += 8)
        {
            state = (state ^ (byte)((bits >> shift) & 0xFF)) * FnvPrime;
        }
        return state;
    }
}
