namespace Orleans.Lattice.Replication;

/// <summary>
/// Deterministic 64-bit FNV-1a hash over the ordered, NUL-separated
/// concatenation of a secret set's UTF-8 bytes. The output is stable
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
    /// fed through the running FNV-1a state followed by a single NUL
    /// byte so distinct partitions cannot alias (e.g.
    /// <c>{"ab", "c"}</c> and <c>{"a", "bc"}</c> hash differently).
    /// Null entries are treated as empty.
    /// </summary>
    public static string Compute(IReadOnlyList<string?> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);

        var state = FnvOffsetBasis;
        for (var i = 0; i < entries.Count; i++)
        {
            var s = entries[i];
            if (!string.IsNullOrEmpty(s))
            {
                // Avoid Encoding.UTF8.GetBytes allocation by hashing
                // the UTF-16 code units directly. Determinism only
                // requires a stable byte-stream view; UTF-16 little-
                // endian as exposed by string indexing is stable
                // across runtimes.
                for (var j = 0; j < s.Length; j++)
                {
                    var ch = s[j];
                    state = (state ^ (byte)(ch & 0xFF)) * FnvPrime;
                    state = (state ^ (byte)((ch >> 8) & 0xFF)) * FnvPrime;
                }
            }
            // Field separator so set partitions cannot alias.
            state = (state ^ 0x00) * FnvPrime;
        }
        return state.ToString("x16");
    }
}
