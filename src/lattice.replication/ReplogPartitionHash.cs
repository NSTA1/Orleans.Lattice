namespace Orleans.Lattice.Replication;

/// <summary>
/// Stable hash function used by <see cref="ShardedReplogSink"/> to map
/// a <see cref="ReplogEntry.Key"/> to a WAL partition index.
/// <para>
/// Stability across silos and processes is required: two activations
/// of the producer that hash the same key must pick the same partition,
/// otherwise concurrent appends would split across grain activations
/// and the per-partition sequence ordering used by downstream shippers
/// would lose its meaning. <see cref="string.GetHashCode()"/> is
/// process-randomised on modern .NET and therefore unsuitable, so a
/// dedicated FNV-1a 32-bit hash over the UTF-16 code units is used.
/// </para>
/// </summary>
internal static class ReplogPartitionHash
{
    /// <summary>FNV-1a 32-bit offset basis.</summary>
    private const uint OffsetBasis = 2166136261u;

    /// <summary>FNV-1a 32-bit prime.</summary>
    private const uint Prime = 16777619u;

    /// <summary>
    /// Returns a stable, non-negative partition index in
    /// <c>[0, partitions)</c> for <paramref name="key"/>.
    /// </summary>
    /// <param name="key">The replog entry key.</param>
    /// <param name="partitions">The total partition count; must be at least <c>1</c>.</param>
    public static int Compute(string key, int partitions)
    {
        ArgumentNullException.ThrowIfNull(key);
        if (partitions < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(partitions),
                partitions,
                "Partition count must be at least 1.");
        }

        if (partitions == 1)
        {
            return 0;
        }

        var hash = OffsetBasis;
        for (var i = 0; i < key.Length; i++)
        {
            var c = key[i];
            hash = (hash ^ (byte)(c & 0xFF)) * Prime;
            hash = (hash ^ (byte)((c >> 8) & 0xFF)) * Prime;
        }

        return (int)(hash % (uint)partitions);
    }
}
