using System.Globalization;

namespace Orleans.Lattice.Vector.Persistence;

/// <summary>
/// The durable key layout of a persisted <see cref="VectorIndex"/>, rooted at a
/// caller-chosen prefix so several indexes can share one tree.
/// <para>
/// Two counters shape the layout. A <i>generation</i> covers a whole
/// partitioning: retraining changes every cell's membership, so a retrain writes
/// a fresh generation and flips the manifest to it rather than editing the live
/// one in place. An <i>epoch</i> covers one flush inside a generation: a dirty
/// partition's chunks are written under a new epoch and committed by rewriting
/// that partition's state record, so an interrupted flush leaves an uncommitted
/// epoch the loader ignores and the next flush sweeps. Neither counter is ever
/// reused, and both are rendered zero-padded so ordinal key order is numeric
/// order.
/// </para>
/// </summary>
public static class VectorIndexStorageKeys
{
    /// <summary>The width every generation and epoch component is padded to.</summary>
    public const int CounterWidth = 19;

    /// <summary>The width every partition identifier is padded to.</summary>
    public const int PartitionWidth = 5;

    /// <summary>The width every chunk sequence number is padded to.</summary>
    public const int SequenceWidth = 8;

    /// <summary>
    /// The commit record for the whole index: the manifest naming the live
    /// generation and carrying the snapshot header. Written last by every flush.
    /// </summary>
    /// <param name="prefix">The index's root key prefix.</param>
    /// <exception cref="ArgumentNullException"><paramref name="prefix"/> is null.</exception>
    public static string Manifest(string prefix)
    {
        ArgumentNullException.ThrowIfNull(prefix);
        return prefix + "m";
    }

    /// <summary>
    /// The resumable background-build checkpoint: the phase reached and the
    /// source cursor consumed so far.
    /// </summary>
    /// <param name="prefix">The index's root key prefix.</param>
    /// <exception cref="ArgumentNullException"><paramref name="prefix"/> is null.</exception>
    public static string BuildState(string prefix)
    {
        ArgumentNullException.ThrowIfNull(prefix);
        return prefix + "b";
    }

    /// <summary>The prefix covering every retirement tombstone.</summary>
    /// <param name="prefix">The index's root key prefix.</param>
    /// <exception cref="ArgumentNullException"><paramref name="prefix"/> is null.</exception>
    public static string RetirementPrefix(string prefix)
    {
        ArgumentNullException.ThrowIfNull(prefix);
        return prefix + "t/";
    }

    /// <summary>
    /// The tombstone recording that a key is being retired. Written before the
    /// in-memory removal and dropped only once that removal is durable, so an
    /// interrupted deletion is completed on the next load rather than lost.
    /// </summary>
    /// <param name="prefix">The index's root key prefix.</param>
    /// <param name="key">The index key being retired.</param>
    /// <exception cref="ArgumentNullException"><paramref name="prefix"/> is null.</exception>
    public static string Retirement(string prefix, long key) =>
        RetirementPrefix(prefix) + key.ToString("X16", CultureInfo.InvariantCulture);

    /// <summary>
    /// Recovers the index key a <see cref="Retirement"/> tombstone was written
    /// for, given the key a scan returned.
    /// </summary>
    /// <param name="prefix">The index's root key prefix.</param>
    /// <param name="key">A key produced by <see cref="Retirement"/>.</param>
    /// <param name="indexKey">The retired index key when this returns <see langword="true"/>.</param>
    /// <returns><see langword="true"/> when the key is a well-formed tombstone key.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="prefix"/> or <paramref name="key"/> is null.</exception>
    public static bool TryReadRetirementKey(string prefix, string key, out long indexKey)
    {
        ArgumentNullException.ThrowIfNull(key);
        var tombstonePrefix = RetirementPrefix(prefix);
        indexKey = 0;
        return key.StartsWith(tombstonePrefix, StringComparison.Ordinal)
            && long.TryParse(
                key.AsSpan(tombstonePrefix.Length),
                NumberStyles.HexNumber,
                CultureInfo.InvariantCulture,
                out indexKey);
    }

    /// <summary>The durable identifier watermark of the key dictionary.</summary>
    /// <param name="prefix">The index's root key prefix.</param>
    /// <exception cref="ArgumentNullException"><paramref name="prefix"/> is null.</exception>
    public static string KeyWatermark(string prefix)
    {
        ArgumentNullException.ThrowIfNull(prefix);
        return prefix + "k/w";
    }

    /// <summary>The prefix covering every external-identifier mapping record.</summary>
    /// <param name="prefix">The index's root key prefix.</param>
    /// <exception cref="ArgumentNullException"><paramref name="prefix"/> is null.</exception>
    public static string KeyMapPrefix(string prefix)
    {
        ArgumentNullException.ThrowIfNull(prefix);
        return prefix + "k/f/";
    }

    /// <summary>
    /// The record mapping one external identifier to its index key. The
    /// identifier is carried verbatim in the key, so the whole mapping is one
    /// prefix scan away and needs no secondary structure.
    /// </summary>
    /// <param name="prefix">The index's root key prefix.</param>
    /// <param name="externalId">The caller's identifier.</param>
    /// <exception cref="ArgumentNullException"><paramref name="prefix"/> or <paramref name="externalId"/> is null.</exception>
    public static string KeyMap(string prefix, string externalId)
    {
        ArgumentNullException.ThrowIfNull(externalId);
        return KeyMapPrefix(prefix) + externalId;
    }

    /// <summary>
    /// Recovers the external identifier a <see cref="KeyMap"/> record was written
    /// for, given the key a scan returned.
    /// </summary>
    /// <param name="prefix">The index's root key prefix.</param>
    /// <param name="key">A key produced by <see cref="KeyMap"/>.</param>
    /// <param name="externalId">The identifier when this returns <see langword="true"/>.</param>
    /// <returns><see langword="true"/> when the key belongs to the mapping range.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="prefix"/> or <paramref name="key"/> is null.</exception>
    public static bool TryReadKeyMapId(string prefix, string key, out string externalId)
    {
        ArgumentNullException.ThrowIfNull(key);
        var mapPrefix = KeyMapPrefix(prefix);
        if (!key.StartsWith(mapPrefix, StringComparison.Ordinal))
        {
            externalId = string.Empty;
            return false;
        }

        externalId = key[mapPrefix.Length..];
        return true;
    }

    /// <summary>The prefix covering one whole generation.</summary>
    /// <param name="prefix">The index's root key prefix.</param>
    /// <param name="generation">The generation number.</param>
    /// <exception cref="ArgumentNullException"><paramref name="prefix"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="generation"/> is negative.</exception>
    public static string GenerationPrefix(string prefix, long generation)
    {
        ArgumentNullException.ThrowIfNull(prefix);
        ArgumentOutOfRangeException.ThrowIfNegative(generation);
        return prefix + "g/" + Counter(generation) + "/";
    }

    /// <summary>The prefix covering every generation, used to sweep superseded ones.</summary>
    /// <param name="prefix">The index's root key prefix.</param>
    /// <exception cref="ArgumentNullException"><paramref name="prefix"/> is null.</exception>
    public static string AllGenerationsPrefix(string prefix)
    {
        ArgumentNullException.ThrowIfNull(prefix);
        return prefix + "g/";
    }

    /// <summary>The prefix covering the centroid chunks of one epoch.</summary>
    /// <param name="prefix">The index's root key prefix.</param>
    /// <param name="generation">The generation number.</param>
    /// <param name="epoch">The epoch the centroids were written under.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="generation"/> or <paramref name="epoch"/> is negative.</exception>
    public static string CentroidPrefix(string prefix, long generation, long epoch)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(epoch);
        return GenerationPrefix(prefix, generation) + "c/" + Counter(epoch) + "/";
    }

    /// <summary>One centroid chunk.</summary>
    /// <param name="prefix">The index's root key prefix.</param>
    /// <param name="generation">The generation number.</param>
    /// <param name="epoch">The epoch the centroids were written under.</param>
    /// <param name="sequence">The chunk's zero-based sequence number.</param>
    /// <exception cref="ArgumentOutOfRangeException">A component is negative.</exception>
    public static string CentroidChunk(string prefix, long generation, long epoch, int sequence)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(sequence);
        return CentroidPrefix(prefix, generation, epoch) + Sequence(sequence);
    }

    /// <summary>The prefix covering every partition-state record of a generation.</summary>
    /// <param name="prefix">The index's root key prefix.</param>
    /// <param name="generation">The generation number.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="generation"/> is negative.</exception>
    public static string PartitionStatePrefix(string prefix, long generation) =>
        GenerationPrefix(prefix, generation) + "p/";

    /// <summary>
    /// One partition's commit record: the epoch its chunks were written under and
    /// how many of them there are. Written after those chunks, so it is the point
    /// at which a partition rewrite becomes visible.
    /// </summary>
    /// <param name="prefix">The index's root key prefix.</param>
    /// <param name="generation">The generation number.</param>
    /// <param name="partitionId">The zero-based partition identifier.</param>
    /// <exception cref="ArgumentOutOfRangeException">A component is negative.</exception>
    public static string PartitionState(string prefix, long generation, int partitionId)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(partitionId);
        return PartitionStatePrefix(prefix, generation) + Partition(partitionId);
    }

    /// <summary>The prefix covering every vector chunk of one partition, across all epochs.</summary>
    /// <param name="prefix">The index's root key prefix.</param>
    /// <param name="generation">The generation number.</param>
    /// <param name="partitionId">The zero-based partition identifier.</param>
    /// <exception cref="ArgumentOutOfRangeException">A component is negative.</exception>
    public static string PartitionVectorPrefix(string prefix, long generation, int partitionId)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(partitionId);
        return GenerationPrefix(prefix, generation) + "v/" + Partition(partitionId) + "/";
    }

    /// <summary>The prefix covering one partition's vector chunks at one epoch.</summary>
    /// <param name="prefix">The index's root key prefix.</param>
    /// <param name="generation">The generation number.</param>
    /// <param name="partitionId">The zero-based partition identifier.</param>
    /// <param name="epoch">The epoch the chunks were written under.</param>
    /// <exception cref="ArgumentOutOfRangeException">A component is negative.</exception>
    public static string PartitionEpochPrefix(string prefix, long generation, int partitionId, long epoch)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(epoch);
        return PartitionVectorPrefix(prefix, generation, partitionId) + Counter(epoch) + "/";
    }

    /// <summary>One vector chunk.</summary>
    /// <param name="prefix">The index's root key prefix.</param>
    /// <param name="generation">The generation number.</param>
    /// <param name="partitionId">The zero-based partition identifier.</param>
    /// <param name="epoch">The epoch the chunk was written under.</param>
    /// <param name="sequence">The chunk's zero-based sequence number within the partition.</param>
    /// <exception cref="ArgumentOutOfRangeException">A component is negative.</exception>
    public static string VectorChunk(string prefix, long generation, int partitionId, long epoch, int sequence)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(sequence);
        return PartitionEpochPrefix(prefix, generation, partitionId, epoch) + Sequence(sequence);
    }

    private static string Counter(long value) =>
        value.ToString(CultureInfo.InvariantCulture).PadLeft(CounterWidth, '0');

    private static string Partition(int value) =>
        value.ToString(CultureInfo.InvariantCulture).PadLeft(PartitionWidth, '0');

    private static string Sequence(int value) =>
        value.ToString(CultureInfo.InvariantCulture).PadLeft(SequenceWidth, '0');
}
