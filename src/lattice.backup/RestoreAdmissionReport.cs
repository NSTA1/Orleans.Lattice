namespace Orleans.Lattice.Backup;

/// <summary>
/// The self-describing size and topology of a restore, resolved from the target
/// backup's manifest chain before any fence is engaged or shadow tree is built.
/// A coordinated restore uses it to hard-refuse an infeasible target up front (a
/// cheap early "no") rather than fence the fleet, build most of a large shadow,
/// and only then fail on a small cluster. A plain in-process value: it is never
/// serialized over a grain boundary, so it carries no Orleans alias.
/// </summary>
public sealed class RestoreAdmissionReport
{
    /// <summary>Initializes a new <see cref="RestoreAdmissionReport"/>.</summary>
    /// <param name="backupId">The backup id probed. Must not be <c>null</c> or empty.</param>
    /// <param name="targetTreeId">The tree the restore targets. Must not be <c>null</c> or empty.</param>
    /// <param name="totalByteLength">The summed artifact byte length across the whole chain. Must not be negative.</param>
    /// <param name="totalChunkCount">The summed streamed-chunk count across the whole chain. Must not be negative.</param>
    /// <param name="shardCount">The captured tree's shard count. Must be positive.</param>
    /// <param name="manifestChain">The base-first ordered chain of backup ids. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentException">A required string argument is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="manifestChain"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A numeric argument is out of range.</exception>
    public RestoreAdmissionReport(
        string backupId,
        string targetTreeId,
        long totalByteLength,
        long totalChunkCount,
        int shardCount,
        IReadOnlyList<string> manifestChain)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        ArgumentException.ThrowIfNullOrEmpty(targetTreeId);
        ArgumentOutOfRangeException.ThrowIfNegative(totalByteLength);
        ArgumentOutOfRangeException.ThrowIfNegative(totalChunkCount);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(shardCount);
        ArgumentNullException.ThrowIfNull(manifestChain);

        BackupId = backupId;
        TargetTreeId = targetTreeId;
        TotalByteLength = totalByteLength;
        TotalChunkCount = totalChunkCount;
        ShardCount = shardCount;
        ManifestChain = manifestChain;
    }

    /// <summary>The backup id probed.</summary>
    public string BackupId { get; }

    /// <summary>The tree the restore targets.</summary>
    public string TargetTreeId { get; }

    /// <summary>The summed artifact byte length the shadow build will materialize.</summary>
    public long TotalByteLength { get; }

    /// <summary>The summed streamed-chunk count across the whole chain.</summary>
    public long TotalChunkCount { get; }

    /// <summary>The captured tree's shard count.</summary>
    public int ShardCount { get; }

    /// <summary>The base-first ordered chain of backup ids that would be replayed.</summary>
    public IReadOnlyList<string> ManifestChain { get; }
}
