namespace Orleans.Lattice.Backup;

/// <summary>
/// The per-origin high-water provenance of a captured backup: for one
/// contributing replica origin, the highest write-ahead-log sequence whose
/// mutations are included in the capture. Restore uses this to drive per-origin
/// re-sync through the existing anti-entropy digest / re-replay path.
/// </summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupOriginProvenance)]
[Immutable]
public sealed record BackupOriginProvenance
{
    /// <summary>Initializes a new <see cref="BackupOriginProvenance"/>.</summary>
    /// <param name="originId">The contributing replica origin id. Must not be <c>null</c> or empty.</param>
    /// <param name="highWaterSequence">
    /// The highest write-ahead-log sequence from <paramref name="originId"/>
    /// included in the capture. Must not be negative.
    /// </param>
    /// <exception cref="ArgumentException"><paramref name="originId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="highWaterSequence"/> is negative.</exception>
    public BackupOriginProvenance(string originId, long highWaterSequence)
    {
        ArgumentException.ThrowIfNullOrEmpty(originId);
        ArgumentOutOfRangeException.ThrowIfNegative(highWaterSequence);
        OriginId = originId;
        HighWaterSequence = highWaterSequence;
    }

    /// <summary>The contributing replica origin id.</summary>
    [Id(0)]
    public string OriginId { get; init; }

    /// <summary>The highest write-ahead-log sequence from this origin included in the capture.</summary>
    [Id(1)]
    public long HighWaterSequence { get; init; }
}
