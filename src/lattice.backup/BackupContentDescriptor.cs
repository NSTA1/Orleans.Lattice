namespace Orleans.Lattice.Backup;

/// <summary>
/// Describes one artifact that makes up a backup: the sink artifact id, the
/// content digest, the byte length, the chunk count of the streamed payload, and
/// the sub-scope of the backup this artifact covers. The artifact id is minted per
/// capture (<c>{treeId}-{kind}-{ticks}-{guid}</c>), so the artifact itself is not
/// content-addressed. The digest (the lowercase hexadecimal SHA-256 of the
/// artifact bytes) is what restore and backup-health verification re-hash the
/// bytes against, and the capture engine derives the backup id from it: a full
/// backup's id is this digest, and an incremental's id is the SHA-256 of its base
/// backup id folded with it. A retried capture that produces identical bytes (off
/// the same base) therefore registers the same backup id, while still writing its
/// own artifact under a new artifact id. The per-artifact <see cref="Scope"/>
/// lets the descriptor granularity follow the backup definition rather than a
/// fixed per-shard or per-page shape.
/// </summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupContentDescriptor)]
[Immutable]
public sealed record BackupContentDescriptor
{
    /// <summary>Initializes a new <see cref="BackupContentDescriptor"/>.</summary>
    /// <param name="artifactId">The sink artifact id. Must not be <c>null</c> or empty.</param>
    /// <param name="contentHash">The content digest of the artifact bytes. Must not be <c>null</c> or empty.</param>
    /// <param name="byteLength">The total byte length of the artifact. Must not be negative.</param>
    /// <param name="chunkCount">The number of streamed chunks the artifact was written as. Must not be negative.</param>
    /// <param name="scope">The sub-scope of the backup this artifact covers. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentException"><paramref name="artifactId"/> or <paramref name="contentHash"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="byteLength"/> or <paramref name="chunkCount"/> is negative.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="scope"/> is <c>null</c>.</exception>
    public BackupContentDescriptor(
        string artifactId,
        string contentHash,
        long byteLength,
        int chunkCount,
        BackupScopeSelector scope)
    {
        ArgumentException.ThrowIfNullOrEmpty(artifactId);
        ArgumentException.ThrowIfNullOrEmpty(contentHash);
        ArgumentOutOfRangeException.ThrowIfNegative(byteLength);
        ArgumentOutOfRangeException.ThrowIfNegative(chunkCount);
        ArgumentNullException.ThrowIfNull(scope);
        ArtifactId = artifactId;
        ContentHash = contentHash;
        ByteLength = byteLength;
        ChunkCount = chunkCount;
        Scope = scope;
    }

    /// <summary>The sink artifact id, minted per capture (not derived from the artifact bytes).</summary>
    [Id(0)]
    public string ArtifactId { get; init; }

    /// <summary>The content digest of the artifact bytes (lowercase hexadecimal SHA-256).</summary>
    [Id(1)]
    public string ContentHash { get; init; }

    /// <summary>The total byte length of the artifact.</summary>
    [Id(2)]
    public long ByteLength { get; init; }

    /// <summary>The number of streamed chunks the artifact was written as.</summary>
    [Id(3)]
    public int ChunkCount { get; init; }

    /// <summary>The sub-scope of the backup this artifact covers.</summary>
    [Id(4)]
    public BackupScopeSelector Scope { get; init; }
}
