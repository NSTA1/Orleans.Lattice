namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of the <c>lattice_backup_export_artifact</c> tool:
/// one bounded page of a content-addressed artifact's bytes, base64-encoded, plus
/// a continuation cursor. The facade streams an artifact chunk-wise with bounded
/// memory; this tool drains at most one page-budget of bytes per call and
/// surfaces <see cref="NextChunkOffset"/> so a caller can resume without the
/// binding ever materialising the whole artifact. <see cref="EndOfStream"/> is
/// <see langword="true"/> on the final page.
/// </summary>
internal sealed record McpBackupArtifactPage
{
    /// <summary>The owning backup id.</summary>
    public required string BackupId { get; init; }

    /// <summary>The exported artifact id.</summary>
    public required string ArtifactId { get; init; }

    /// <summary>
    /// The base64-encoded artifact bytes for this page. Empty on a page that
    /// begins past the end of the artifact.
    /// </summary>
    public required string Base64Chunk { get; init; }

    /// <summary>The number of raw (pre-base64) bytes carried in this page.</summary>
    public required int ByteCount { get; init; }

    /// <summary>
    /// The chunk offset to pass back as the continuation cursor to resume the
    /// export, or <see langword="null"/> when <see cref="EndOfStream"/> is
    /// <see langword="true"/>.
    /// </summary>
    public int? NextChunkOffset { get; init; }

    /// <summary>Whether this is the final page of the artifact.</summary>
    public required bool EndOfStream { get; init; }
}
