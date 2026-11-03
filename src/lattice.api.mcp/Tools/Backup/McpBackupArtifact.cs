namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// A compact projection of one content-addressed backup artifact for MCP
/// structured content. It surfaces the <see cref="ArtifactId"/> that
/// <c>lattice_backup_export_artifact</c> requires to drive an export, alongside
/// the content digest and size so an agent can pick and verify an artifact
/// without materialising its bytes.
/// </summary>
internal sealed record McpBackupArtifact
{
    /// <summary>The sink artifact id - the value to pass to export_artifact.</summary>
    public required string ArtifactId { get; init; }

    /// <summary>The content digest of the artifact bytes.</summary>
    public required string ContentHash { get; init; }

    /// <summary>The total byte length of the artifact.</summary>
    public required long ByteLength { get; init; }

    /// <summary>The number of streamed chunks the artifact was written as.</summary>
    public required int ChunkCount { get; init; }
}
