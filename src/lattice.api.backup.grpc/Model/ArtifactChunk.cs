namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// One ordered chunk of an artifact's bytes, streamed by the server-streaming
/// export-artifact RPC. The facade yields the artifact as a sequence of
/// <see cref="ReadOnlyMemory{T}"/> segments; each is carried on the wire as a
/// single <see cref="Data"/> chunk so a large artifact streams with bounded
/// memory instead of being materialized whole.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.ArtifactChunk)]
[Immutable]
public sealed record ArtifactChunk
{
    /// <summary>The chunk's bytes, in artifact order.</summary>
    [Id(0)] public required byte[] Data { get; init; }
}
