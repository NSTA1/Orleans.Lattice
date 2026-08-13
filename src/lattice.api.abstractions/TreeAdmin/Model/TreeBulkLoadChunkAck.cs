namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// The acknowledgement of a single appended bulk-load chunk from
/// <see cref="ILatticeTreeAdmin.AppendBulkLoadAsync"/>. It reports how many
/// entries the chunk contributed and the next chunk index the caller should send,
/// so a streaming producer can advance (or resume) without server-side state.
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeBulkLoadChunkAck)]
[Immutable]
public sealed record TreeBulkLoadChunkAck
{
    /// <summary>The tree the chunk was appended to.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The session operation id the chunk was appended under.</summary>
    [Id(1)] public required string OperationId { get; init; }

    /// <summary>The zero-based index of the chunk this acknowledgement is for.</summary>
    [Id(2)] public required long ChunkIndex { get; init; }

    /// <summary>
    /// The number of entries accepted from this chunk (after any server-side
    /// write interception), grafted onto the right edge of the tree.
    /// </summary>
    [Id(3)] public required int AcceptedEntryCount { get; init; }

    /// <summary>
    /// The next chunk index the caller should send, always
    /// <see cref="ChunkIndex"/> + 1. A caller that is unsure whether this chunk
    /// was applied (for example a lost acknowledgement) may re-drive the same
    /// <see cref="ChunkIndex"/> instead; the append is idempotent under the
    /// session operation id.
    /// </summary>
    [Id(4)] public required long NextChunkIndex { get; init; }
}
