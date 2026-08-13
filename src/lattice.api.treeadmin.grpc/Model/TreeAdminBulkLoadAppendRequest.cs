using Orleans.Lattice.Api.Data;

namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Wire request for the <c>AppendBulkLoad</c> RPC: one strictly-ascending chunk of
/// entries grafted onto the tree under a bulk-load session. Carries the session
/// identity (<see cref="TreeId"/> plus <see cref="OperationId"/>), the monotonic
/// <see cref="ChunkIndex"/> that keys per-chunk idempotency, and the chunk's
/// ordered <see cref="Entries"/>. Re-sending the same <see cref="ChunkIndex"/>
/// with the same operation id is idempotent, so a client resumes a broken stream
/// by replaying from its last un-acknowledged chunk.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTreeAdminTypeAliases.TreeAdminBulkLoadAppendRequest)]
[Immutable]
public sealed record TreeAdminBulkLoadAppendRequest
{
    /// <summary>The tree the bulk-load session targets.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The caller's stable, idempotent bulk-load operation id.</summary>
    [Id(1)] public required string OperationId { get; init; }

    /// <summary>The zero-based, monotonically increasing chunk index.</summary>
    [Id(2)] public long ChunkIndex { get; init; }

    /// <summary>The chunk's entries, in strictly ascending key order.</summary>
    [Id(3)] public IReadOnlyList<DataEntry> Entries { get; init; } = [];
}
