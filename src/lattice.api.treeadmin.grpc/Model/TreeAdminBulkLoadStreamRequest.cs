using Orleans.Lattice.Api.Data;

namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// One client frame of the duplex <c>StreamBulkLoad</c> RPC. The first frame opens the
/// stream and names the session (<see cref="TreeId"/> and <see cref="OperationId"/>);
/// every frame carrying entries is one chunk, appended at <see cref="ChunkIndex"/>
/// exactly as <c>AppendBulkLoad</c> appends it. When the client half-closes the stream
/// the server commits the load and answers with its result.
/// </summary>
/// <remarks>
/// <see cref="Resume"/> on the first frame continues a session an earlier stream opened,
/// skipping the emptiness check that opens one: the client sets it once the server has
/// confirmed the session open, and then re-sends every chunk not yet acknowledged, each
/// an idempotent no-op if it had already landed. Skipping the check grants nothing a
/// caller holding the bulk-load capability could not already do through the unary
/// <c>AppendBulkLoad</c> RPC, which never re-checks emptiness either.
/// </remarks>
[GenerateSerializer]
[Alias(GrpcTreeAdminTypeAliases.TreeAdminBulkLoadStreamRequest)]
[Immutable]
public sealed record TreeAdminBulkLoadStreamRequest
{
    /// <summary>The tree the session loads. Every frame of a stream names the same tree.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The session's idempotency key. Every frame of a stream names the same operation.</summary>
    [Id(1)] public required string OperationId { get; init; }

    /// <summary>
    /// On the first frame, whether the stream continues a session already opened rather
    /// than opening one. Ignored on later frames.
    /// </summary>
    [Id(2)] public bool Resume { get; init; }

    /// <summary>The zero-based chunk index the frame's entries are appended at. Ignored when the frame carries no entries.</summary>
    [Id(3)] public long ChunkIndex { get; init; }

    /// <summary>The chunk's entries, in strictly ascending key order. A frame with none appends nothing.</summary>
    [Id(4)] public IReadOnlyList<DataEntry> Entries { get; init; } = [];
}
