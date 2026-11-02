namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// One server frame of the duplex <c>StreamBulkLoad</c> RPC. Exactly one member is
/// set: <see cref="Opened"/> once the session is open, an <see cref="Ack"/> for each
/// chunk appended, in chunk order, and finally the <see cref="Result"/> of the commit
/// the client's half-close asked for.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTreeAdminTypeAliases.TreeAdminBulkLoadStreamResponse)]
[Immutable]
public sealed record TreeAdminBulkLoadStreamResponse
{
    /// <summary>Whether this frame confirms the session is open (the tree was empty, or the stream resumes one).</summary>
    [Id(0)] public bool Opened { get; init; }

    /// <summary>The acknowledgement of one appended chunk, or <see langword="null"/>.</summary>
    [Id(1)] public TreeBulkLoadChunkAck? Ack { get; init; }

    /// <summary>The committed load's result, on the last frame; otherwise <see langword="null"/>.</summary>
    [Id(2)] public TreeBulkLoadResult? Result { get; init; }
}
