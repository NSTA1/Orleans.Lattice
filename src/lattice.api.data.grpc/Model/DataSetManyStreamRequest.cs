namespace Orleans.Lattice.Api.Data.Grpc;

/// <summary>
/// One client frame of the duplex <c>StreamSetMany</c> RPC: one batch of upserts, written
/// non-atomically exactly as <c>SetMany</c> writes one, with each entry refused for its own
/// sake reported rather than failing the batch. Every frame of a stream names the same
/// tree; the client half-closes the stream to ask for the totals.
/// </summary>
/// <remarks>
/// Deliberately <b>not</b> marked <c>[Immutable]</c>: it nests mutable
/// <see cref="DataEntry"/> value buffers, so it must remain copy-eligible.
/// </remarks>
[GenerateSerializer]
[Alias(GrpcDataTypeAliases.DataSetManyStreamRequest)]
public sealed record DataSetManyStreamRequest
{
    /// <summary>Logical tree identifier. Every frame of a stream names the same tree.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>
    /// The batch's upserts, in any key order; at most
    /// <see cref="DataSetManyStreamOptions.MaxBatchSize"/>. An empty batch writes nothing.
    /// </summary>
    [Id(1)] public IReadOnlyList<DataEntry> Upserts { get; init; } = [];
}
