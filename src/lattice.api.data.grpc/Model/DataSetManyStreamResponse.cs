namespace Orleans.Lattice.Api.Data.Grpc;

/// <summary>
/// One server frame of the duplex <c>StreamSetMany</c> RPC. Exactly one member is set: a
/// <see cref="Batch"/> outcome for each frame the client sent, in order, and finally the
/// stream's <see cref="Result"/> once the client half-closed.
/// </summary>
[GenerateSerializer]
[Alias(GrpcDataTypeAliases.DataSetManyStreamResponse)]
[Immutable]
public sealed record DataSetManyStreamResponse
{
    /// <summary>The outcome of one batch, or <see langword="null"/>.</summary>
    [Id(0)] public DataSetManyStreamBatch? Batch { get; init; }

    /// <summary>The stream's totals, on the last frame; otherwise <see langword="null"/>.</summary>
    [Id(1)] public DataSetManyStreamResult? Result { get; init; }
}
