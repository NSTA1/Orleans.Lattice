namespace Orleans.Lattice.Api.Schema.Grpc;

/// <summary>
/// Wire request of the accept-then-poll <c>StartComplianceScan</c> RPC: the tree to
/// scan and an optional caller-chosen idempotency id for the tracked operation.
/// </summary>
[GenerateSerializer]
[Alias(GrpcSchemaTypeAliases.SchemaComplianceScanStartRequest)]
[Immutable]
public sealed record SchemaComplianceScanStartRequest
{
    /// <summary>The governed tree id to scan.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The optional idempotency id; <see langword="null"/> lets the server generate one.</summary>
    [Id(1)] public string? OperationId { get; init; }
}
