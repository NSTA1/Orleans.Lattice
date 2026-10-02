using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Schema.Grpc;

/// <summary>Wire request for the <c>Remediate</c> and <c>StartRemediation</c> RPCs.</summary>
[GenerateSerializer]
[Alias(GrpcSchemaTypeAliases.RemediateRequest)]
[Immutable]
public sealed record RemediateRequest
{
    /// <summary>The governed tree id to remediate.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The per-value remediation transform to apply.</summary>
    [Id(1)] public required LatticeValueTransform Transform { get; init; }

    /// <summary>The policy the transformed values must satisfy.</summary>
    [Id(2)] public required LatticeSchemaPolicy TargetPolicy { get; init; }

    /// <summary>
    /// The caller's idempotency id for the <c>StartRemediation</c> RPC, or <c>null</c>
    /// to have one generated. Ignored by the blocking <c>Remediate</c> RPC.
    /// </summary>
    [Id(3)] public string? OperationId { get; init; }
}
