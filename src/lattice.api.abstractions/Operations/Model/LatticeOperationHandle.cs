namespace Orleans.Lattice.Api.Operations;

/// <summary>
/// The handle a start verb returns as soon as a long-running operation has been
/// accepted: the id to poll with, its kind and its scope. The work continues in
/// the background, independent of the call that started it.
/// </summary>
[GenerateSerializer]
[Alias(ApiOperationTypeAliases.LatticeOperationHandle)]
[Immutable]
public sealed record LatticeOperationHandle
{
    /// <summary>The operation id, unique within the owning tenant.</summary>
    [Id(0)] public required string OperationId { get; init; }

    /// <summary>The operation kind, an open string such as <c>backup.capture</c>.</summary>
    [Id(1)] public required string Kind { get; init; }

    /// <summary>The operation's scope.</summary>
    [Id(2)] public required LatticeOperationScope Scope { get; init; }

    /// <summary>
    /// <see langword="true"/> when this call started the operation;
    /// <see langword="false"/> when an operation with the same id already existed
    /// and this call started nothing (an idempotent start).
    /// </summary>
    [Id(3)] public bool Created { get; init; }
}
