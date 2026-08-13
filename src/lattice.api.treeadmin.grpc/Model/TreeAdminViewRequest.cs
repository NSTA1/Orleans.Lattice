namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Wire request for the per-view materialised-view administration RPCs (status,
/// rebuild, reconcile, drop): the logical view name the operation targets. The name
/// is carried on the wire so the facade resolves the view's source tree
/// authoritatively and applies the same argument validation, source-tree gating, and
/// <c>InvalidArgument</c> / <c>NotFound</c> / <c>FailedPrecondition</c> mapping a
/// local caller sees.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTreeAdminTypeAliases.TreeAdminViewRequest)]
[Immutable]
public sealed record TreeAdminViewRequest
{
    /// <summary>The logical materialised-view name the operation targets.</summary>
    [Id(0)] public required string ViewName { get; init; }
}
