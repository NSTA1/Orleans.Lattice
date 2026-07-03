namespace Orleans.Lattice.Api.Auth.Grpc;

/// <summary>
/// Wire response carrying an ordered list of ids. Returned by the membership
/// query RPCs whose facade result is an <c>IReadOnlyList&lt;string&gt;</c>
/// (<c>ListGroupMembersAsync</c> and <c>ListSubjectGroupsAsync</c>), preserving
/// the facade's ascending ordinal ordering.
/// </summary>
[GenerateSerializer]
[Alias(ApiAuthTypeAliases.AuthStringList)]
[Immutable]
public sealed record AuthStringList
{
    /// <summary>The ids on this result, in ascending ordinal order.</summary>
    [Id(0)] public IReadOnlyList<string> Values { get; init; } = Array.Empty<string>();
}
