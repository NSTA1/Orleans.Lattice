namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// The result of a tree-existence check: whether the named logical tree is
/// registered in the tree registry. A pure read with no side effects.
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeExistenceResult)]
[Immutable]
public sealed record TreeExistenceResult
{
    /// <summary>The logical tree id whose existence was checked.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>
    /// <see langword="true"/> when the tree is registered; otherwise
    /// <see langword="false"/>.
    /// </summary>
    [Id(1)] public bool Exists { get; init; }
}
