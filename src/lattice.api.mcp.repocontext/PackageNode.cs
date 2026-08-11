namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// A package / module / directory structural node stored at the key
/// <c>repo/{repoId}/pkg/{path}</c> (see
/// <see cref="RepoContextKeys.Package(string, string)"/>). Groups the files and
/// symbols beneath a path in the repository.
/// <para>
/// <see cref="RepoId"/> and <see cref="Path"/> are immutable identity derived
/// from the key; all other state is CRDT-backed (last-writer-wins scalars plus an
/// add-wins tag set) so concurrent ingesters converge. Merge with
/// <see cref="Merge(PackageNode, PackageNode)"/>.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(RepoContextTypeAliases.PackageNode)]
internal sealed record PackageNode
{
    /// <summary>The repository identifier - immutable identity carried in the key.</summary>
    [Id(0)]
    public string RepoId { get; init; } = string.Empty;

    /// <summary>The package path - immutable identity carried in the key.</summary>
    [Id(1)]
    public string Path { get; init; } = string.Empty;

    /// <summary>Last-writer-wins primary language of the package.</summary>
    [Id(2)]
    public BoundedRegister Language { get; init; } = new();

    /// <summary>Last-writer-wins package version string.</summary>
    [Id(3)]
    public BoundedRegister Version { get; init; } = new();

    /// <summary>Last-writer-wins last-ingested marker.</summary>
    [Id(4)]
    public BoundedRegister LastIngested { get; init; } = new();

    /// <summary>Add-wins observed-remove set of free-form tags (UTF-8 encoded elements).</summary>
    [Id(5)]
    public OrSet Tags { get; init; } = new();

    /// <summary>
    /// Lattice merge of two replicas of the same package node. Identity is
    /// preserved from <paramref name="left"/>; every mutable field is folded
    /// through its CRDT join, so the result is commutative, associative, and
    /// idempotent.
    /// </summary>
    /// <param name="left">The first replica. Must not be <see langword="null"/>.</param>
    /// <param name="right">The second replica. Must not be <see langword="null"/>.</param>
    public static PackageNode Merge(PackageNode left, PackageNode right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        return new PackageNode
        {
            RepoId = left.RepoId.Length != 0 ? left.RepoId : right.RepoId,
            Path = left.Path.Length != 0 ? left.Path : right.Path,
            Language = BoundedRegister.Merge(left.Language, right.Language),
            Version = BoundedRegister.Merge(left.Version, right.Version),
            LastIngested = BoundedRegister.Merge(left.LastIngested, right.LastIngested),
            Tags = OrSet.Merge(left.Tags, right.Tags),
        };
    }
}
