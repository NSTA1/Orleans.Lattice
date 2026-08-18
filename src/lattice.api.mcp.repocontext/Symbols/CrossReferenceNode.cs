namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The reverse cross-reference projection for one referenced simple type-name,
/// stored at the key <c>repo/{repoId}/xref/{name}</c> (see
/// <see cref="RepoContextKeys.CrossReference(string, string)"/>). It is the inverse
/// of the outbound <see cref="SymbolRecord.References"/> edges: where a symbol
/// record lists the names <b>it</b> references, this record lists, for a given
/// name, the symbols that reference it (<see cref="Referrers"/>) and the test types
/// that cover it (<see cref="Tests"/>). The symbol reconciler maintains it
/// incrementally on every reconcile so <c>repocontext_related</c> can answer
/// inbound-dependent and test-linkage lookups without a full scan.
/// <para>
/// <see cref="RepoId"/> and <see cref="Name"/> are immutable identity derived from
/// the key. <see cref="Referrers"/> and <see cref="Tests"/> are add-wins
/// observed-remove sets, so concurrent reconcilers that add and drop distinct edges
/// converge; the reconciler prunes the whole record only once both sets are empty.
/// Merge with <see cref="Merge(CrossReferenceNode, CrossReferenceNode)"/>.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(RepoContextTypeAliases.CrossReferenceNode)]
internal sealed record CrossReferenceNode
{
    /// <summary>The repository identifier - immutable identity carried in the key.</summary>
    [Id(0)]
    public string RepoId { get; init; } = string.Empty;

    /// <summary>The referenced simple type-name - immutable identity carried in the key.</summary>
    [Id(1)]
    public string Name { get; init; } = string.Empty;

    /// <summary>
    /// Add-wins observed-remove set of the fully-qualified names of the symbols that
    /// reference <see cref="Name"/> (UTF-8 encoded elements). These are the inbound
    /// dependents surfaced by <c>repocontext_related</c>.
    /// </summary>
    [Id(2)]
    public OrSet Referrers { get; init; } = new();

    /// <summary>
    /// Add-wins observed-remove set of the fully-qualified names of the test types
    /// that cover the type named <see cref="Name"/> (UTF-8 encoded elements),
    /// recorded from the <c>{Name}Tests</c> / <c>{Name}Test</c> naming convention.
    /// </summary>
    [Id(3)]
    public OrSet Tests { get; init; } = new();

    /// <summary>
    /// Lattice merge of two replicas of the same cross-reference node. Identity is
    /// preserved from <paramref name="left"/> (falling back to <paramref name="right"/>
    /// only when the left side is unset); both edge sets are folded through their CRDT
    /// join, so the result is commutative, associative, and idempotent.
    /// </summary>
    /// <param name="left">The first replica. Must not be <see langword="null"/>.</param>
    /// <param name="right">The second replica. Must not be <see langword="null"/>.</param>
    public static CrossReferenceNode Merge(CrossReferenceNode left, CrossReferenceNode right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        return new CrossReferenceNode
        {
            RepoId = left.RepoId.Length != 0 ? left.RepoId : right.RepoId,
            Name = left.Name.Length != 0 ? left.Name : right.Name,
            Referrers = OrSet.Merge(left.Referrers, right.Referrers),
            Tests = OrSet.Merge(left.Tests, right.Tests),
        };
    }
}
