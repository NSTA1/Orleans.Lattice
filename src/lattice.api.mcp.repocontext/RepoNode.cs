namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The repository root structural node stored at the key
/// <c>repo/{repoId}</c> (see <see cref="RepoContextKeys.Repo(string)"/>). Holds
/// repository-level metadata that concurrent ingesters may update.
/// <para>
/// <see cref="RepoId"/> is immutable identity derived from the key; all other
/// state is CRDT-backed so concurrent writes from different agents or sessions
/// converge without loss: scalar metadata uses last-writer-wins registers (see
/// <see cref="RepoContextValues"/>) and <see cref="Tags"/> is an add-wins
/// observed-remove set. Merge the node with <see cref="Merge(RepoNode, RepoNode)"/>.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(RepoContextTypeAliases.RepoNode)]
internal sealed record RepoNode
{
    /// <summary>The repository identifier - immutable identity carried in the key.</summary>
    [Id(0)]
    public string RepoId { get; init; } = string.Empty;

    /// <summary>Last-writer-wins human-readable display name for the repository.</summary>
    [Id(1)]
    public BoundedRegister DisplayName { get; init; } = new();

    /// <summary>Last-writer-wins default branch name.</summary>
    [Id(2)]
    public BoundedRegister DefaultBranch { get; init; } = new();

    /// <summary>Last-writer-wins last-ingested marker (implementation-defined opaque token or timestamp).</summary>
    [Id(3)]
    public BoundedRegister LastIngested { get; init; } = new();

    /// <summary>Add-wins observed-remove set of free-form tags (UTF-8 encoded elements).</summary>
    [Id(4)]
    public OrSet Tags { get; init; } = new();

    /// <summary>
    /// Last-writer-wins count of files present in the repository as of the last
    /// ingestion. Written as an 8-byte integer register (see
    /// <see cref="RepoContextValues.Lww(long, HybridLogicalClock)"/>); unset on a
    /// repository ingested before this field existed.
    /// </summary>
    [Id(5)]
    public BoundedRegister FileCount { get; init; } = new();

    /// <summary>
    /// Lattice merge of two replicas of the same repository node. Identity is
    /// preserved from <paramref name="left"/> (both sides share the key-derived
    /// <see cref="RepoId"/>); every mutable field is folded through its CRDT
    /// join, so the result is commutative, associative, and idempotent.
    /// </summary>
    /// <param name="left">The first replica. Must not be <see langword="null"/>.</param>
    /// <param name="right">The second replica. Must not be <see langword="null"/>.</param>
    public static RepoNode Merge(RepoNode left, RepoNode right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        return new RepoNode
        {
            RepoId = left.RepoId.Length != 0 ? left.RepoId : right.RepoId,
            DisplayName = BoundedRegister.Merge(left.DisplayName, right.DisplayName),
            DefaultBranch = BoundedRegister.Merge(left.DefaultBranch, right.DefaultBranch),
            LastIngested = BoundedRegister.Merge(left.LastIngested, right.LastIngested),
            FileCount = BoundedRegister.Merge(left.FileCount, right.FileCount),
            Tags = OrSet.Merge(left.Tags, right.Tags),
        };
    }
}
