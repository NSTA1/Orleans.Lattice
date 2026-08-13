namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The membership record for a named vector collection, held in the dedicated
/// <see cref="RepoContextTrees.VectorMembership"/> tree at the key
/// <c>repo/{repoId}/vmem/{collection}</c> (see
/// <see cref="RepoContextKeys.VectorMembership(string, string)"/>). It tracks
/// which vectors are currently live members of the collection, so a derived index
/// can be rebuilt from the exact set the store of record considers authoritative.
/// <para>
/// <b>Add-wins observed-remove.</b> <see cref="Members"/> is an <see cref="OrSet"/>
/// so a vector can be added and later forgotten while concurrent writers converge
/// without loss - a concurrent re-add wins over a remove. Enumerating this set is
/// the single, clean source for rebuilding the discardable derived projection
/// (the in-box kNN scan or any external ANN index); the projection is never an
/// authoritative second copy of membership.
/// </para>
/// <para>
/// <see cref="RepoId"/> and <see cref="Collection"/> are immutable identity
/// derived from the key. Merge with
/// <see cref="Merge(VectorMembershipRecord, VectorMembershipRecord)"/>.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(RepoContextTypeAliases.VectorMembershipRecord)]
internal sealed record VectorMembershipRecord
{
    /// <summary>The repository identifier - immutable identity carried in the key.</summary>
    [Id(0)]
    public string RepoId { get; init; } = string.Empty;

    /// <summary>The vector collection name - immutable identity carried in the key.</summary>
    [Id(1)]
    public string Collection { get; init; } = string.Empty;

    /// <summary>
    /// Add-wins observed-remove set of member vector identifiers (UTF-8 encoded
    /// elements), each addressing a <see cref="VectorMetadataRecord"/> under the
    /// same repository.
    /// </summary>
    [Id(2)]
    public OrSet Members { get; init; } = new();

    /// <summary>
    /// Lattice merge of two replicas of the same membership record. Identity is
    /// preserved from <paramref name="left"/> (falling back to
    /// <paramref name="right"/> only when the left side is unset);
    /// <see cref="Members"/> is folded through its observed-remove join, so the
    /// result is commutative, associative, and idempotent.
    /// </summary>
    /// <param name="left">The first replica. Must not be <see langword="null"/>.</param>
    /// <param name="right">The second replica. Must not be <see langword="null"/>.</param>
    public static VectorMembershipRecord Merge(VectorMembershipRecord left, VectorMembershipRecord right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        return new VectorMembershipRecord
        {
            RepoId = left.RepoId.Length != 0 ? left.RepoId : right.RepoId,
            Collection = left.Collection.Length != 0 ? left.Collection : right.Collection,
            Members = OrSet.Merge(left.Members, right.Members),
        };
    }
}
