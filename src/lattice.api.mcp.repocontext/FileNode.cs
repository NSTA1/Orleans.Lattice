namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// A source-file structural node stored at the key
/// <c>repo/{repoId}/file/{path}</c> (see
/// <see cref="RepoContextKeys.File(string, string)"/>). The <c>/file/</c> prefix
/// keeps a whole directory subtree contiguous under an ordered range scan.
/// <para>
/// <see cref="RepoId"/> and <see cref="Path"/> are immutable identity derived
/// from the key. Scalar metadata (<see cref="Digest"/>, <see cref="Language"/>,
/// <see cref="SizeBytes"/>, <see cref="LastIngested"/>) uses last-writer-wins
/// registers; <see cref="Tags"/> is an add-wins observed-remove set; and
/// <see cref="ContentBlobs"/> is a grow-only, content-addressed set for immutable
/// payloads that must never be lost. Merge with
/// <see cref="Merge(FileNode, FileNode)"/>.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(RepoContextTypeAliases.FileNode)]
internal sealed record FileNode
{
    /// <summary>The repository identifier - immutable identity carried in the key.</summary>
    [Id(0)]
    public string RepoId { get; init; } = string.Empty;

    /// <summary>The file path relative to the repository root - immutable identity carried in the key.</summary>
    [Id(1)]
    public string Path { get; init; } = string.Empty;

    /// <summary>Last-writer-wins content digest (e.g. a hex content hash).</summary>
    [Id(2)]
    public BoundedRegister Digest { get; init; } = new();

    /// <summary>Last-writer-wins detected source language.</summary>
    [Id(3)]
    public BoundedRegister Language { get; init; } = new();

    /// <summary>Last-writer-wins file size in bytes (integer-encoded scalar).</summary>
    [Id(4)]
    public BoundedRegister SizeBytes { get; init; } = new();

    /// <summary>Last-writer-wins last-ingested marker.</summary>
    [Id(5)]
    public BoundedRegister LastIngested { get; init; } = new();

    /// <summary>Add-wins observed-remove set of free-form tags (UTF-8 encoded elements).</summary>
    [Id(6)]
    public OrSet Tags { get; init; } = new();

    /// <summary>
    /// Grow-only, content-addressed set of immutable payload blobs associated
    /// with the file (e.g. captured snippets). A grow-only set is used so a
    /// payload observed by any replica survives every merge.
    /// </summary>
    [Id(7)]
    public GSet ContentBlobs { get; init; } = new();

    /// <summary>
    /// Lattice merge of two replicas of the same file node. Identity is preserved
    /// from <paramref name="left"/>; every mutable field is folded through its
    /// CRDT join, so the result is commutative, associative, and idempotent.
    /// </summary>
    /// <param name="left">The first replica. Must not be <see langword="null"/>.</param>
    /// <param name="right">The second replica. Must not be <see langword="null"/>.</param>
    public static FileNode Merge(FileNode left, FileNode right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        return new FileNode
        {
            RepoId = left.RepoId.Length != 0 ? left.RepoId : right.RepoId,
            Path = left.Path.Length != 0 ? left.Path : right.Path,
            Digest = BoundedRegister.Merge(left.Digest, right.Digest),
            Language = BoundedRegister.Merge(left.Language, right.Language),
            SizeBytes = BoundedRegister.Merge(left.SizeBytes, right.SizeBytes),
            LastIngested = BoundedRegister.Merge(left.LastIngested, right.LastIngested),
            Tags = OrSet.Merge(left.Tags, right.Tags),
            ContentBlobs = GSet.Merge(left.ContentBlobs, right.ContentBlobs),
        };
    }
}
