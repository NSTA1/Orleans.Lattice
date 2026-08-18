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
    /// Last-writer-wins set of fully-qualified names of the symbols this file
    /// declares, encoded as a newline-joined, ordered string. The single-writer
    /// indexer owns this projection, so a last-writer-wins register is the correct
    /// and simplest join; the symbol reconciler reads the prior value to compute
    /// which symbols a changed or removed file no longer declares.
    /// </summary>
    [Id(8)]
    public BoundedRegister DeclaredSymbols { get; init; } = new();

    /// <summary>
    /// Last-writer-wins marker recording that this file has been symbol-processed -
    /// its declared symbols were extracted (even when it declares none). It is the
    /// presence signal the background symbol back-fill probes: a supported-language
    /// file whose node predates symbol extraction (or was written by a run that never
    /// reached the symbol phase) carries no marker, so the reconciler re-extracts it
    /// without re-processing files that already have one. Distinct from
    /// <see cref="DeclaredSymbols"/> because a file that genuinely declares nothing
    /// still needs to be recorded as processed, which an empty declared-set register
    /// cannot express.
    /// </summary>
    [Id(9)]
    public BoundedRegister SymbolsProcessed { get; init; } = new();

    /// <summary>
    /// Last-writer-wins marker recording that this file has been content-processed -
    /// its searchable body text was projected into the
    /// <see cref="RepoContextTrees.Content"/> tree (even when the file is empty). It
    /// is the presence signal the background content back-fill probes: a text file
    /// whose node predates the content projection (or was written by a run that never
    /// reached the content phase) carries no marker, so the reconciler projects it
    /// without re-processing files that already have one. Decoupled from
    /// <see cref="SymbolsProcessed"/> because content is projected for every text
    /// file while symbols are extracted only for supported languages, so the two
    /// back-fills cover different file sets.
    /// </summary>
    [Id(10)]
    public BoundedRegister ContentProcessed { get; init; } = new();

    /// <summary>
    /// Last-writer-wins count of BPE tokens in the file's decoded body text, under
    /// the configured tokenizer profile (integer-encoded scalar). It is computed once
    /// in <see cref="RepoContextContentReconciler"/> where the body is already in hand
    /// and stored so budgets and reported counts are read here, not recomputed per
    /// call. This register is additive and migration-safe: a node written before it
    /// existed simply carries the empty default until the content back-fill recomputes
    /// it, exactly like <see cref="ContentProcessed"/>.
    /// </summary>
    [Id(11)]
    public BoundedRegister TokenCount { get; init; } = new();

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
            DeclaredSymbols = BoundedRegister.Merge(left.DeclaredSymbols, right.DeclaredSymbols),
            SymbolsProcessed = BoundedRegister.Merge(left.SymbolsProcessed, right.SymbolsProcessed),
            ContentProcessed = BoundedRegister.Merge(left.ContentProcessed, right.ContentProcessed),
            TokenCount = BoundedRegister.Merge(left.TokenCount, right.TokenCount),
        };
    }
}
