namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// A symbol record stored at the key <c>repo/{repoId}/symbol/{fqName}</c> (see
/// <see cref="RepoContextKeys.Symbol(string, string)"/>). Captures a
/// type/member/function declaration and where it lives.
/// <para>
/// <see cref="RepoId"/> and <see cref="FullyQualifiedName"/> are immutable
/// identity derived from the key; <see cref="Kind"/> is immutable classification
/// captured at ingest. Location and shape scalars (<see cref="FilePath"/>,
/// <see cref="StartLine"/>, <see cref="EndLine"/>, <see cref="Signature"/>,
/// <see cref="Digest"/>) are last-writer-wins registers, and
/// <see cref="Tags"/> / <see cref="References"/> are add-wins observed-remove
/// sets. Merge with <see cref="Merge(SymbolRecord, SymbolRecord)"/>.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(RepoContextTypeAliases.SymbolRecord)]
internal sealed record SymbolRecord
{
    /// <summary>The repository identifier - immutable identity carried in the key.</summary>
    [Id(0)]
    public string RepoId { get; init; } = string.Empty;

    /// <summary>The fully-qualified symbol name - immutable identity carried in the key.</summary>
    [Id(1)]
    public string FullyQualifiedName { get; init; } = string.Empty;

    /// <summary>The structural kind of the symbol - immutable classification captured at ingest.</summary>
    [Id(2)]
    public SymbolKind Kind { get; init; } = SymbolKind.Unspecified;

    /// <summary>Last-writer-wins path of the file that declares the symbol.</summary>
    [Id(3)]
    public BoundedRegister FilePath { get; init; } = new();

    /// <summary>Last-writer-wins 1-based start line of the symbol's span (integer-encoded scalar).</summary>
    [Id(4)]
    public BoundedRegister StartLine { get; init; } = new();

    /// <summary>Last-writer-wins 1-based end line of the symbol's span (integer-encoded scalar).</summary>
    [Id(5)]
    public BoundedRegister EndLine { get; init; } = new();

    /// <summary>Last-writer-wins declaration signature.</summary>
    [Id(6)]
    public BoundedRegister Signature { get; init; } = new();

    /// <summary>Last-writer-wins content digest of the symbol body.</summary>
    [Id(7)]
    public BoundedRegister Digest { get; init; } = new();

    /// <summary>Add-wins observed-remove set of free-form tags (UTF-8 encoded elements).</summary>
    [Id(8)]
    public OrSet Tags { get; init; } = new();

    /// <summary>
    /// Add-wins observed-remove set of fully-qualified names this symbol
    /// references (UTF-8 encoded elements).
    /// </summary>
    [Id(9)]
    public OrSet References { get; init; } = new();

    /// <summary>
    /// Add-wins observed-remove set of repository-relative file paths that declare
    /// this symbol (UTF-8 encoded elements). A single symbol may be declared in
    /// more than one file - C# partial types are the canonical case - so ownership
    /// is a set rather than a scalar. The reconciler removes a file from this set
    /// when the file no longer declares the symbol and prunes the whole record
    /// only once the set becomes empty.
    /// </summary>
    [Id(10)]
    public OrSet DeclaringFiles { get; init; } = new();

    /// <summary>
    /// Lattice merge of two replicas of the same symbol record. Identity and the
    /// immutable <see cref="Kind"/> are preserved from <paramref name="left"/>
    /// (falling back to <paramref name="right"/> only when the left side is
    /// unset); every mutable field is folded through its CRDT join, so the result
    /// is commutative, associative, and idempotent.
    /// </summary>
    /// <param name="left">The first replica. Must not be <see langword="null"/>.</param>
    /// <param name="right">The second replica. Must not be <see langword="null"/>.</param>
    public static SymbolRecord Merge(SymbolRecord left, SymbolRecord right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        return new SymbolRecord
        {
            RepoId = left.RepoId.Length != 0 ? left.RepoId : right.RepoId,
            FullyQualifiedName = left.FullyQualifiedName.Length != 0
                ? left.FullyQualifiedName
                : right.FullyQualifiedName,
            Kind = left.Kind != SymbolKind.Unspecified ? left.Kind : right.Kind,
            FilePath = BoundedRegister.Merge(left.FilePath, right.FilePath),
            StartLine = BoundedRegister.Merge(left.StartLine, right.StartLine),
            EndLine = BoundedRegister.Merge(left.EndLine, right.EndLine),
            Signature = BoundedRegister.Merge(left.Signature, right.Signature),
            Digest = BoundedRegister.Merge(left.Digest, right.Digest),
            Tags = OrSet.Merge(left.Tags, right.Tags),
            References = OrSet.Merge(left.References, right.References),
            DeclaringFiles = OrSet.Merge(left.DeclaringFiles, right.DeclaringFiles),
        };
    }
}
