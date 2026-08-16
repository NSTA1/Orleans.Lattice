namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The per-file searchable-content projection record, held in the dedicated
/// <see cref="RepoContextTrees.Content"/> tree at the key
/// <c>repo/{repoId}/content/{path}</c> (see
/// <see cref="RepoContextKeys.Content(string, string)"/>). It carries the bounded
/// UTF-8 body text of one text file so the keyword/degraded search path can rank
/// over file <b>content</b> rather than filenames and symbol names alone.
/// <para>
/// <b>Rebuildable projection, not store-of-record.</b> The record is derived from
/// the file bytes during the structural reconcile - decoupled from the embedding
/// provider so it improves the no-embedder path - and is discarded and re-derived
/// on a rebuild. It is retired when its file is deleted. The stored text is capped
/// at <see cref="MaxContentChars"/> so a single huge generated file cannot grow the
/// store without bound.
/// </para>
/// <para>
/// <see cref="RepoId"/> and <see cref="Path"/> are immutable identity carried in
/// the key; <see cref="Text"/> is a last-writer-wins register so concurrent
/// replicas converge on the newest body. Merge with
/// <see cref="Merge(ContentRecord, ContentRecord)"/>.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(RepoContextTypeAliases.ContentRecord)]
internal sealed record ContentRecord
{
    /// <summary>
    /// The maximum number of characters of body text stored per file. A file whose
    /// text exceeds this bound is truncated to the first <see cref="MaxContentChars"/>
    /// characters before it is stored, so the projection stays bounded on a huge
    /// generated file. Mirrors the embedding ingestor's per-file character cap.
    /// </summary>
    internal const int MaxContentChars = 64 * 1024;

    /// <summary>The repository identifier - immutable identity carried in the key.</summary>
    [Id(0)]
    public string RepoId { get; init; } = string.Empty;

    /// <summary>The repository-relative file path - immutable identity carried in the key.</summary>
    [Id(1)]
    public string Path { get; init; } = string.Empty;

    /// <summary>
    /// Last-writer-wins register holding the file's bounded UTF-8 body text (capped
    /// at <see cref="MaxContentChars"/> characters). Read it with
    /// <see cref="RepoContextValues.ReadString(BoundedRegister)"/>.
    /// </summary>
    [Id(2)]
    public BoundedRegister Text { get; init; } = new();

    /// <summary>
    /// Creates a content record carrying <paramref name="text"/> under the given
    /// identity, authored at <paramref name="clock"/>. The text is truncated to
    /// <see cref="MaxContentChars"/> characters when it is longer.
    /// </summary>
    /// <param name="repoId">The repository identifier. Must not be <see langword="null"/>.</param>
    /// <param name="path">The repository-relative file path. Must not be <see langword="null"/>.</param>
    /// <param name="text">The file's body text. Must not be <see langword="null"/>.</param>
    /// <param name="clock">The authoring hybrid logical clock.</param>
    /// <exception cref="ArgumentNullException">Any reference argument is null.</exception>
    public static ContentRecord Create(string repoId, string path, string text, HybridLogicalClock clock)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(text);

        var bounded = text.Length > MaxContentChars ? text[..MaxContentChars] : text;
        return new ContentRecord
        {
            RepoId = repoId,
            Path = path,
            Text = RepoContextValues.Lww(bounded, clock),
        };
    }

    /// <summary>
    /// Lattice merge of two replicas of the same content record. Identity is
    /// preserved from <paramref name="left"/> (falling back to
    /// <paramref name="right"/> only when the left side is unset);
    /// <see cref="Text"/> is folded through its last-writer-wins join, so the result
    /// is commutative, associative, and idempotent.
    /// </summary>
    /// <param name="left">The first replica. Must not be <see langword="null"/>.</param>
    /// <param name="right">The second replica. Must not be <see langword="null"/>.</param>
    public static ContentRecord Merge(ContentRecord left, ContentRecord right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        return new ContentRecord
        {
            RepoId = left.RepoId.Length != 0 ? left.RepoId : right.RepoId,
            Path = left.Path.Length != 0 ? left.Path : right.Path,
            Text = BoundedRegister.Merge(left.Text, right.Text),
        };
    }
}
