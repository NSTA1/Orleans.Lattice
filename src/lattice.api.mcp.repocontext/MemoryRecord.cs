namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// An agent-authored memory record stored at the key
/// <c>repo/{repoId}/mem/{topic}/{id}</c> (see
/// <see cref="RepoContextKeys.Memory(string, string, string)"/>). Captures
/// free-form context - a decision, a note, or short-lived working memory - with
/// provenance.
/// <para>
/// <see cref="RepoId"/>, <see cref="Topic"/>, and <see cref="Id"/> are immutable
/// identity derived from the key; <see cref="Kind"/> is immutable classification.
/// Free-form scalars (<see cref="Title"/>, <see cref="Body"/>,
/// <see cref="Author"/>, <see cref="Provenance"/>, <see cref="CreatedAt"/>) are
/// last-writer-wins registers; <see cref="Tags"/> is an add-wins observed-remove
/// set; <see cref="Links"/> is an observed-remove map of relation to a set of
/// target keys; and <see cref="Revisions"/> is a grow-only, content-addressed set
/// of prior payloads that must never be lost. Merge with
/// <see cref="Merge(MemoryRecord, MemoryRecord)"/>.
/// </para>
/// <para>
/// Time-to-live / expiry semantics are intentionally out of scope for this record
/// and are layered on separately.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(RepoContextTypeAliases.MemoryRecord)]
internal sealed record MemoryRecord
{
    /// <summary>The repository identifier - immutable identity carried in the key.</summary>
    [Id(0)]
    public string RepoId { get; init; } = string.Empty;

    /// <summary>The memory topic bucket - immutable identity carried in the key.</summary>
    [Id(1)]
    public string Topic { get; init; } = string.Empty;

    /// <summary>The per-topic record identifier - immutable identity carried in the key.</summary>
    [Id(2)]
    public string Id { get; init; } = string.Empty;

    /// <summary>The kind of memory - immutable classification captured at creation.</summary>
    [Id(3)]
    public MemoryKind Kind { get; init; } = MemoryKind.Unspecified;

    /// <summary>Last-writer-wins short title.</summary>
    [Id(4)]
    public BoundedRegister Title { get; init; } = new();

    /// <summary>Last-writer-wins free-form body text.</summary>
    [Id(5)]
    public BoundedRegister Body { get; init; } = new();

    /// <summary>Last-writer-wins author identity (the agent or session that wrote the record).</summary>
    [Id(6)]
    public BoundedRegister Author { get; init; } = new();

    /// <summary>Last-writer-wins provenance descriptor (where the context came from).</summary>
    [Id(7)]
    public BoundedRegister Provenance { get; init; } = new();

    /// <summary>Last-writer-wins creation timestamp (integer-encoded scalar, e.g. UTC ticks).</summary>
    [Id(8)]
    public BoundedRegister CreatedAt { get; init; } = new();

    /// <summary>Add-wins observed-remove set of free-form tags (UTF-8 encoded elements).</summary>
    [Id(9)]
    public OrSet Tags { get; init; } = new();

    /// <summary>
    /// Observed-remove map from a relation name to an add-wins set of target keys
    /// (e.g. related file, symbol, or memory keys). Concurrent additions under
    /// different relations - or under the same relation - all survive the merge.
    /// </summary>
    [Id(10)]
    public OrMap<string, OrSet> Links { get; init; } = new();

    /// <summary>
    /// Grow-only, content-addressed set of prior immutable payload revisions, so
    /// no revision observed by any replica is ever lost on merge.
    /// </summary>
    [Id(11)]
    public GSet Revisions { get; init; } = new();

    /// <summary>
    /// Lattice merge of two replicas of the same memory record. Identity and the
    /// immutable <see cref="Kind"/> are preserved from <paramref name="left"/>
    /// (falling back to <paramref name="right"/> only when the left side is
    /// unset); every mutable field is folded through its CRDT join, so the result
    /// is commutative, associative, and idempotent.
    /// </summary>
    /// <param name="left">The first replica. Must not be <see langword="null"/>.</param>
    /// <param name="right">The second replica. Must not be <see langword="null"/>.</param>
    public static MemoryRecord Merge(MemoryRecord left, MemoryRecord right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        return new MemoryRecord
        {
            RepoId = left.RepoId.Length != 0 ? left.RepoId : right.RepoId,
            Topic = left.Topic.Length != 0 ? left.Topic : right.Topic,
            Id = left.Id.Length != 0 ? left.Id : right.Id,
            Kind = left.Kind != MemoryKind.Unspecified ? left.Kind : right.Kind,
            Title = BoundedRegister.Merge(left.Title, right.Title),
            Body = BoundedRegister.Merge(left.Body, right.Body),
            Author = BoundedRegister.Merge(left.Author, right.Author),
            Provenance = BoundedRegister.Merge(left.Provenance, right.Provenance),
            CreatedAt = BoundedRegister.Merge(left.CreatedAt, right.CreatedAt),
            Tags = OrSet.Merge(left.Tags, right.Tags),
            Links = OrMap<string, OrSet>.Merge(left.Links, right.Links),
            Revisions = GSet.Merge(left.Revisions, right.Revisions),
        };
    }
}
