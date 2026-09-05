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
/// target keys; <see cref="LinkDigests"/> captures the content digest each
/// structural link target carried when the edge was written, so a later read can
/// flag a link whose target has since drifted; and <see cref="Revisions"/> is a
/// grow-only, content-addressed set of prior payloads that must never be lost.
/// Merge with <see cref="Merge(MemoryRecord, MemoryRecord)"/>.
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
    /// Last-writer-wins map from a structural link target key (a file or symbol)
    /// to the content digest that target carried when the edge was written. On a
    /// per-key read the captured digest is compared against the target's current
    /// digest so a linked file or symbol that has since changed can be surfaced as
    /// stale, without mutating the link itself. Only structural targets carry a
    /// digest; memory-to-memory edges are not tracked here.
    /// </summary>
    [Id(12)]
    public OrMap<string, BoundedRegister> LinkDigests { get; init; } = new();

    /// <summary>
    /// The record's fencing high-water mark: the highest
    /// <see cref="LockToken.FencingToken"/> any claim on this record has been
    /// granted, encoded by <see cref="RepoContextClaimFence.Encode"/> as both the
    /// register's value and its total-order key. Because
    /// <see cref="BoundedRegister"/> is a monotone max-register, that encoding makes
    /// the fence a lattice maximum over tokens: a lower token can never displace a
    /// higher one, through a direct write or a concurrent merge. The write path
    /// refuses any write presenting a token below it.
    /// </summary>
    [Id(13)]
    public BoundedRegister ClaimFence { get; init; } = new();

    /// <summary>
    /// The identity that took the claim owning <see cref="ClaimFence"/>, written
    /// under the same order key so the owner always describes the current fence
    /// rather than drifting independently.
    /// </summary>
    [Id(14)]
    public BoundedRegister ClaimOwner { get; init; } = new();

    /// <summary>
    /// The region the claim owning <see cref="ClaimFence"/> was taken in, written
    /// under the same order key. Claims are cluster-scoped, so a write served from
    /// a different region than the recorded one fails closed rather than racing a
    /// claim it cannot observe.
    /// </summary>
    [Id(15)]
    public BoundedRegister ClaimRegion { get; init; } = new();

    /// <summary>
    /// The highest fencing token whose claim has been released. A claim is live
    /// while <see cref="ClaimFence"/> exceeds this mark; once released, the record
    /// admits unfenced writes again, so releasing a claim returns the record to its
    /// pre-claim behaviour without ever lowering the fence.
    /// </summary>
    [Id(16)]
    public BoundedRegister ClaimReleasedFence { get; init; } = new();

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
            LinkDigests = OrMap<string, BoundedRegister>.Merge(left.LinkDigests, right.LinkDigests),
            ClaimFence = BoundedRegister.Merge(left.ClaimFence, right.ClaimFence),
            ClaimOwner = BoundedRegister.Merge(left.ClaimOwner, right.ClaimOwner),
            ClaimRegion = BoundedRegister.Merge(left.ClaimRegion, right.ClaimRegion),
            ClaimReleasedFence = BoundedRegister.Merge(left.ClaimReleasedFence, right.ClaimReleasedFence),
        };
    }
}
