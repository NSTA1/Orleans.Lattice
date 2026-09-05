namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The result of the read-only <c>repocontext_claim_status</c> tool: the claim
/// recorded on a repository-context record, alongside the live status of the lock
/// that grants it.
/// </summary>
/// <remarks>
/// <para>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans
/// grain message, so it carries no Orleans serialization attributes.
/// </para>
/// <para>
/// The lock-derived fields (<see cref="IsHeld"/>, <see cref="LeaseExpiresAtUtc"/>,
/// <see cref="QueueDepth"/>) are advisory and racy by construction - the lock can
/// be granted, renewed, or reclaimed between the read and the caller acting on it.
/// Never gate a write on them; the only trustworthy admission signal is the fencing
/// token from an actual grant, checked at the write path. <see cref="Authoritative"/>
/// is always <see langword="false"/> to make that explicit in the payload itself.
/// </para>
/// </remarks>
public sealed record RepoContextClaimStatusResult
{
    /// <summary>The full repository-context key the status describes.</summary>
    public required string Key { get; init; }

    /// <summary>The cluster-wide lock name claims on this key are taken under.</summary>
    public required string LockName { get; init; }

    /// <summary>
    /// Whether the record carries a live claim: it has been claimed and that claim
    /// has not been released. Derived from the record's durable fencing state, not
    /// from the lock.
    /// </summary>
    public required bool Claimed { get; init; }

    /// <summary>
    /// Whether the underlying lock currently reports a live, unexpired holder.
    /// Advisory only - see the type's remarks.
    /// </summary>
    public required bool IsHeld { get; init; }

    /// <summary>
    /// The record's fencing high-water mark: the highest token any claim on it has
    /// been granted, or <see langword="null"/> when it has never been claimed. A
    /// write presenting a lower token is refused.
    /// </summary>
    public long? FencingToken { get; init; }

    /// <summary>
    /// The highest fencing token whose claim has been released, or
    /// <see langword="null"/> when no claim on this record has been released.
    /// </summary>
    public long? ReleasedFencingToken { get; init; }

    /// <summary>The identity holding the claim that owns the current fence, or <see langword="null"/>.</summary>
    public string? Owner { get; init; }

    /// <summary>The region the claim owning the current fence was taken in, or <see langword="null"/>.</summary>
    public string? Region { get; init; }

    /// <summary>
    /// The lock's own current fencing token. It can run ahead of
    /// <see cref="FencingToken"/> when a grant has been made but its holder has not
    /// yet written to the record.
    /// </summary>
    public required long LockFencingToken { get; init; }

    /// <summary>
    /// When the live lease expires, in round-trip ISO-8601 UTC form, or
    /// <see langword="null"/> when the lock is not held. Advisory only.
    /// </summary>
    public string? LeaseExpiresAtUtc { get; init; }

    /// <summary>The number of callers waiting in the lock's FIFO queue. Advisory only.</summary>
    public required int QueueDepth { get; init; }

    /// <summary>
    /// Always <see langword="false"/>. The status is a racy snapshot and must never
    /// be used to decide whether it is safe to write; only a fencing token from an
    /// actual grant is authoritative. It is computed rather than settable so no
    /// call site can ever project a status that claims otherwise.
    /// </summary>
    public bool Authoritative => false;

    /// <summary>
    /// Whether a record exists at <see cref="Key"/>. A key with no live record
    /// reports <see langword="false"/> so an absent record is distinguishable from
    /// an unclaimed one.
    /// </summary>
    public required bool Exists { get; init; }
}
