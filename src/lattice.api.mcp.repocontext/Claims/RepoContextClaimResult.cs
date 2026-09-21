namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The result of the <c>repocontext_claim</c> and <c>repocontext_renew_claim</c>
/// tools: whether the caller now holds the record, and - when it does - the fencing
/// token it must present on every subsequent write.
/// </summary>
/// <remarks>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans
/// grain message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record RepoContextClaimResult
{
    /// <summary>The full repository-context key the claim guards.</summary>
    public required string Key { get; init; }

    /// <summary>
    /// The cluster-wide lock name the claim was taken under, derived from
    /// <see cref="Key"/>. Reported so an operator can correlate a claim with the
    /// underlying distributed lock.
    /// </summary>
    public required string LockName { get; init; }

    /// <summary>
    /// Whether the claim was granted. A refusal under contention is reported here
    /// as <see langword="false"/> with a <see cref="Reason"/>, not raised as an
    /// error: losing a race is an ordinary outcome an agent must handle, not a
    /// fault.
    /// </summary>
    public required bool Granted { get; init; }

    /// <summary>
    /// The monotonically increasing fencing token identifying this grant, or
    /// <see langword="null"/> when the claim was not granted. Present it as
    /// <c>fencingToken</c> on every <c>repocontext_remember</c> and
    /// <c>repocontext_update</c> write against <see cref="Key"/>; a write bearing a
    /// token that a later claim has superseded is refused.
    /// </summary>
    public long? FencingToken { get; init; }

    /// <summary>The identity that holds the claim, or <see langword="null"/> when it was not granted.</summary>
    public string? Owner { get; init; }

    /// <summary>The region the claim was taken in, or <see langword="null"/> when it was not granted.</summary>
    public string? Region { get; init; }

    /// <summary>
    /// When the lease expires if it is neither renewed nor released, in round-trip
    /// ISO-8601 UTC form, or <see langword="null"/> when the claim was not granted.
    /// After expiry the lock reclaims the lease and grants the next waiter a
    /// strictly higher token, which fences this holder out.
    /// </summary>
    public string? LeaseExpiresAtUtc { get; init; }

    /// <summary>
    /// The granted lease length in seconds, or <see langword="null"/> when the claim
    /// was not granted. This is the length actually granted, which may be shorter
    /// than the length requested: the lock clamps every lease to the configured
    /// maximum.
    /// </summary>
    public double? LeaseSeconds { get; init; }

    /// <summary>
    /// When the lease that a renew replaced was due to expire, in round-trip
    /// ISO-8601 UTC form. Always <see langword="null"/> on a claim, on a refusal,
    /// and whenever no prior lease could be observed. Reported alongside
    /// <see cref="LeaseShortened"/> so a caller can see by how much a renew moved
    /// its own deadline, not merely that it moved.
    /// </summary>
    public string? PreviousLeaseExpiresAtUtc { get; init; }

    /// <summary>
    /// Whether this renew <b>shortened</b> the lease it replaced: <see langword="true"/>
    /// when the new expiry is earlier than the previous one, <see langword="false"/>
    /// when it is not, and <see langword="null"/> when the question does not apply
    /// (a claim, or a refusal) or could not be answered because no prior lease was
    /// observed.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A renew that shortens a lease is a successful renew, so it is reported here
    /// rather than refused. It exists because the alternative - reporting it as a
    /// plain <c>granted: true</c> - is the shape that lets the damage run
    /// undetected. The lease length defers to the cluster default when
    /// <c>leaseSeconds</c> is omitted, and that default is deliberately short, so a
    /// caller holding a long explicit lease that renews without naming one cuts its
    /// own deadline dramatically and is told only that the renew succeeded. The
    /// failure then surfaces one step later, on the *next* renew, as
    /// <see cref="Granted"/> <see langword="false"/> with reason <c>superseded</c> -
    /// at a call site that did nothing wrong.
    /// </para>
    /// <para>
    /// <see langword="null"/> deliberately means <i>unknown</i> and never <i>fine</i>.
    /// A renew whose prior lease could not be read reports <see langword="null"/>
    /// rather than <see langword="false"/>, so an absent observation is not
    /// presented as a positive assurance that nothing shrank.
    /// </para>
    /// </remarks>
    public bool? LeaseShortened { get; init; }

    /// <summary>
    /// Why the claim was not granted, or <see langword="null"/> when it was. One of
    /// <c>contended</c> (another agent holds it), <c>timeout</c> (the bounded wait
    /// elapsed), or <c>missing</c> (no record exists at the key).
    /// </summary>
    public string? Reason { get; init; }
}
