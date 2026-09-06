namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The admission decision the fencing check reaches for one write against one
/// repository-context record. Only <see cref="Accepted"/> lets the write proceed;
/// every other value is a fail-closed refusal the write path turns into a
/// <see cref="RepoContextClaimConflictException"/>.
/// </summary>
/// <remarks>
/// The verdict is computed by <see cref="RepoContextClaimFence.Evaluate"/>, a pure
/// function of the claim state already read off the record and the token the
/// caller presented. Keeping it pure is deliberate: the decision is the
/// load-bearing safety property of the claim surface, so it is unit-testable
/// without a silo, a lock grain, or a clock.
/// </remarks>
internal enum RepoContextFenceVerdict
{
    /// <summary>The write may proceed.</summary>
    Accepted,

    /// <summary>
    /// The caller presented a fencing token lower than the highest the record has
    /// seen: its claim was superseded by a later one and it is writing under a
    /// lease it no longer holds.
    /// </summary>
    StaleToken,

    /// <summary>
    /// The record carries a live claim and the caller presented no fencing token
    /// at all. Accepting would make the claim decoration, so it is refused.
    /// </summary>
    ClaimRequired,

    /// <summary>
    /// The caller's token matches the record's fence, but that claim has already
    /// been released. A released holder must re-claim before writing again.
    /// </summary>
    ClaimReleased,

    /// <summary>
    /// The live claim was taken in a different region from the one serving this
    /// write. The underlying lock is cluster-scoped, so the claim is not
    /// observable here and the write fails closed rather than racing it.
    /// </summary>
    ForeignRegion,
}
