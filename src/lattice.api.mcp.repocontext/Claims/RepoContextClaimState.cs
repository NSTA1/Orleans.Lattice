namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The claim state read off one repository-context memory record: the highest
/// fencing token the record has ever seen, the highest token that has been
/// released, and the identity and region of the grant that owns the current fence.
/// </summary>
/// <param name="FencingToken">The record's fencing high-water mark, or <see langword="null"/> when it has never been claimed.</param>
/// <param name="ReleasedFencingToken">The highest token whose claim was released, or <see langword="null"/> when no claim has been released.</param>
/// <param name="Owner">The agent identity that took the claim owning the current fence, or <see langword="null"/>.</param>
/// <param name="Region">The region the claim owning the current fence was taken in, or <see langword="null"/>.</param>
internal readonly record struct RepoContextClaimState(
    long? FencingToken,
    long? ReleasedFencingToken,
    string? Owner,
    string? Region)
{
    /// <summary>
    /// Whether the record carries a live claim: it has been claimed, and that
    /// claim has not been released. A live claim is what makes an unfenced write
    /// fail closed.
    /// </summary>
    /// <remarks>
    /// Lease expiry is deliberately <em>not</em> consulted here. The record is the
    /// fencing high-water mark, not a second copy of the lease; liveness of the
    /// lease itself belongs to <see cref="ILatticeLockGrain"/>, which reclaims an
    /// expired lease and hands the next waiter a strictly higher token. That higher
    /// token is what supersedes the old holder, so expiry reaches the write path
    /// through the fence rather than through a clock read here.
    /// </remarks>
    public bool IsClaimLive =>
        FencingToken is { } fence && (ReleasedFencingToken is not { } released || released < fence);
}
