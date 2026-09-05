namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The result of the <c>repocontext_release_claim</c> tool: whether the presented
/// token still held the claim at the moment of release.
/// </summary>
/// <remarks>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans
/// grain message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record RepoContextReleaseClaimResult
{
    /// <summary>The full repository-context key the claim guarded.</summary>
    public required string Key { get; init; }

    /// <summary>The cluster-wide lock name the claim was taken under.</summary>
    public required string LockName { get; init; }

    /// <summary>
    /// Whether this call released a claim the presented token still held. A release
    /// with a superseded token reports <see langword="false"/> rather than failing:
    /// releasing a lease you no longer hold is harmless, and the underlying lock
    /// treats it as a no-op so the current holder is never disturbed.
    /// </summary>
    public required bool Released { get; init; }

    /// <summary>The fencing token that was presented.</summary>
    public required long FencingToken { get; init; }

    /// <summary>
    /// Why the release did not apply, or <see langword="null"/> when it did. One of
    /// <c>stale</c> (the token had already been superseded) or <c>missing</c> (no
    /// record exists at the key).
    /// </summary>
    public string? Reason { get; init; }
}
