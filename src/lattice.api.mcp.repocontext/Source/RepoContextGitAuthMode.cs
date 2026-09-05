namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// How a git-sourced repository authenticates its outbound fetch. Every mode is an
/// explicit opt-in: there is no "try anonymous if the token is missing" fallback,
/// because that would silently downgrade a private remote to an unauthenticated
/// probe.
/// </summary>
internal enum RepoContextGitAuthMode
{
    /// <summary>
    /// The default. A per-repository read-only token (a GitHub App installation
    /// token, a personal access token, or any username/password pair the transport
    /// accepts) must be resolvable, or the repository does not index.
    /// </summary>
    Token = 0,

    /// <summary>
    /// Explicitly unauthenticated: the remote is public or local (a
    /// <c>file://</c> path), so the transport's default credentials are used. This
    /// must be configured deliberately; it is never inferred from a missing token.
    /// </summary>
    Anonymous = 1,
}
