namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Thrown by <see cref="RepoContextWorkspaceGuard"/> when a requested repository
/// path resolves outside every configured workspace root. It is the fail-closed
/// verdict of the single ingestion seam: the walk never starts, so no filesystem
/// outside the mounted workspace is read. The tool handlers map it to a caller
/// error (an <c>McpException</c>) rather than letting it surface as an internal
/// fault.
/// </summary>
internal sealed class RepoContextWorkspaceViolationException : Exception
{
    /// <summary>Creates the violation with a caller-facing message.</summary>
    /// <param name="message">The reason the path was rejected.</param>
    public RepoContextWorkspaceViolationException(string message)
        : base(message)
    {
    }
}
