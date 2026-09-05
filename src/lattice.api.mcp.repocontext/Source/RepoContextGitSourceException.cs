namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Raised by the git transport when a fetch, ref resolution, or checkout fails. The
/// message is always secret-redacted before construction, so it is safe to log and
/// safe to surface as a preparation failure reason.
/// <para>
/// It is a control-flow signal inside the host process and never crosses an Orleans
/// wire (the source converts it into a
/// <see cref="RepoContextSourceOutcome.Failed"/> preparation), so it carries no
/// serialization attributes.
/// </para>
/// </summary>
internal sealed class RepoContextGitSourceException : Exception
{
    /// <summary>Creates the exception with a redacted message.</summary>
    /// <param name="message">The already secret-redacted description.</param>
    public RepoContextGitSourceException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception with a redacted message and an inner cause.</summary>
    /// <param name="message">The already secret-redacted description.</param>
    /// <param name="innerException">The underlying transport failure.</param>
    public RepoContextGitSourceException(string message, Exception? innerException)
        : base(message, innerException)
    {
    }
}
