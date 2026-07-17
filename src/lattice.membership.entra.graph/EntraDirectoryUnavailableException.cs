namespace Orleans.Lattice.Membership.Entra.Graph;

/// <summary>
/// Signals that a Microsoft Graph directory query could not be served - typically
/// because the app-only token lacks the required <c>User.Read.All</c> /
/// <c>Group.Read.All</c> scopes, or Graph otherwise denied the request. The
/// production <see cref="GraphEntraDirectoryClient"/> translates a Graph error
/// into this exception, and <see cref="EntraGraphIdentityDirectory"/> catches it
/// to degrade cleanly to an empty page / null resolve rather than surfacing an
/// unhandled fault to the caller.
/// </summary>
internal sealed class EntraDirectoryUnavailableException : Exception
{
    /// <summary>
    /// Initializes a new <see cref="EntraDirectoryUnavailableException"/>.
    /// </summary>
    /// <param name="message">A description of the failure.</param>
    /// <param name="innerException">The underlying Graph error, if any.</param>
    public EntraDirectoryUnavailableException(string message, Exception? innerException = null)
        : base(message, innerException)
    {
    }
}
