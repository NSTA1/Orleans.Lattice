namespace Orleans.Lattice.Explorer.Entra.Web;

/// <summary>
/// Thrown by an <see cref="IExplorerWebTokenAcquirer"/> when a token can no
/// longer be acquired silently for the current browser session and the user must
/// complete an interactive sign-in again (for example the session cookie or
/// refresh material expired, or consent was withdrawn). The auth method treats
/// this as a signal to re-challenge rather than a hard failure.
/// </summary>
public sealed class ExplorerWebReauthRequiredException : Exception
{
    /// <summary>Creates the exception with a default message.</summary>
    public ExplorerWebReauthRequiredException()
        : base("The browser session must complete an interactive sign-in again before a token can be acquired.")
    {
    }

    /// <summary>Creates the exception with a custom <paramref name="message"/>.</summary>
    /// <param name="message">The message.</param>
    public ExplorerWebReauthRequiredException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception with a <paramref name="message"/> and <paramref name="innerException"/>.</summary>
    /// <param name="message">The message.</param>
    /// <param name="innerException">The underlying cause.</param>
    public ExplorerWebReauthRequiredException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
