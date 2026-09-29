namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>
/// The cluster could not be reached, or failed in a way no facade contract
/// describes: the endpoint was unavailable, the call timed out, or the server
/// reported an internal fault. It is what an area sees in place of a raw
/// transport exception, so no area has to know the transport is gRPC.
/// </summary>
/// <remarks>
/// The originating transport exception is kept as
/// <see cref="Exception.InnerException"/> for diagnostics. The message is the
/// server's sanitised status detail, never a credential.
/// </remarks>
internal sealed class ShellTransportException : Exception
{
    /// <summary>Creates the exception.</summary>
    /// <param name="message">The sanitised reason.</param>
    /// <param name="isTransient">Whether retrying the same call may succeed.</param>
    /// <param name="innerException">The originating transport exception.</param>
    public ShellTransportException(string message, bool isTransient, Exception innerException)
        : base(message, innerException)
    {
        IsTransient = isTransient;
    }

    /// <summary>
    /// <see langword="true"/> when retrying the same call may succeed: the endpoint
    /// was unavailable, the call timed out, or it was aborted by a conflict.
    /// </summary>
    public bool IsTransient { get; }
}
