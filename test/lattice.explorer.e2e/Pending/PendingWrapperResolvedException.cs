namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Pending;

/// <summary>A <see cref="KnownDefect"/> or <see cref="KnownGap"/> whose issue is resolved: the wrapped step now behaves.</summary>
public sealed class PendingWrapperResolvedException : Exception
{
    /// <summary>Creates the exception.</summary>
    /// <param name="message">What resolved, and what to do.</param>
    /// <param name="inner">The assertion that no longer holds.</param>
    public PendingWrapperResolvedException(string message, Exception inner)
        : base(message, inner)
    {
    }
}
