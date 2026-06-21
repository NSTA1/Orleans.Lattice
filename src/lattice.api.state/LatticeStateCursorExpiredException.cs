namespace Orleans.Lattice.Api.State;

/// <summary>
/// Thrown when a change-observation subscription is resumed from a
/// <see cref="StateObserveRequest.ContinuationToken"/> that has fallen outside
/// the WAL retention window, so the missed changes can no longer be replayed.
/// The caller must restart the subscription from the live tail (a fresh
/// snapshot read) rather than relying on gap-free resume.
/// </summary>
public sealed class LatticeStateCursorExpiredException : Exception
{
    /// <summary>Initialises the exception with a default message.</summary>
    public LatticeStateCursorExpiredException()
        : base("The change-observation resume cursor has expired (the referenced changes "
            + "have been trimmed from the WAL retention window). Restart the subscription from the live tail.")
    {
    }

    /// <summary>Initialises the exception with a custom <paramref name="message"/>.</summary>
    public LatticeStateCursorExpiredException(string message)
        : base(message)
    {
    }

    /// <summary>Initialises the exception with a custom message and inner exception.</summary>
    public LatticeStateCursorExpiredException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
