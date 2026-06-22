namespace Orleans.Lattice.Explorer.Core.Connection;

/// <summary>
/// Thrown when a state-API call fails in a way the UI should surface as a
/// readable error rather than a raw gRPC fault. The <see cref="Exception.Message"/>
/// is safe to show to the user; the inner exception carries the original fault.
/// </summary>
public sealed class LatticeStateApiException : Exception
{
    /// <summary>Initializes a new instance with a user-facing message.</summary>
    public LatticeStateApiException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance with a user-facing message and the underlying fault.</summary>
    public LatticeStateApiException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>
    /// <see langword="true"/> when the failure was transient (the endpoint may
    /// recover) rather than a permanent fault such as an authentication error.
    /// </summary>
    public bool IsTransient { get; init; }

    /// <summary>
    /// <see langword="true"/> when the failure was an authentication /
    /// authorization rejection (gRPC <c>Unauthenticated</c> or
    /// <c>PermissionDenied</c>), so the UI can offer a "Sign in" action rather
    /// than (or alongside) a plain reconnect.
    /// </summary>
    public bool RequiresAuthentication { get; init; }
}
