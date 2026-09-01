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

    /// <summary>
    /// <see langword="true"/> when the server refused an <em>authenticated</em>
    /// caller for want of a grant (gRPC <c>PermissionDenied</c>), as opposed to
    /// refusing an anonymous one for want of a credential
    /// (<c>Unauthenticated</c>).
    /// </summary>
    /// <remarks>
    /// <see cref="RequiresAuthentication"/> is true for both, because both are
    /// answered by the same reconnect-or-sign-in affordance. They are different
    /// situations to a reader, though: signing in fixes one and cannot fix the
    /// other, so a surface that offers "sign in" to a caller who is already
    /// signed in sends them round a loop. Surfaces that explain a refusal use
    /// this to pick between "you are not signed in" and "you lack the grant".
    /// </remarks>
    public bool IsPermissionDenied { get; init; }
}
