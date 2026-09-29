using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.UI.Areas.Access;

/// <summary>
/// A failed auth-facade call, classified so a page can put it where it belongs:
/// a directory validation failure or an app-owned rule id beside the field that
/// caused it, a denial as "not permitted", and anything else as one sentence.
/// </summary>
/// <remarks>
/// A typed exception is recognised directly. Over gRPC the auth binding sends
/// every <see cref="ArgumentException"/> as <c>InvalidArgument</c> with its
/// message, which the transport surfaces as a plain <see cref="ArgumentException"/>,
/// so the two server-authored messages are also recognised by their fixed opening.
/// </remarks>
/// <param name="Kind">What went wrong.</param>
/// <param name="Message">The sentence to show.</param>
internal sealed record AccessFailure(AccessFailureKind Kind, string Message)
{
    private const string DirectoryValidationOpening = "Directory validation failed:";
    private const string AppOwnedFragment = "is owned by an installed app";

    /// <summary>The sentence shown when the caller may not administer access.</summary>
    public const string NotPermittedMessage = "You are not permitted to administer access on this cluster. Ask a cluster administrator for the Admin grant on access administration.";

    /// <summary>
    /// Classifies <paramref name="exception"/>, or returns <see langword="null"/> for
    /// a cancellation, which is never shown.
    /// </summary>
    /// <param name="exception">The fault.</param>
    public static AccessFailure? From(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        return exception switch
        {
            OperationCanceledException => null,
            LatticeAuthorizationDeniedException => new(AccessFailureKind.Denied, NotPermittedMessage),
            LatticeDirectoryValidationException directory => new(AccessFailureKind.DirectoryValidation, directory.Message),
            LatticeAppOwnedRuleException owned => new(AccessFailureKind.AppOwned, owned.Message),
            ArgumentException argument when argument.Message.StartsWith(DirectoryValidationOpening, StringComparison.Ordinal) =>
                new(AccessFailureKind.DirectoryValidation, argument.Message),
            ArgumentException argument when argument.Message.Contains(AppOwnedFragment, StringComparison.Ordinal) =>
                new(AccessFailureKind.AppOwned, argument.Message),
            ArgumentException argument => new(AccessFailureKind.Invalid, argument.Message),
            KeyNotFoundException => new(AccessFailureKind.NotFound, "It no longer exists."),
            NotSupportedException => new(AccessFailureKind.Unavailable, "This cluster does not serve access administration."),
            InvalidOperationException => new(AccessFailureKind.Unavailable, "The cluster could not be reached. Check the connection and try again."),
            _ => new(AccessFailureKind.Unavailable, "The cluster did not answer. Try again."),
        };
    }
}
