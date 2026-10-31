namespace Orleans.Lattice.Explorer.Core.Authentication;

/// <summary>
/// Thrown by <see cref="IExplorerAuthSession.LoginAsync"/> and
/// <see cref="IExplorerAuthSession.LoginWithMethodAsync"/> when the cluster refuses
/// the credential being signed in with (for example a wrong password). The sign-in
/// is not applied: no credential is persisted, the connection is left as it was,
/// and the session stays signed out. A caller the cluster recognises but grants
/// nothing is not refused - it signs in and meets its no-grant states.
/// </summary>
/// <remarks>
/// The message is written for the sign-in dialog and never carries the credential.
/// </remarks>
public sealed class ExplorerSignInRefusedException : InvalidOperationException
{
    /// <summary>The message a refused username and password sign-in fails with.</summary>
    public const string PasswordRefusedMessage = "The cluster did not accept this username and password.";

    /// <summary>The message any other refused sign-in fails with.</summary>
    public const string SignInRefusedMessage = "The cluster did not accept this sign-in.";

    /// <summary>Creates the exception with <see cref="SignInRefusedMessage"/>.</summary>
    public ExplorerSignInRefusedException()
        : base(SignInRefusedMessage)
    {
    }

    /// <summary>Creates the exception with <paramref name="message"/>.</summary>
    /// <param name="message">The message shown to the operator. Must not carry the credential.</param>
    public ExplorerSignInRefusedException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception with <paramref name="message"/> and the fault that caused it.</summary>
    /// <param name="message">The message shown to the operator. Must not carry the credential.</param>
    /// <param name="innerException">The fault that caused the refusal.</param>
    public ExplorerSignInRefusedException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
