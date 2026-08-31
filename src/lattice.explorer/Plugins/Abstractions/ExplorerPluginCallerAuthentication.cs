namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// What a probe learned about whether the caller presented a credential.
/// <para>
/// A gate is often unable to tell an anonymous refusal from an authenticated
/// one: a server answers <c>PermissionDenied</c> to both. So a gate reports what
/// it actually observed, and <see cref="Unknown"/> - the default - defers to the
/// shell's own sign-in state rather than guessing. Guessing is exactly how an
/// anonymous visitor came to be told that a surface "is not available for your
/// account".
/// </para>
/// </summary>
public enum ExplorerPluginCallerAuthentication
{
    /// <summary>
    /// The probe learned nothing about the credential, so the shell's sign-in
    /// state decides. The default.
    /// </summary>
    Unknown = 0,

    /// <summary>
    /// The probe established that no accepted credential was presented - for
    /// example the transport answered <c>Unauthenticated</c>. A refusal is then
    /// recoverable by signing in, never a permanent denial.
    /// </summary>
    Anonymous = 1,

    /// <summary>
    /// The probe established that an accepted credential was presented, so a
    /// refusal is a genuine authorization denial and signing in again would not
    /// help.
    /// </summary>
    Authenticated = 2,
}
