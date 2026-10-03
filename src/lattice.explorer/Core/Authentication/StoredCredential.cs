namespace Orleans.Lattice.Explorer.Core.Authentication;

/// <summary>
/// A username/password credential the explorer uses to authenticate to the
/// state API. This is local application state, not a wire type: it is never
/// persisted in the plaintext config store and is held by a per-user, OS-backed
/// or server-side encrypted <see cref="ICredentialStore"/>.
/// </summary>
/// <param name="Username">The credential username.</param>
/// <param name="Password">The credential password.</param>
public sealed record StoredCredential(string Username, string Password)
{
    /// <summary>
    /// A description that never carries the password. Deliberately overrides the
    /// compiler-generated record <see cref="object.ToString"/>, which prints
    /// every property and would therefore put the plaintext password into the
    /// first log line, exception message or diagnostic dump that formats this
    /// credential. The redaction is a fixed token rather than a run of stars
    /// sized to the secret, so it does not disclose the password's length
    /// either.
    /// </summary>
    /// <returns>The redacted description.</returns>
    public override string ToString() => $"StoredCredential {{ Username = {Username}, Password = [redacted] }}";
}
