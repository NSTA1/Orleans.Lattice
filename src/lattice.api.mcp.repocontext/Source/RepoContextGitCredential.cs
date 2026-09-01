namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// A resolved credential for one repository's outbound git fetch. Deliberately a
/// class with a redacting <see cref="ToString"/> rather than a record: a record's
/// compiler-generated <c>ToString</c> prints every property, which would put the
/// secret into the first log line or exception message that formats it.
/// <para>
/// An instance is scoped to a single repository - it is never shared between
/// repositories - so a token that authorises one remote can never be presented to
/// another.
/// </para>
/// </summary>
internal sealed class RepoContextGitCredential
{
    /// <summary>The username presented when a token carries no explicit user.</summary>
    internal const string DefaultTokenUsername = "x-access-token";

    private RepoContextGitCredential(string username, string secret, bool isAnonymous)
    {
        Username = username;
        Secret = secret;
        IsAnonymous = isAnonymous;
    }

    /// <summary>
    /// The explicitly unauthenticated credential, used only when a repository is
    /// configured with <see cref="RepoContextGitAuthMode.Anonymous"/>.
    /// </summary>
    public static RepoContextGitCredential Anonymous { get; } =
        new(string.Empty, string.Empty, isAnonymous: true);

    /// <summary>Whether this credential authenticates or is an explicit anonymous fetch.</summary>
    public bool IsAnonymous { get; }

    /// <summary>The username half of the credential; empty when anonymous.</summary>
    public string Username { get; }

    /// <summary>
    /// The secret half of the credential. Read only by the transport when it builds
    /// the fetch's credential callback; never logged, never surfaced through a tool
    /// response, and never included in a failure reason.
    /// </summary>
    public string Secret { get; }

    /// <summary>
    /// Creates a token credential. A blank secret yields <see langword="null"/>
    /// rather than an unusable credential, so an empty environment variable fails
    /// closed exactly like a missing one.
    /// </summary>
    /// <param name="secret">The token, personal access token, or password. May be
    /// <see langword="null"/> or blank, in which case no credential is produced.</param>
    /// <param name="username">The username to present, or <see langword="null"/> to
    /// use <see cref="DefaultTokenUsername"/> (which is what GitHub App installation
    /// tokens and PATs expect over HTTPS).</param>
    /// <returns>The credential, or <see langword="null"/> when the secret is blank.</returns>
    public static RepoContextGitCredential? Token(string? secret, string? username = null)
    {
        if (string.IsNullOrWhiteSpace(secret))
        {
            return null;
        }

        var user = string.IsNullOrWhiteSpace(username) ? DefaultTokenUsername : username.Trim();
        return new RepoContextGitCredential(user, secret, isAnonymous: false);
    }

    /// <summary>
    /// A redacted description safe to log or embed in a failure reason. It never
    /// includes the secret, nor its length, which would narrow a brute-force search.
    /// </summary>
    /// <returns>A constant-shape, secret-free description.</returns>
    public override string ToString() =>
        IsAnonymous ? "anonymous" : "token(username=" + Username + ", secret=redacted)";
}
