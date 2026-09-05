namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The shipped credential provider: a per-repository token read from the process
/// environment, isolated by repository identity so one repository's token is never
/// presented to another repository's remote.
/// <para>
/// The token for repository <c>my-repo</c> is read from
/// <c>LATTICE_REPOCONTEXT_GIT_MY_REPO_TOKEN</c>, with an optional
/// <c>LATTICE_REPOCONTEXT_GIT_MY_REPO_USERNAME</c>. There is deliberately no
/// un-suffixed shared token variable: a single ambient token would be presented to
/// every configured remote, which is the isolation failure this seam exists to
/// prevent.
/// </para>
/// <para>
/// The map is snapshotted once at construction, so a token rotated in the
/// environment takes effect at the next host start. A rotating provider (for
/// example a GitHub App installation token) implements
/// <see cref="IRepoContextGitCredentialProvider"/> directly and re-mints per call.
/// </para>
/// </summary>
internal sealed class RepoContextEnvironmentGitCredentialProvider : IRepoContextGitCredentialProvider
{
    /// <summary>The per-repository setting holding the token.</summary>
    internal const string TokenSetting = "TOKEN";

    /// <summary>The per-repository setting holding the optional username.</summary>
    internal const string UsernameSetting = "USERNAME";

    private readonly Dictionary<string, RepoContextGitCredential> _byRepoId;

    /// <summary>
    /// Creates a provider over an explicit per-repository credential map. Used by
    /// tests and by a host that supplies credentials in code.
    /// </summary>
    /// <param name="credentials">The per-repository credentials, keyed by repository
    /// id. Must not be <see langword="null"/>.</param>
    public RepoContextEnvironmentGitCredentialProvider(
        IReadOnlyDictionary<string, RepoContextGitCredential> credentials)
    {
        ArgumentNullException.ThrowIfNull(credentials);
        _byRepoId = new Dictionary<string, RepoContextGitCredential>(credentials, StringComparer.Ordinal);
    }

    /// <summary>
    /// Reads a per-repository token for every repository in
    /// <paramref name="registry"/>. A repository with no token is simply absent from
    /// the map, so resolution fails closed for it.
    /// </summary>
    /// <param name="registry">The configured git sources. Must not be
    /// <see langword="null"/>.</param>
    /// <returns>A provider holding one credential per repository that has a token.</returns>
    public static RepoContextEnvironmentGitCredentialProvider FromEnvironment(
        RepoContextGitSourceRegistry registry)
    {
        ArgumentNullException.ThrowIfNull(registry);

        var credentials = new Dictionary<string, RepoContextGitCredential>(StringComparer.Ordinal);
        foreach (var source in registry.Sources)
        {
            var token = Environment.GetEnvironmentVariable(
                RepoContextGitSourceRegistry.VariableName(source.RepoId, TokenSetting));
            var username = Environment.GetEnvironmentVariable(
                RepoContextGitSourceRegistry.VariableName(source.RepoId, UsernameSetting));

            var credential = RepoContextGitCredential.Token(token, username);
            if (credential is not null)
            {
                credentials[source.RepoId] = credential;
            }
        }

        return new RepoContextEnvironmentGitCredentialProvider(credentials);
    }

    /// <inheritdoc />
    public ValueTask<RepoContextGitCredential?> ResolveAsync(
        RepoContextGitSourceOptions source,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(source);
        cancellationToken.ThrowIfCancellationRequested();

        // An anonymous fetch is a deliberate configuration choice, never a fallback
        // for a missing token: a repository configured for token auth with no token
        // resolves to null and does not index.
        if (source.AuthMode == RepoContextGitAuthMode.Anonymous)
        {
            return ValueTask.FromResult<RepoContextGitCredential?>(RepoContextGitCredential.Anonymous);
        }

        return ValueTask.FromResult(_byRepoId.GetValueOrDefault(source.RepoId));
    }
}
