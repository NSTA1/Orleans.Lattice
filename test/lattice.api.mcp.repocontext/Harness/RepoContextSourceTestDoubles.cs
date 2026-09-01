using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Mcp.RepoContext;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

/// <summary>
/// Construction helpers for the repository-context source-strategy seam, so a test
/// can build a real <see cref="RepoContextIndexSourceGate"/> - mounted-only, or with
/// a declared git source - without repeating its collaborator graph.
/// </summary>
internal static class RepoContextSourceTestDoubles
{
    /// <summary>
    /// A gate with no git source configured at all: every repository is mounted, and
    /// the git strategy is unreachable. This is the shipped default posture.
    /// </summary>
    /// <returns>A mounted-only gate.</returns>
    public static RepoContextIndexSourceGate MountedOnlyGate() =>
        Gate(RepoContextGitSourceRegistry.Empty, credentials: null, fetcher: null, role: RepoContextIndexingRole.Hub);

    /// <summary>
    /// A gate over an explicit registry, credential provider, and fetcher.
    /// </summary>
    /// <param name="registry">The configured git sources. Must not be <see langword="null"/>.</param>
    /// <param name="credentials">The credential provider, or <see langword="null"/> for one that resolves none.</param>
    /// <param name="fetcher">The transport, or <see langword="null"/> for one that always throws.</param>
    /// <param name="role">The indexing role the gate enforces.</param>
    /// <returns>The constructed gate.</returns>
    public static RepoContextIndexSourceGate Gate(
        RepoContextGitSourceRegistry registry,
        IRepoContextGitCredentialProvider? credentials = null,
        IRepoContextGitFetcher? fetcher = null,
        RepoContextIndexingRole role = RepoContextIndexingRole.Hub)
    {
        ArgumentNullException.ThrowIfNull(registry);

        var options = new RepoContextIndexingOptions { Role = role };
        var gitSource = new GitRemoteSource(
            registry,
            credentials ?? new RepoContextEnvironmentGitCredentialProvider(
                new Dictionary<string, RepoContextGitCredential>(StringComparer.Ordinal)),
            fetcher ?? new UnreachableGitFetcher(),
            options,
            NullLogger<GitRemoteSource>.Instance);

        return new RepoContextIndexSourceGate(
            registry,
            new MountedWorkspaceSource(),
            gitSource,
            options,
            NullLogger<RepoContextIndexSourceGate>.Instance);
    }

    /// <summary>
    /// A credential provider that resolves a token for exactly the named repositories
    /// and nothing else, so a test can prove per-repository isolation.
    /// </summary>
    /// <param name="repoIds">The repository ids that get a credential.</param>
    /// <returns>The provider.</returns>
    public static IRepoContextGitCredentialProvider CredentialsFor(params string[] repoIds)
    {
        ArgumentNullException.ThrowIfNull(repoIds);

        var map = new Dictionary<string, RepoContextGitCredential>(StringComparer.Ordinal);
        foreach (var repoId in repoIds)
        {
            map[repoId] = RepoContextGitCredential.Token("test-token", username: "x-access-token")!;
        }

        return new RepoContextEnvironmentGitCredentialProvider(map);
    }

    /// <summary>
    /// A fetcher that fails every call. Used where the test asserts the gate stands
    /// down before it would ever reach the transport.
    /// </summary>
    private sealed class UnreachableGitFetcher : IRepoContextGitFetcher
    {
        public RepoContextGitFetchResult Fetch(
            RepoContextGitFetchRequest request, CancellationToken cancellationToken) =>
            throw new RepoContextGitSourceException("The transport must not be reached in this test.");

        public IReadOnlyList<RepoFileEntry> ScanCommit(
            string workTreePath,
            string commitSha,
            IReadOnlyList<string>? includeGlobs,
            IReadOnlyList<string>? excludeGlobs,
            bool excludeBinary,
            CancellationToken cancellationToken) =>
            throw new RepoContextGitSourceException("The transport must not be reached in this test.");
    }
}
