namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The pluggable, fail-closed seam that resolves the credential for one
/// repository's git fetch. An implementation returns <see langword="null"/> when it
/// cannot produce a credential; the caller must then stand down rather than attempt
/// an unauthenticated fetch, so a missing or revoked token can never silently
/// downgrade a private remote to an anonymous probe.
/// <para>
/// The shipped implementation is
/// <see cref="RepoContextEnvironmentGitCredentialProvider"/>, which reads a
/// per-repository token from the environment. A GitHub App installation-token
/// provider (minting and rotating an installation token from an app id and private
/// key) is the intended extension point: register a different implementation before
/// <c>AddRepoContextTools</c> and nothing else changes.
/// </para>
/// </summary>
internal interface IRepoContextGitCredentialProvider
{
    /// <summary>
    /// Resolves the credential for <paramref name="source"/>, or
    /// <see langword="null"/> when none is available.
    /// </summary>
    /// <param name="source">The git source whose credential is needed. Must not be
    /// <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the resolution, which may involve a
    /// token exchange in a non-default implementation.</param>
    /// <returns>The credential, or <see langword="null"/> to fail closed.</returns>
    ValueTask<RepoContextGitCredential?> ResolveAsync(
        RepoContextGitSourceOptions source,
        CancellationToken cancellationToken);
}
