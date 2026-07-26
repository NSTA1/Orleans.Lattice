using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.ReferenceArchitecture.Explorer;

/// <summary>
/// Development-only Explorer sign-in method for the local compose harness. When
/// Entra is disabled the console has no identity provider, so a stock anonymous
/// connection is fail-closed by the silo's state-visibility filter: the tree
/// catalog comes back empty and the Access area is denied, even though MCP - which
/// forwards a trusted bootstrap-admin bearer token - sees everything. This method
/// closes that gap symmetrically: it replaces the built-in Basic provider and
/// signs the console in by forwarding <c>authorization: Bearer &lt;username&gt;</c>,
/// exactly the credential the silo's <c>DevBypassCredentialAuthenticator</c>
/// trusts (and the MCP head already forwards), so the console is served as the
/// configured bootstrap administrator.
/// </summary>
/// <remarks>
/// It reuses the <see cref="ExplorerAuthSchemes.Basic"/> scheme so the console's
/// launcher-friendly sign-in seed (<c>LATTICE_EXPLORER_USERNAME</c> /
/// <c>LATTICE_EXPLORER_PASSWORD</c>) auto-applies it on first load with no dialog.
/// The username is the bootstrap-admin subject id; the password is ignored (the
/// seed merely requires one to be present). The host registers this method ONLY
/// when Entra is disabled, so it can never coexist with, or weaken, a real
/// deployment's Entra sign-in.
/// </remarks>
internal sealed class DevBypassExplorerAuthMethod : IExplorerAuthMethod
{
    /// <inheritdoc />
    public string SchemeId => ExplorerAuthSchemes.Basic;

    /// <inheritdoc />
    public bool CanHandle(string advertisedScheme)
        => string.IsNullOrEmpty(advertisedScheme)
            || string.Equals(advertisedScheme, SchemeId, StringComparison.OrdinalIgnoreCase);

    /// <inheritdoc />
    public Task<ExplorerAuthSignIn> ChallengeAsync(
        ExplorerAuthChallengeContext context,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);

        context.Inputs.TryGetValue(ExplorerAuthSchemes.UsernameInput, out var username);
        ArgumentException.ThrowIfNullOrWhiteSpace(username);

        // Forward the username as the bearer subject; the silo's dev authenticator
        // trusts it only when it names a configured bootstrap administrator, so this
        // can only re-assert a pre-declared admin identity, never mint an arbitrary
        // subject. No password is attached: the dev silo has no identity provider
        // to validate one against, and the token IS the identity.
        var signIn = new ExplorerAuthSignIn
        {
            SchemeId = SchemeId,
            DisplayName = username,
            Authentication = new LatticeCallAuthentication
            {
                Headers = new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    [LatticeCallAuthentication.AuthorizationHeaderName] = $"Bearer {username}",
                },
            },
        };

        return Task.FromResult(signIn);
    }
}
