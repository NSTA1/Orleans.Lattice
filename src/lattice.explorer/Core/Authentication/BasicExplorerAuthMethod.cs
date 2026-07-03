using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Core.Authentication;

/// <summary>
/// The built-in username/password auth method. It refactors the explorer's
/// original Basic sign-in into one <see cref="IExplorerAuthMethod"/> among many
/// with no behaviour change: it attaches the same
/// <c>authorization: Basic base64(user:password)</c> header the server-side
/// env-var authorizer expects. It handles the <see cref="ExplorerAuthSchemes.Basic"/>
/// scheme and, so a non-advertising endpoint keeps working exactly as before,
/// an empty (undiscovered) advertised scheme.
/// </summary>
public sealed class BasicExplorerAuthMethod : IExplorerAuthMethod
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
        context.Inputs.TryGetValue(ExplorerAuthSchemes.PasswordInput, out var password);

        // Preserve the original validation contract exactly: a null-or-whitespace
        // username throws ArgumentException, a null password throws
        // ArgumentNullException - both before any state is touched.
        ArgumentException.ThrowIfNullOrWhiteSpace(username);
        ArgumentNullException.ThrowIfNull(password);

        var signIn = new ExplorerAuthSignIn
        {
            SchemeId = SchemeId,
            DisplayName = username,
            Authentication = LatticeCallAuthentication.Basic(username, password),
        };

        return Task.FromResult(signIn);
    }
}
