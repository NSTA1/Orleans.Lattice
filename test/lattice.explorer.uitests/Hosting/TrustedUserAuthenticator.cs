using System.Text;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// The test world's credential authenticator: it trusts the user name in a Basic
/// credential as the caller, and refuses only <see cref="WorldIdentities.WrongPassword"/>,
/// the world's stand-in for a mistyped password.
/// </summary>
/// <remarks>
/// Only the identity is trusted. Everything that identity may do is decided by the
/// real authorization engine over the rules and groups the world seeds, which is what
/// the suite exercises.
/// </remarks>
internal sealed class TrustedUserAuthenticator : ILatticeCredentialAuthenticator
{
    /// <summary>The credential scheme the world's gRPC bindings hand to this authenticator.</summary>
    public const string Scheme = "Basic";

    private const string Issuer = "https://issuer.explorer-uitests.invalid/";

    /// <inheritdoc />
    public bool CanHandle(in LatticeCredential credential) =>
        string.Equals(credential.Scheme, Scheme, StringComparison.OrdinalIgnoreCase);

    /// <inheritdoc />
    public ValueTask<LatticePrincipal?> AuthenticateAsync(LatticeCredential credential, CancellationToken cancellationToken = default)
    {
        string user;
        string password;
        try
        {
            var decoded = Encoding.UTF8.GetString(Convert.FromBase64String(credential.Token));
            var separator = decoded.IndexOf(':', StringComparison.Ordinal);
            user = separator >= 0 ? decoded[..separator] : decoded;
            password = separator >= 0 ? decoded[(separator + 1)..] : string.Empty;
        }
        catch (FormatException)
        {
            return ValueTask.FromResult<LatticePrincipal?>(null);
        }

        if (string.IsNullOrEmpty(user) || string.Equals(password, WorldIdentities.WrongPassword, StringComparison.Ordinal))
        {
            return ValueTask.FromResult<LatticePrincipal?>(null);
        }

        return ValueTask.FromResult<LatticePrincipal?>(new LatticePrincipal(user, Issuer));
    }
}
