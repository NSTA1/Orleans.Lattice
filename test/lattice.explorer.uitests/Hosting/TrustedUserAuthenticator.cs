using System.Text;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// The test world's credential authenticator: it trusts the user name in a Basic
/// credential as the caller, and never checks the password.
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
        try
        {
            var decoded = Encoding.UTF8.GetString(Convert.FromBase64String(credential.Token));
            var separator = decoded.IndexOf(':', StringComparison.Ordinal);
            user = separator >= 0 ? decoded[..separator] : decoded;
        }
        catch (FormatException)
        {
            return ValueTask.FromResult<LatticePrincipal?>(null);
        }

        return ValueTask.FromResult(string.IsNullOrEmpty(user) ? null : new LatticePrincipal(user, Issuer));
    }
}
