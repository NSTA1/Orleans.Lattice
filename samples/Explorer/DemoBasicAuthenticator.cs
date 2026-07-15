using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Samples.Explorer;

/// <summary>
/// A minimal demo <see cref="ILatticeCredentialAuthenticator"/> that trusts the
/// username in the console's auto-applied Basic sign-in as the caller subject id.
/// The Explorer web console signs in with
/// <c>authorization: Basic base64(username:password)</c>; the auth / schema gRPC
/// bindings (configured with a <c>Basic</c> credential scheme) strip the scheme
/// and hand this authenticator the base64 token, which it decodes to recover the
/// username. That username is the caller subject, and the sample registers it as
/// a bootstrap administrator so the Access and Schema admin areas light up.
///
/// A real deployment resolves the subject from a validated JWT or Entra token
/// (see the JWT / Entra authenticators shipped with the Membership package); this
/// sample uses a trivial trusted-token authenticator so the whole flow runs on
/// one silo with no identity provider and the password is never checked.
/// </summary>
internal sealed class DemoBasicAuthenticator : ILatticeCredentialAuthenticator
{
    /// <summary>The credential scheme this authenticator claims.</summary>
    public const string Scheme = "Basic";

    /// <summary>The issuer stamped on the resolved principal.</summary>
    public const string Issuer = "https://issuer.explorer.sample/";

    /// <inheritdoc />
    public bool CanHandle(in LatticeCredential credential) =>
        string.Equals(credential.Scheme, Scheme, StringComparison.OrdinalIgnoreCase);

    /// <inheritdoc />
    public ValueTask<LatticePrincipal?> AuthenticateAsync(
        LatticeCredential credential,
        CancellationToken cancellationToken = default)
    {
        // The token is base64(username:password); the caller subject is the
        // username. A malformed token resolves to no principal (anonymous).
        string username;
        try
        {
            var decoded = Encoding.UTF8.GetString(Convert.FromBase64String(credential.Token));
            var separator = decoded.IndexOf(':');
            username = separator >= 0 ? decoded[..separator] : decoded;
        }
        catch (FormatException)
        {
            return new ValueTask<LatticePrincipal?>((LatticePrincipal?)null);
        }

        return string.IsNullOrEmpty(username)
            ? new ValueTask<LatticePrincipal?>((LatticePrincipal?)null)
            : new ValueTask<LatticePrincipal?>(new LatticePrincipal(username, Issuer));
    }
}
