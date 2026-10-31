using System.Security.Cryptography;
using System.Text;
using Orleans.Lattice.Explorer.EndToEnd.Hosting;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.EndToEnd.Hosts.Silo;

/// <summary>
/// The journey estate's sign-in check, and the only test-owned seam in its request path:
/// a Basic credential is accepted when its user is in the harness's identity directory and
/// its password matches, and refused otherwise. It maps a user to a subject id and nothing
/// else; what that subject may do, and which groups it is in, is decided by the real
/// membership and authorization grains.
/// </summary>
/// <remarks>
/// The directory file is re-read when it changes, so the harness can add a persona to a
/// running estate the way a directory gains a user.
/// </remarks>
internal sealed class DirectoryCredentialAuthenticator(JourneyDirectoryFile directory) : ILatticeCredentialAuthenticator
{
    /// <summary>The credential scheme every journey binding hands to this authenticator.</summary>
    public const string Scheme = "Basic";

    /// <summary>The issuer of every principal this authenticator produces.</summary>
    public const string Issuer = "https://directory.journeys.lattice.invalid/";

    /// <inheritdoc />
    public bool CanHandle(in LatticeCredential credential) =>
        string.Equals(credential.Scheme, Scheme, StringComparison.OrdinalIgnoreCase);

    /// <inheritdoc />
    public ValueTask<LatticePrincipal?> AuthenticateAsync(LatticeCredential credential, CancellationToken cancellationToken = default)
    {
        string decoded;
        try
        {
            decoded = Encoding.UTF8.GetString(Convert.FromBase64String(credential.Token));
        }
        catch (FormatException)
        {
            return ValueTask.FromResult<LatticePrincipal?>(null);
        }

        var separator = decoded.IndexOf(':', StringComparison.Ordinal);
        if (separator <= 0)
        {
            return ValueTask.FromResult<LatticePrincipal?>(null);
        }

        var user = decoded[..separator];
        var password = decoded[(separator + 1)..];
        var entry = directory.Current.Users.Find(candidate => string.Equals(candidate.Id, user, StringComparison.Ordinal));
        if (entry is null || !FixedTimeEquals(entry.Password, password))
        {
            return ValueTask.FromResult<LatticePrincipal?>(null);
        }

        return ValueTask.FromResult<LatticePrincipal?>(new LatticePrincipal(entry.Id, Issuer));
    }

    private static bool FixedTimeEquals(string expected, string actual) =>
        CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(expected), Encoding.UTF8.GetBytes(actual));
}
