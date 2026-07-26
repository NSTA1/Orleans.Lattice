using Microsoft.Extensions.Configuration;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.ReferenceArchitecture.Silo;

/// <summary>
/// Development-only credential authenticator for the local compose harness. When
/// Entra is disabled the silo has no identity provider, so a credential forwarded
/// from the MCP head (or any facade caller) resolves to no subject and reads as
/// anonymous - which the control-plane-isolated authorization tree denies, so MCP
/// discovery surfaces no permission-scoped tools. This authenticator closes that
/// gap by trusting a forwarded bearer token as the caller's subject id, but ONLY
/// when that id is one of the configured bootstrap administrators
/// (<c>Auth:BootstrapAdministrators</c>). It therefore cannot mint an arbitrary
/// subject - it can only re-assert a pre-declared administrator identity - and the
/// wiring in <c>Program.cs</c> registers it exclusively when Entra is disabled, so
/// it can never coexist with, or weaken, a real deployment's Entra authenticator.
/// </summary>
/// <remarks>
/// This is the local-harness analogue of the Entra authenticator's job on a
/// deployed estate: map an inbound credential to the administrator subject the
/// authorization model enforces on. On Azure the mapping is a validated Entra JWT
/// resolving to the admin's object id; here it is a trusted forwarded token
/// matching a configured administrator id.
/// </remarks>
internal sealed class DevBypassCredentialAuthenticator : ILatticeCredentialAuthenticator
{
    /// <summary>The issuer stamped on the resolved principal.</summary>
    public const string Issuer = "https://local-dev-bypass.reference-architecture.invalid/";

    private readonly HashSet<string> _trustedSubjects;

    /// <summary>
    /// Initialises the authenticator from the configured bootstrap administrators.
    /// </summary>
    /// <param name="configuration">The host configuration.</param>
    public DevBypassCredentialAuthenticator(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        _trustedSubjects = new HashSet<string>(
            AdministratorAccessSeeder.ParseAdministrators(configuration),
            StringComparer.Ordinal);
    }

    /// <inheritdoc />
    public bool CanHandle(in LatticeCredential credential) =>
        credential.Scheme is null
        || string.Equals(credential.Scheme, "Bearer", StringComparison.OrdinalIgnoreCase);

    /// <inheritdoc />
    public ValueTask<LatticePrincipal?> AuthenticateAsync(
        LatticeCredential credential,
        CancellationToken cancellationToken = default)
    {
        // Trust the forwarded token as the caller subject only when it names a
        // configured bootstrap administrator; anything else resolves to anonymous.
        var subjectId = credential.Token;
        if (string.IsNullOrEmpty(subjectId) || !_trustedSubjects.Contains(subjectId))
        {
            return new ValueTask<LatticePrincipal?>((LatticePrincipal?)null);
        }

        return new ValueTask<LatticePrincipal?>(new LatticePrincipal(subjectId, Issuer));
    }
}
