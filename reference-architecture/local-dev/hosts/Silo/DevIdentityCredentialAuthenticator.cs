using Orleans.Lattice.Membership;

namespace Orleans.Lattice.ReferenceArchitecture.Silo;

/// <summary>
/// Development-only credential authenticator for the local-dev dual-cluster
/// harness. When Entra is disabled the silo has no identity provider, so it needs
/// a stand-in that maps an inbound credential to a subject the authorization model
/// can enforce on. This authenticator trusts a forwarded bearer token as the
/// caller's subject id verbatim - any token value authenticates AS that identity -
/// and attaches the groups the mounted identity model
/// (<c>reference-architecture/local-dev/identities.json</c>) declares for it. That
/// is exactly what lets an agent "act as" any identity by setting
/// <c>Authorization: Bearer &lt;id&gt;</c> (through the MCP head, a gRPC facade, or
/// the Explorer sign-in) without an Entra tenant.
/// </summary>
/// <remarks>
/// <para>
/// Trusting an arbitrary token as a subject is safe ONLY because this is a
/// throwaway local harness with no real secrets, and because enforcement is real:
/// a subject carries authority solely through the groups the identity model grants
/// it under deny-by-default, so an unknown or unlisted id authenticates as itself
/// but with no groups and therefore no access. The <c>Program.cs</c> wiring
/// registers this authenticator exclusively when Entra is disabled, so it can never
/// coexist with, or weaken, a real deployment's Entra authenticator.
/// </para>
/// <para>
/// Asserted groups are attached to the principal directly (independent of the
/// durable membership directory) so group grants resolve on the very first call,
/// before the <see cref="LocalDevIdentitySeeder"/> has necessarily finished seeding
/// the directory. The seeder additionally writes the same groups and memberships
/// into the directory so they are introspectable through the ordinary read / scan
/// surface (and the Explorer Access tab).
/// </para>
/// </remarks>
internal sealed class DevIdentityCredentialAuthenticator : ILatticeCredentialAuthenticator
{
    /// <summary>The issuer stamped on the resolved principal.</summary>
    public const string Issuer = "https://local-dev-identity.reference-architecture.invalid/";

    private readonly LocalDevIdentityModel _model;

    /// <summary>Initialises the authenticator from the loaded identity model.</summary>
    /// <param name="model">The parsed identity model (identity to groups map). Must not be <c>null</c>.</param>
    public DevIdentityCredentialAuthenticator(LocalDevIdentityModel model)
    {
        ArgumentNullException.ThrowIfNull(model);
        _model = model;
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
        var subjectId = credential.Token;
        if (string.IsNullOrEmpty(subjectId))
        {
            // No forwarded token => anonymous, which deny-by-default rejects.
            return new ValueTask<LatticePrincipal?>((LatticePrincipal?)null);
        }

        // Attach the identity's declared groups directly, so group grants resolve
        // without waiting on the directory seed. An id not in the model carries no
        // groups and is therefore left with no authority under deny-by-default.
        IReadOnlyCollection<string>? assertedGroups = null;
        if (_model.Identities.TryGetValue(subjectId, out var identity) && identity.Groups.Count > 0)
        {
            assertedGroups = identity.Groups;
        }

        return new ValueTask<LatticePrincipal?>(new LatticePrincipal(subjectId, Issuer, assertedGroups: assertedGroups));
    }
}
