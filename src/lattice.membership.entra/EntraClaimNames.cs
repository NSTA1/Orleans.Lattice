namespace Orleans.Lattice.Membership.Entra;

/// <summary>
/// The Microsoft Entra ID v2.0 token claim names this package reads. Centralized
/// so the authenticator and the group-overage detection agree on the exact claim
/// keys Entra emits.
/// </summary>
public static class EntraClaimNames
{
    /// <summary>The immutable object id of the subject (user or service principal): the stable subject id.</summary>
    public const string ObjectId = "oid";

    /// <summary>The tenant id the token was issued for.</summary>
    public const string TenantId = "tid";

    /// <summary>The security group object ids the subject belongs to.</summary>
    public const string Groups = "groups";

    /// <summary>The application role values granted to the caller.</summary>
    public const string Roles = "roles";

    /// <summary>The delegated permission scopes (space-delimited) present on a delegated (user) token.</summary>
    public const string Scope = "scp";

    /// <summary>The authorized party (client id) present on an app-only token.</summary>
    public const string AuthorizedParty = "azp";

    /// <summary>The subject (fallback subject id when <see cref="ObjectId"/> is absent).</summary>
    public const string Subject = "sub";

    /// <summary>The overage marker naming the distributed claim sources. Present when a claim overflowed the token.</summary>
    public const string ClaimNames = "_claim_names";

    /// <summary>The overage marker describing where an overflowed claim can be retrieved from.</summary>
    public const string ClaimSources = "_claim_sources";
}
