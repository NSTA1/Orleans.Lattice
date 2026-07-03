namespace Orleans.Lattice.Api.Auth;

/// <summary>
/// Centralised Orleans serialization alias constants for every
/// <c>Orleans.Lattice.Api.Auth</c> type that participates in the wire format.
/// Each alias is a short, fixed string that gives a type a stable wire identity
/// independent of its CLR name. Auth-API facade aliases use the <c>oli.</c>
/// prefix (Orleans Lattice Api) and are at most six characters, mirroring the
/// core (<c>ol.</c>) and authorization (<c>olz.</c>) alias tables; the
/// invariants are enforced by <c>ApiAuthTypeAliasesTests</c>.
/// </summary>
/// <remarks>
/// Never rename or reuse an alias value: it is part of the on-the-wire format.
/// New types append new constants.
/// </remarks>
public static class ApiAuthTypeAliases
{
    /// <summary>Alias for <see cref="AuthUser"/>.</summary>
    public const string AuthUser = "oli.us";

    /// <summary>Alias for <see cref="AuthGroup"/>.</summary>
    public const string AuthGroup = "oli.gr";

    /// <summary>Alias for <see cref="AuthPageRequest"/>.</summary>
    public const string AuthPageRequest = "oli.pq";

    /// <summary>Alias for <see cref="AuthUserPage"/>.</summary>
    public const string AuthUserPage = "oli.up";

    /// <summary>Alias for <see cref="AuthGroupPage"/>.</summary>
    public const string AuthGroupPage = "oli.gp";

    /// <summary>Alias for <see cref="AuthRulePage"/>.</summary>
    public const string AuthRulePage = "oli.rp";

    /// <summary>Alias for <see cref="AuthExplanation"/>.</summary>
    public const string AuthExplanation = "oli.ex";

    /// <summary>Alias for <see cref="AuthEffectivePermissions"/>.</summary>
    public const string AuthEffectivePermissions = "oli.ep";
}
