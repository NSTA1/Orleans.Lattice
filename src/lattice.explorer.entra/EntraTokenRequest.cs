namespace Orleans.Lattice.Explorer.Entra;

/// <summary>
/// The resolved parameters for a single Entra token acquisition: the authority,
/// client id, and scopes an <see cref="IEntraInteractiveTokenAcquirer"/> uses to
/// drive the interactive or silent flow. Built from the advertised scheme
/// parameters (or static options) at challenge time.
/// </summary>
public sealed record EntraTokenRequest
{
    /// <summary>The OIDC authority the token is acquired from.</summary>
    public required string Authority { get; init; }

    /// <summary>The public client (application) id.</summary>
    public required string ClientId { get; init; }

    /// <summary>The scopes requested for the access token (the State API audience).</summary>
    public required IReadOnlyList<string> Scopes { get; init; }

    /// <summary>
    /// Whether to use the device-code flow rather than an interactive browser
    /// redirect for the initial acquisition.
    /// </summary>
    public bool UseDeviceCode { get; init; }

    /// <summary>
    /// The username (UPN) of the account this request's silent renewal must
    /// bind to, captured from the interactive sign-in. When set, silent
    /// acquisition selects the matching cached account rather than an arbitrary
    /// one, so a shared MSAL token cache holding more than one account never
    /// renews with a different operator's identity. Empty or <see langword="null"/>
    /// on the initial interactive request (the account is not yet known).
    /// </summary>
    public string? Username { get; init; }
}
