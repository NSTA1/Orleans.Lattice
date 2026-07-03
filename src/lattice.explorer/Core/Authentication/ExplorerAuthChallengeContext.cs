namespace Orleans.Lattice.Explorer.Core.Authentication;

/// <summary>
/// The input to an <see cref="IExplorerAuthMethod.ChallengeAsync"/> run: the
/// scheme the endpoint requires, the public parameters the server advertised for
/// it (OIDC authority, tenant, client id, audience), any interactive inputs the
/// user supplied (a Basic username/password), and the clock the method should
/// use for token-expiry maths so the flow stays deterministically testable.
/// </summary>
public sealed record ExplorerAuthChallengeContext
{
    /// <summary>The selected scheme id, matching the chosen method's <see cref="IExplorerAuthMethod.SchemeId"/>.</summary>
    public required string SchemeId { get; init; }

    /// <summary>
    /// The public parameters the server advertised for this scheme (keys from
    /// <see cref="ExplorerAuthSchemes"/>, for example the Entra authority and
    /// client id). Empty when the endpoint did not advertise or the scheme takes
    /// no parameters.
    /// </summary>
    public IReadOnlyDictionary<string, string> Parameters { get; init; } =
        new Dictionary<string, string>(StringComparer.Ordinal);

    /// <summary>
    /// The interactive inputs the user supplied for this challenge (for example
    /// the Basic username and password under the
    /// <see cref="ExplorerAuthSchemes.UsernameInput"/> /
    /// <see cref="ExplorerAuthSchemes.PasswordInput"/> keys). Empty for schemes
    /// whose challenge is a browser redirect or device-code flow.
    /// </summary>
    public IReadOnlyDictionary<string, string?> Inputs { get; init; } =
        new Dictionary<string, string?>(StringComparer.Ordinal);

    /// <summary>The state-API endpoint being signed in to, or <see langword="null"/> when unknown.</summary>
    public string? Endpoint { get; init; }

    /// <summary>The clock a token-based method uses for expiry maths. Defaults to the system clock.</summary>
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;
}
