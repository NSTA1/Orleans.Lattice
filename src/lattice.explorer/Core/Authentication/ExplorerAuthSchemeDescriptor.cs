namespace Orleans.Lattice.Explorer.Core.Authentication;

/// <summary>
/// One authentication scheme an endpoint advertises: its stable id (matched
/// against an <see cref="IExplorerAuthMethod.SchemeId"/>), a friendly name for
/// the sign-in UI, and the public parameters a provider needs to run the
/// challenge (for example the Entra authority, tenant, client id, and audience).
/// The parameters carry only public configuration; the advertisement never
/// exposes a secret or any user data.
/// </summary>
public sealed record ExplorerAuthSchemeDescriptor
{
    /// <summary>The stable scheme id (for example <see cref="ExplorerAuthSchemes.Entra"/>).</summary>
    public required string SchemeId { get; init; }

    /// <summary>A friendly, human-readable name for the scheme, shown in the sign-in UI.</summary>
    public string DisplayName { get; init; } = string.Empty;

    /// <summary>
    /// The public parameters a provider needs to run the challenge (keys from
    /// <see cref="ExplorerAuthSchemes"/>). Empty for schemes that need none.
    /// </summary>
    public IReadOnlyDictionary<string, string> Parameters { get; init; } =
        new Dictionary<string, string>(StringComparer.Ordinal);
}
