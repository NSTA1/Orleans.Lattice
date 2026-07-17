namespace Orleans.Lattice.Api.Auth;

/// <summary>
/// A best-effort description of a cluster's access model, returned by
/// <see cref="ILatticeAuthAdmin.GetAccessModelAsync"/>, reporting what the silo
/// can <b>authoritatively</b> determine from its own registrations: the active
/// authentication mode, whether authorization rules are actually enforced, and
/// whether an identity directory is available for validating candidate ids
/// (together with the provider id and operator-facing explanation a create form
/// should render).
/// </summary>
/// <remarks>
/// The values reflect only in-silo registrations and cannot see a
/// transport-specific authorizer sitting in front of the cluster (for example a
/// flat-Basic authorizer at the gRPC edge), so <see cref="AuthenticationMode"/>
/// is a best-effort classification - see <see cref="AccessAuthenticationMode"/>.
/// </remarks>
[GenerateSerializer]
[Alias(ApiAuthTypeAliases.AccessModelDescriptor)]
[Immutable]
public sealed record AccessModelDescriptor
{
    /// <summary>
    /// The best-effort active authentication mode determined from the silo's
    /// registered credential authenticators.
    /// </summary>
    [Id(0)] public AccessAuthenticationMode AuthenticationMode { get; init; }

    /// <summary>
    /// <see langword="true"/> when a real access gate enforces membership and
    /// authorization rules on the data path; <see langword="false"/> when the
    /// no-op gate is in force and every request is allowed.
    /// </summary>
    [Id(1)] public bool RulesEnforced { get; init; }

    /// <summary>
    /// <see langword="true"/> when a searchable identity directory is configured,
    /// so candidate principal ids can be validated before access is granted;
    /// <see langword="false"/> when ids are accepted without validation.
    /// </summary>
    [Id(2)] public bool DirectoryAvailable { get; init; }

    /// <summary>
    /// The stable id of the configured identity-directory provider (for example
    /// <c>"null"</c>, <c>"entra"</c>, or <c>"static"</c>).
    /// </summary>
    [Id(3)] public required string DirectoryProviderId { get; init; }

    /// <summary>
    /// The operator-facing explanation of what a valid principal id is for this
    /// deployment, scoped to the kind of principal the Access create form enters
    /// (the group create form), suitable for rendering inline beneath that form's
    /// picker.
    /// </summary>
    [Id(4)] public required string DirectoryExplanation { get; init; }
}
