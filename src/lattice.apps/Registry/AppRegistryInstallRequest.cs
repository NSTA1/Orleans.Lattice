namespace Orleans.Lattice.Apps;

/// <summary>
/// The consent an operator gives when installing or upgrading an app: the exact app
/// identity, the capability ceiling approved for that version, and the role-to-group
/// bindings. A ceiling is required on every install and every upgrade, so a version
/// change can never reuse the previous version's consent.
/// </summary>
[GenerateSerializer, Alias(AppRegistryTypeAliases.AppRegistryInstallRequest)]
public sealed record AppRegistryInstallRequest
{
    /// <summary>
    /// The tenant the install belongs to. Defaults to <see cref="TenantId.Default"/>,
    /// which is the tenant a cluster with tenancy off resolves to.
    /// </summary>
    [Id(0)] public TenantId Tenant { get; init; } = TenantId.Default;

    /// <summary>The slug, exact version and descriptive provenance being consented to.</summary>
    [Id(1)] public required AppIdentity Identity { get; init; }

    /// <summary>The capability ceiling consented for <see cref="AppIdentity.Version"/>.</summary>
    [Id(2)] public required AppCapabilityCeiling Ceiling { get; init; }

    /// <summary>The role-to-membership-group bindings. Role names must be unique; empty by default.</summary>
    [Id(3)] public IReadOnlyList<AppRoleBinding> RoleBindings { get; init; } = Array.Empty<AppRoleBinding>();
}
