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

    /// <summary>
    /// The version the caller read as installed when it decided this transition, or <c>null</c>
    /// to apply against whatever is installed. When set, the transition is applied only while the
    /// live record still carries exactly this version, compared inside the registry's
    /// optimistic-concurrency loop; otherwise it is rejected with
    /// <see cref="AppRegistryTransitionError.ConcurrencyConflict"/> and nothing changes. An
    /// install (which requires no live record) that sets it is therefore always rejected. This
    /// closes the read-then-upgrade race in which a consent update decided against one version
    /// would otherwise roll back an upgrade that landed after the read.
    /// </summary>
    [Id(4)] public AppVersion? ExpectedVersion { get; init; }

    /// <summary>
    /// The consented app UI bridge grants to record, or <c>null</c> to leave them as they are: an upgrade
    /// keeps the grants the current record carries, and an install records none. Consent is never widened
    /// implicitly, so an upgrade that requests more than the kept grants cannot activate until it is
    /// re-consented.
    /// </summary>
    [Id(5)] public AppUiBridgeRequest? BridgeConsent { get; init; }
}
