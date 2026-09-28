namespace Orleans.Lattice.Apps;

/// <summary>
/// One app install as durably recorded in the app registry, keyed
/// <c>{tenantId}/{appSlug}</c> in the reserved <c>sys-app-registry</c> tree. It carries
/// the app's identity (slug, version, provenance), the isolation context it was
/// installed in, the capability ceiling the operator consented to - pinned to the
/// exact version it was consented for - the role-to-membership-group bindings, and the
/// lifecycle state. Records are produced only by the registry's lifecycle transitions;
/// they are never authored field by field.
/// </summary>
/// <remarks>
/// <b>The ceiling is pinned per (app, version).</b> <see cref="CeilingVersion"/> names
/// the version <see cref="Ceiling"/> was consented for, and every transition that
/// changes <see cref="Version"/> must supply a fresh ceiling, so a version change can
/// never silently inherit the previous version's consent. A consumer activating the app
/// must refuse a record whose <see cref="IsCeilingPinnedToVersion"/> is <c>false</c>.
/// </remarks>
[GenerateSerializer, Alias(AppRegistryTypeAliases.AppRegistryRecord)]
public sealed record AppRegistryRecord
{
    /// <summary>The tenant and cluster the install belongs to.</summary>
    [Id(0)] public required AppIsolationContext Isolation { get; init; }

    /// <summary>The app slug; with <see cref="AppIsolationContext.Tenant"/> it forms the registry key.</summary>
    [Id(1)] public required AppSlug Slug { get; init; }

    /// <summary>The installed app version.</summary>
    [Id(2)] public required AppVersion Version { get; init; }

    /// <summary>The descriptive origin of the installed artifact. Metadata, never authority.</summary>
    [Id(3)] public required AppProvenance Provenance { get; init; }

    /// <summary>The operator-consented capability ceiling every compiled rule is intersected with.</summary>
    [Id(4)] public required AppCapabilityCeiling Ceiling { get; init; }

    /// <summary>The version <see cref="Ceiling"/> was consented for.</summary>
    [Id(5)] public required AppVersion CeilingVersion { get; init; }

    /// <summary>The bindings of manifest roles to membership groups, in the order supplied at consent.</summary>
    [Id(6)] public IReadOnlyList<AppRoleBinding> RoleBindings { get; init; } = Array.Empty<AppRoleBinding>();

    /// <summary>The current lifecycle state.</summary>
    [Id(7)] public required AppRegistryLifecycleState State { get; init; }

    /// <summary>
    /// The record revision: <c>1</c> on the first install and incremented by every
    /// applied transition, including a re-install after uninstall, so it never regresses.
    /// </summary>
    [Id(8)] public long Revision { get; init; }

    /// <summary>When the current install (the latest install transition) was recorded.</summary>
    [Id(9)] public DateTimeOffset InstalledAtUtc { get; init; }

    /// <summary>When <see cref="State"/> last changed.</summary>
    [Id(10)] public DateTimeOffset StateChangedAtUtc { get; init; }

    /// <summary>When the current ceiling and role bindings were consented (install or upgrade).</summary>
    [Id(11)] public DateTimeOffset ConsentedAtUtc { get; init; }

    /// <summary>
    /// The subject id of the caller that consented to the current ceiling, or <c>null</c>
    /// when the consent was recorded by trusted system-origin infrastructure.
    /// </summary>
    [Id(12)] public string? ConsentedBy { get; init; }

    /// <summary>
    /// The app UI bridge grants the operator consented to, or <c>null</c> when none was ever recorded (a
    /// record written before bridge consent existed). An activation refuses a manifest whose requested
    /// bridge grants (<see cref="AppUiBridgeRequest.FromManifest(AppManifest)"/>) add anything this set does
    /// not cover, so an upgrade that widens the bridge must be re-consented, exactly as a widened ceiling
    /// must. A null value covers nothing.
    /// </summary>
    [Id(13)] public AppUiBridgeRequest? ConsentedBridge { get; init; }

    /// <summary>The tenant that owns the install (shorthand for <see cref="AppIsolationContext.Tenant"/>).</summary>
    public TenantId Tenant => Isolation.Tenant;

    /// <summary>
    /// <c>true</c> when <see cref="Ceiling"/> was consented for exactly <see cref="Version"/>.
    /// Always <c>true</c> for a record the registry produced; a <c>false</c> value means the
    /// stored record is inconsistent and the app must be re-consented before it is activated.
    /// </summary>
    public bool IsCeilingPinnedToVersion => CeilingVersion == Version;
}
