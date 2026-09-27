namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// Transport-independent app lifecycle and consent management in the caller's
/// active tenant, or the cluster when tenancy is disabled.
/// </summary>
/// <remarks>
/// Except for the advisory capability probe, implementations authorize every operation
/// with <see cref="LatticeOperation.AppInstall"/> before accessing registry or source
/// metadata. Probes do not grant authority. Bindings opt in explicitly and never load app code to inspect
/// a manifest. Responses and exception messages must not disclose composed physical
/// tree ids; tree references use app slugs and local names instead.
/// Lifecycle mutations return only Installed, Enabled, Disabled, or Uninstalled;
/// failures throw with sanitized messages. NotInstalled is exclusive to DescribeAsync.
/// DescribeAsync and ListAsync may report Failed only from known activation-failure
/// evidence, never by interpreting a disabled registry state as a failure.
/// </remarks>
public interface ILatticeAppsControl
{
    /// <summary>
    /// Installs the specified source version with group-only role bindings and an
    /// explicit version-pinned ceiling. Installation does not enable the app.
    /// </summary>
    /// <param name="request">The non-null installation request, validated before mutation.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The resulting lifecycle state and whether it changed.</returns>
    Task<AppLifecycleResult> InstallAsync(AppInstallRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Enables an installed app only after validating its manifest and intersecting
    /// all requested permissions with its consented ceiling. Excess capabilities
    /// fail activation until re-consented; an already-enabled app is unchanged.
    /// </summary>
    /// <param name="appSlug">The non-empty installed app slug.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The resulting lifecycle state and whether it changed.</returns>
    Task<AppLifecycleResult> EnableAsync(string appSlug, CancellationToken cancellationToken = default);

    /// <summary>Disables an installed app without deleting its trees; an already-disabled app is unchanged.</summary>
    /// <param name="appSlug">The non-empty installed app slug.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The resulting lifecycle state and whether it changed.</returns>
    Task<AppLifecycleResult> DisableAsync(string appSlug, CancellationToken cancellationToken = default);

    /// <summary>
    /// Uninstalls an app, removing its owned grants and soft-deleting its trees
    /// under their configured retention policy. Never physically purges data:
    /// purge remains a separate <see cref="LatticeOperation.TreeLifecycle"/> operation.
    /// </summary>
    /// <param name="appSlug">The non-empty app slug to uninstall.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The resulting lifecycle state and whether it changed.</returns>
    Task<AppLifecycleResult> UninstallAsync(string appSlug, CancellationToken cancellationToken = default);

    /// <summary>Lists installed app summaries visible in the caller's active isolation context.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The installed app catalog; no trees or composed physical ids are exposed.</returns>
    Task<AppCatalog> ListAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Describes a manifest and its requested capabilities without loading app code,
    /// including before installation. A null version selects the installed version,
    /// or the source's available version when not installed. Unknown apps or versions
    /// return null, not an empty success-shaped descriptor.
    /// </summary>
    /// <param name="appSlug">The non-empty app slug to inspect.</param>
    /// <param name="version">An exact source version, or null for the default selection.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The source description and matching installation state, or null when absent.</returns>
    Task<AppDescriptor?> DescribeAsync(string appSlug, string? version = null, CancellationToken cancellationToken = default);

    /// <summary>Reads the ceiling pinned to the installed app version, or null when the app is not installed.</summary>
    /// <param name="appSlug">The non-empty app slug whose consent to inspect.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The version-pinned consent, or null when absent.</returns>
    Task<AppConsentReport?> GetConsentAsync(string appSlug, CancellationToken cancellationToken = default);

    /// <summary>
    /// Replaces the approved ceiling, including its entire exception list, only for
    /// the explicitly named installed version. A version mismatch fails without
    /// mutation. Revalidates active grants so a reduced ceiling cannot leave stale
    /// authority; does not implicitly enable a disabled app.
    /// </summary>
    /// <param name="request">The non-null version-pinned replacement consent.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The consent read back after the update.</returns>
    Task<AppConsentReport> UpdateConsentAsync(AppConsentUpdate request, CancellationToken cancellationToken = default);

    /// <summary>Probes caller access without registry mutation; all permissions default to denied.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Advisory permissions; every actual operation still authorizes independently.</returns>
    Task<LatticeAppsCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default);
}
