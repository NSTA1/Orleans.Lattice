namespace Orleans.Lattice.Apps;

/// <summary>
/// Activates, deactivates, and removes installed apps: resolves the installed version's
/// manifest from the <see cref="IAppSource"/>, validates it, compiles its roles against the
/// consented capability ceiling, provisions its structural trees, persists the compiled
/// rules as the app's whole owned rule set, and transitions the registry record.
/// </summary>
/// <remarks>
/// <para>
/// Every activation problem - an invalid or over-ceiling manifest, a missing source, a
/// missing membership or authorization registration, a tree or rule write failure - is
/// returned as a failed <see cref="AppActivationOutcome"/> and, when the activation-status record
/// can be read and written, recorded against the app. An unreadable status is not overwritten, and
/// a failed status write is logged rather than thrown. Runs for one tenant's app are serialized
/// cluster-wide. Only argument validation and an <see cref="LatticeAuthorizationDeniedException"/> for a caller without
/// <see cref="LatticeOperation.AppInstall"/> throw.
/// </para>
/// <para>
/// The mutating verbs require <see cref="LatticeOperation.AppInstall"/> over
/// <see cref="Orleans.Lattice.Auth.LatticeScope.ClusterWide"/>, checked on the grain every run passes through;
/// system-origin callers inside the cluster skip the check.
/// <see cref="GetStatusAsync"/> is an ungated, trusted in-process read, so a facade that
/// exposes it must gate it itself.
/// </para>
/// </remarks>
public interface IAppActivationPipeline
{
    /// <summary>Activates an installed app and marks it enabled.</summary>
    /// <param name="tenant">The tenant the app is installed for.</param>
    /// <param name="slug">The app to enable.</param>
    /// <param name="cancellationToken">Cancels the run.</param>
    /// <returns>The run's outcome.</returns>
    Task<AppActivationOutcome> EnableAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default);

    /// <summary>Withdraws an enabled app's rules and marks it disabled, keeping its trees and data.</summary>
    /// <param name="tenant">The tenant the app is installed for.</param>
    /// <param name="slug">The app to disable.</param>
    /// <param name="cancellationToken">Cancels the run.</param>
    /// <returns>The run's outcome.</returns>
    Task<AppActivationOutcome> DisableAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default);

    /// <summary>
    /// Withdraws an app's rules, soft-deletes its structural trees (never purging them itself, and
    /// never touching adopted trees), releases its adopted-tree ownership claims, and marks it
    /// uninstalled. The core purges each soft-deleted tree once its soft-delete window elapses unless
    /// it is recovered first; its structural ownership claims are held until then. On success every
    /// other enabled app in the tenant that reaches the uninstalled app through a cross-app role scope
    /// or subscription is then reconciled, so its cross-app grants to the absent owner are withdrawn;
    /// each dependant's outcome is recorded against it, not returned here.
    /// </summary>
    /// <param name="tenant">The tenant the app is installed for.</param>
    /// <param name="slug">The app to uninstall.</param>
    /// <param name="cancellationToken">Cancels the run.</param>
    /// <returns>The run's outcome.</returns>
    Task<AppActivationOutcome> UninstallAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default);

    /// <summary>
    /// Re-applies an app's current registry state: an enabled app is re-activated, any other
    /// state has its owned rules withdrawn. The registry state is never changed.
    /// </summary>
    /// <param name="tenant">The tenant the app is installed for.</param>
    /// <param name="slug">The app to reconcile.</param>
    /// <param name="cancellationToken">Cancels the run.</param>
    /// <returns>The run's outcome.</returns>
    Task<AppActivationOutcome> ReconcileAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default);

    /// <summary>Reads the activation evidence recorded against an app.</summary>
    /// <param name="tenant">The tenant the app is installed for.</param>
    /// <param name="slug">The app.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The recorded status, or <c>null</c> when no run was ever recorded.</returns>
    Task<AppActivationStatus?> GetStatusAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default);
}
