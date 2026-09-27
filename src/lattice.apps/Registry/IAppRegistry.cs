namespace Orleans.Lattice.Apps;

/// <summary>
/// The durable app registry: one <see cref="AppRegistryRecord"/> per app install, stored
/// in the reserved <c>sys-app-registry</c> tree keyed <c>{tenantId}/{appSlug}</c>, with
/// the lifecycle transitions that produce those records.
/// </summary>
/// <remarks>
/// <para>
/// <b>Transitions are gated; reads are not.</b> Every transition authorizes the caller
/// for <c>LatticeOperation.AppInstall</c> over <c>LatticeScope.ClusterWide()</c> through
/// the shared access gate before touching storage, throwing
/// <see cref="LatticeAuthorizationDeniedException"/> on a denial (trusted system-origin
/// callers skip the check). The storage reads and writes themselves run system-origin,
/// like the tenant registry's. The read methods are an in-process surface for trusted
/// infrastructure (activation, facades) and perform no authorization: a facade exposing
/// them to external callers must gate them itself. The backing tree is additionally
/// protected by control-plane read isolation, so no data-plane read grant (not even a
/// cluster-wide wildcard) exposes it through the ordinary tree surface.
/// </para>
/// <para>
/// <b>Rejections are structured.</b> An illegal transition returns an
/// <see cref="AppRegistryTransitionResult"/> carrying the reason rather than throwing.
/// Repeating a transition whose target state already holds (enable an enabled app,
/// disable a disabled one, uninstall an uninstalled one) is an idempotent success that
/// writes nothing.
/// </para>
/// </remarks>
public interface IAppRegistry
{
    /// <summary>Reads one install record.</summary>
    /// <param name="tenant">The owning tenant. Must be initialised.</param>
    /// <param name="slug">The app slug. Must be initialised.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The record, or <c>null</c> when the app was never installed for the tenant.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenant"/> or <paramref name="slug"/> is uninitialised.</exception>
    Task<AppRegistryRecord?> GetAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default);

    /// <summary>Enumerates every install record fleet-wide, in ascending key order.</summary>
    /// <param name="cancellationToken">Cancels the scan.</param>
    /// <returns>Every record, including uninstalled ones.</returns>
    IAsyncEnumerable<AppRegistryRecord> ListAsync(CancellationToken cancellationToken = default);

    /// <summary>Enumerates one tenant's install records by prefix scan, in ascending slug order.</summary>
    /// <param name="tenant">The owning tenant. Must be initialised.</param>
    /// <param name="cancellationToken">Cancels the scan.</param>
    /// <returns>The tenant's records, including uninstalled ones.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenant"/> is uninitialised.</exception>
    IAsyncEnumerable<AppRegistryRecord> ListForTenantAsync(TenantId tenant, CancellationToken cancellationToken = default);

    /// <summary>
    /// Installs an app that is absent or <see cref="AppRegistryLifecycleState.Uninstalled"/>,
    /// recording its identity, isolation context, pinned ceiling and role bindings in the
    /// <see cref="AppRegistryLifecycleState.Installed"/> state.
    /// </summary>
    /// <param name="request">The consented install. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the transition.</param>
    /// <returns>The outcome; <see cref="AppRegistryTransitionError.AlreadyInstalled"/> when an install is live.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="request"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException">The request is malformed.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller does not hold <c>AppInstall</c>.</exception>
    Task<AppRegistryTransitionResult> InstallAsync(AppRegistryInstallRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Re-consents a live install (<see cref="AppRegistryLifecycleState.Installed"/>,
    /// <see cref="AppRegistryLifecycleState.Enabled"/> or <see cref="AppRegistryLifecycleState.Disabled"/>):
    /// replaces its version, provenance, ceiling and role bindings, pinning the new ceiling
    /// to the new version, and keeps its lifecycle state. The same version may be supplied
    /// to re-consent without an upgrade.
    /// </summary>
    /// <param name="request">The consented upgrade. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the transition.</param>
    /// <returns>The outcome; <see cref="AppRegistryTransitionError.NotInstalled"/> when no install is live.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="request"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException">The request is malformed.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller does not hold <c>AppInstall</c>.</exception>
    Task<AppRegistryTransitionResult> UpgradeAsync(AppRegistryInstallRequest request, CancellationToken cancellationToken = default);

    /// <summary>Enables an <see cref="AppRegistryLifecycleState.Installed"/> or <see cref="AppRegistryLifecycleState.Disabled"/> app.</summary>
    /// <param name="tenant">The owning tenant. Must be initialised.</param>
    /// <param name="slug">The app slug. Must be initialised.</param>
    /// <param name="cancellationToken">Cancels the transition.</param>
    /// <returns>The outcome.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenant"/> or <paramref name="slug"/> is uninitialised.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller does not hold <c>AppInstall</c>.</exception>
    Task<AppRegistryTransitionResult> EnableAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default);

    /// <summary>Disables an <see cref="AppRegistryLifecycleState.Enabled"/> app.</summary>
    /// <param name="tenant">The owning tenant. Must be initialised.</param>
    /// <param name="slug">The app slug. Must be initialised.</param>
    /// <param name="cancellationToken">Cancels the transition.</param>
    /// <returns>The outcome.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenant"/> or <paramref name="slug"/> is uninitialised.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller does not hold <c>AppInstall</c>.</exception>
    Task<AppRegistryTransitionResult> DisableAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default);

    /// <summary>
    /// Uninstalls a live install, retaining its record in the
    /// <see cref="AppRegistryLifecycleState.Uninstalled"/> state. Data is not touched here.
    /// </summary>
    /// <param name="tenant">The owning tenant. Must be initialised.</param>
    /// <param name="slug">The app slug. Must be initialised.</param>
    /// <param name="cancellationToken">Cancels the transition.</param>
    /// <returns>The outcome.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenant"/> or <paramref name="slug"/> is uninitialised.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller does not hold <c>AppInstall</c>.</exception>
    Task<AppRegistryTransitionResult> UninstallAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default);
}
