using Orleans.Lattice.Apps;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// The in-process implementation of <see cref="ILatticeAppsControl"/>: one generic
/// facade that dispatches every app lifecycle and consent verb by app slug. It owns
/// no admin plane of its own; it composes the app registry (<see cref="IAppRegistry"/>:
/// install, upgrade, consent, listing), the app source seam (<see cref="IAppSource"/>:
/// manifest description without loading app code) and the activation pipeline
/// (<see cref="IAppActivationPipeline"/>: enable, disable, uninstall, recorded status),
/// mirroring how the tree-administration facade composes its engines.
/// </summary>
/// <remarks>
/// <para>
/// <b>Order of every verb.</b> (1) Parse and validate caller input; (2) resolve the
/// caller's active tenant through <see cref="ITenantContextResolver"/> (the default
/// tenant when tenancy is off) and compose every caller-supplied tree reference under
/// it, checking the single effective id against the reserved namespaces; (3) authorize
/// <see cref="LatticeOperation.AppInstall"/> over the cluster-wide scope through the
/// shared access gate; only then (4) touch registry, source or activation metadata.
/// A denied caller therefore learns nothing about which apps exist.
/// </para>
/// <para>
/// <b>Read verbs are gated too.</b> The registry holds ceilings, consent and role
/// bindings and is control-plane read isolated, so the ability to see an app is the
/// <see cref="LatticeOperation.AppInstall"/> capability itself. The registry reads and
/// the pipeline's status read are ungated in-process surfaces, which is why this facade
/// gates before calling them.
/// </para>
/// <para>
/// <b>No physical ids escape.</b> Responses carry slugs and app-local tree names only.
/// Every verb funnels its exceptions through <see cref="AppsControlExceptionSanitizer"/>,
/// so an engine failure whose message carries a composed id is rethrown with the id
/// replaced by its app-local name.
/// </para>
/// </remarks>
internal sealed partial class LatticeAppsControl : ILatticeAppsControl
{
    private readonly IAppRegistry _registry;
    private readonly IAppSource _source;
    private readonly IAppActivationPipeline _pipeline;
    private readonly ILatticeAccessGate _gate;
    private readonly ITenantContextResolver _tenants;
    private readonly ILatticeMembershipContext? _membership;

    /// <summary>Initializes a new <see cref="LatticeAppsControl"/>.</summary>
    /// <param name="registry">The app registry for install, upgrade, consent and listing.</param>
    /// <param name="source">The app source seam for manifest description.</param>
    /// <param name="pipeline">The activation pipeline for enable, disable, uninstall and status.</param>
    /// <param name="gate">The shared access gate every verb authorizes through.</param>
    /// <param name="tenants">The active-tenant resolver.</param>
    /// <param name="membership">The membership context resolving the caller, or null for anonymous.</param>
    /// <exception cref="ArgumentNullException">A required dependency is null.</exception>
    public LatticeAppsControl(
        IAppRegistry registry,
        IAppSource source,
        IAppActivationPipeline pipeline,
        ILatticeAccessGate gate,
        ITenantContextResolver tenants,
        ILatticeMembershipContext? membership = null)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(pipeline);
        ArgumentNullException.ThrowIfNull(gate);
        ArgumentNullException.ThrowIfNull(tenants);
        _registry = registry;
        _source = source;
        _pipeline = pipeline;
        _gate = gate;
        _tenants = tenants;
        _membership = membership;
    }

    /// <inheritdoc />
    public async Task<AppLifecycleResult> EnableAsync(string appSlug, CancellationToken cancellationToken = default)
    {
        try
        {
            return await RunActivationAsync(appSlug, AppActivationOperation.Enable, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (AppsControlExceptionSanitizer.TryRewrite(ex, appSlug, out var sanitized))
        {
            throw sanitized;
        }
    }

    /// <inheritdoc />
    public async Task<AppLifecycleResult> DisableAsync(string appSlug, CancellationToken cancellationToken = default)
    {
        try
        {
            return await RunActivationAsync(appSlug, AppActivationOperation.Disable, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (AppsControlExceptionSanitizer.TryRewrite(ex, appSlug, out var sanitized))
        {
            throw sanitized;
        }
    }

    /// <inheritdoc />
    public async Task<AppLifecycleResult> UninstallAsync(string appSlug, CancellationToken cancellationToken = default)
    {
        try
        {
            return await RunActivationAsync(appSlug, AppActivationOperation.Uninstall, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (AppsControlExceptionSanitizer.TryRewrite(ex, appSlug, out var sanitized))
        {
            throw sanitized;
        }
    }

    private async Task<AppLifecycleResult> RunActivationAsync(
        string appSlug,
        AppActivationOperation operation,
        CancellationToken cancellationToken)
    {
        var slug = AppsControlMapping.ParseSlug(appSlug, nameof(appSlug));
        var tenant = await ResolveTenantAsync(cancellationToken).ConfigureAwait(false);
        await AuthorizeAsync(cancellationToken).ConfigureAwait(false);

        var outcome = operation switch
        {
            AppActivationOperation.Enable => await _pipeline.EnableAsync(tenant, slug, cancellationToken).ConfigureAwait(false),
            AppActivationOperation.Disable => await _pipeline.DisableAsync(tenant, slug, cancellationToken).ConfigureAwait(false),
            _ => await _pipeline.UninstallAsync(tenant, slug, cancellationToken).ConfigureAwait(false),
        };

        return ToLifecycleResult(slug, outcome);
    }

    private static AppLifecycleResult ToLifecycleResult(AppSlug slug, AppActivationOutcome outcome)
    {
        if (!outcome.Succeeded)
        {
            throw AppsControlFailures.FromActivation(slug, outcome);
        }

        if (outcome.Version is not { } version || outcome.State is not { } state)
        {
            throw new InvalidOperationException(
                $"The {outcome.Operation.ToString().ToLowerInvariant()} of app '{slug}' reported no resulting state.");
        }

        return new AppLifecycleResult
        {
            Slug = slug.Value,
            Version = version.Value,
            State = AppsControlMapping.ToWireState(state),
            Changed = outcome.Changed,
        };
    }

    private static AppLifecycleResult ToLifecycleResult(AppRegistryRecord record, bool changed) =>
        new()
        {
            Slug = record.Slug.Value,
            Version = record.Version.Value,
            State = AppsControlMapping.ToWireState(record.State),
            Changed = changed,
        };

    /// <summary>
    /// Authorizes <see cref="LatticeOperation.AppInstall"/> over the cluster-wide scope
    /// through the shared gate. A key-filtered allow is refused: the capability is not
    /// attached to a key. System-origin callers and the no-op gate skip enforcement.
    /// </summary>
    private ValueTask AuthorizeAsync(CancellationToken cancellationToken) =>
        LatticeAccessGateEnforcement.EnforceWholeTreeControlAsync(
            _gate, _membership, LatticeScope.ClusterWideTreeId, LatticeOperation.AppInstall, cancellationToken);

    /// <summary>
    /// Resolves the caller's active tenant, preferring the synchronous warm path. A
    /// resolver that denies by resolving the uninitialised tenant fails closed.
    /// </summary>
    private ValueTask<TenantId> ResolveTenantAsync(CancellationToken cancellationToken)
    {
        if (_tenants.TryResolveCurrent(out var tenant))
        {
            return new ValueTask<TenantId>(RequireTenant(tenant));
        }

        return ResolveTenantSlowAsync(cancellationToken);
    }

    private async ValueTask<TenantId> ResolveTenantSlowAsync(CancellationToken cancellationToken)
    {
        var tenant = await _tenants.ResolveCurrentAsync(cancellationToken).ConfigureAwait(false);
        return RequireTenant(tenant);
    }

    private static TenantId RequireTenant(TenantId tenant) =>
        tenant.Value is null ? throw new LatticeTenantAccessDeniedException() : tenant;
}
