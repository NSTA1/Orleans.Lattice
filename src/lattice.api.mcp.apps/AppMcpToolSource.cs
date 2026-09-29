using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Mcp.Apps;

/// <summary>
/// The app tool surface: the <see cref="ILatticeApiMcpAppToolSource"/> that advertises
/// every enabled app's tools on the single Lattice MCP endpoint under the mandatory
/// <c>{slug}_{tool}</c> namespace, gated per tool through the shared access gate.
/// </summary>
/// <remarks>
/// <para>
/// <b>Per epoch.</b> Whenever the app registry projection's epoch advances, the source
/// rebuilds an <see cref="AppMcpToolCatalog"/>: for each enabled install whose ceiling is
/// pinned to its version it resolves the manifest through <see cref="IAppSource"/> at the
/// installed version and pairs the manifest's tool declarations with the tools every
/// <see cref="IAppMcpToolProvider"/> for the slug supplies. Activations are shared by every
/// tenant running the same version and reused across epochs once they succeed. Each
/// install's roles are compiled for its tenant once per epoch.
/// </para>
/// <para>
/// <b>Hard-fail pairing.</b> A declared tool with no implementation, an undeclared
/// implementation, or a duplicate local name fails that app's tool activation as a
/// whole: the app contributes no tools, and the failure is logged. This replaces the
/// discovery core's warn-and-skip for this surface only.
/// </para>
/// <para>
/// <b>Per session.</b> Under the caller's bridged credential and asserted active tenant
/// the source resolves the caller's tenant and subject, then offers each tool of the
/// tenant's installs whose declared role the caller holds (see <see cref="AppMcpRoleGate"/>
/// for the exact rule). Only prebuilt tool instances are selected; nothing is
/// re-materialised per session.
/// </para>
/// <para>
/// <b>Fail-closed.</b> Without a registry projection, an app source, an access gate (the
/// app-owned rules a role is held through are enforced nowhere without one) or any
/// provider the source offers nothing. A caller with no resolved group holds no role. A denied tenant resolution offers nothing. A
/// transient backend fault surfaces as a retryable discovery error rather than a falsely
/// narrow tool list; any other fault offers nothing and is logged.
/// </para>
/// </remarks>
internal sealed class AppMcpToolSource : ILatticeApiMcpAppToolSource
{
    private readonly Dictionary<AppSlug, IAppMcpToolProvider[]> _providersBySlug;
    private readonly ILogger<AppMcpToolSource> _logger;
    private readonly IAppRegistryProjection? _projection;
    private readonly IAppSource? _appSource;
    private readonly ILatticeAccessGate? _gate;
    private readonly ILatticeMembershipContext? _membership;
    private readonly ITenantContextResolver? _tenantResolver;
    private readonly ILatticeApiMcpActiveTenantBridge? _tenantBridge;
    private readonly SemaphoreSlim _rebuildLock = new(1, 1);
    private volatile AppMcpToolCatalog _catalog = AppMcpToolCatalog.Empty;

    /// <summary>Initializes a new <see cref="AppMcpToolSource"/>.</summary>
    /// <param name="providers">Every registered tool provider.</param>
    /// <param name="logger">The logger activation failures are reported to.</param>
    /// <param name="projection">The app registry projection, or <c>null</c> when none is registered.</param>
    /// <param name="appSource">The app source manifests resolve through, or <c>null</c> when none is registered.</param>
    /// <param name="gate">The shared access gate, or <c>null</c> when none is registered.</param>
    /// <param name="membership">The membership context callers resolve through, or <c>null</c>.</param>
    /// <param name="tenantResolver">The active-tenant resolver, or <c>null</c> (every caller is then in the default tenant).</param>
    /// <param name="tenantBridge">The MCP active-tenant bridge, or <c>null</c>.</param>
    public AppMcpToolSource(
        IEnumerable<IAppMcpToolProvider> providers,
        ILogger<AppMcpToolSource> logger,
        IAppRegistryProjection? projection = null,
        IAppSource? appSource = null,
        ILatticeAccessGate? gate = null,
        ILatticeMembershipContext? membership = null,
        ITenantContextResolver? tenantResolver = null,
        ILatticeApiMcpActiveTenantBridge? tenantBridge = null)
    {
        ArgumentNullException.ThrowIfNull(providers);
        ArgumentNullException.ThrowIfNull(logger);

        var grouped = new Dictionary<AppSlug, List<IAppMcpToolProvider>>();
        foreach (var provider in providers)
        {
            if (provider is null || provider.Slug.Value is null)
                continue;
            if (!grouped.TryGetValue(provider.Slug, out var list))
                grouped.Add(provider.Slug, list = []);
            list.Add(provider);
        }

        _providersBySlug = new Dictionary<AppSlug, IAppMcpToolProvider[]>(grouped.Count);
        foreach (var (slug, list) in grouped)
            _providersBySlug.Add(slug, list.ToArray());

        _logger = logger;
        _projection = projection;
        _appSource = appSource;
        _gate = gate;
        _membership = membership;
        _tenantResolver = tenantResolver;
        _tenantBridge = tenantBridge;
    }

    /// <summary>The most recently built catalog; <see cref="AppMcpToolCatalog.Empty"/> before the first build.</summary>
    internal AppMcpToolCatalog Catalog => _catalog;

    private bool CanServe => _providersBySlug.Count > 0 && _projection is not null && _appSource is not null && _gate is not null;

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<McpServerTool>> GetPermittedToolsAsync(
        HttpContext httpContext,
        LatticeCredential credential,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(httpContext);
        if (!CanServe)
            return [];

        try
        {
            var catalog = await GetCatalogAsync(cancellationToken).ConfigureAwait(false);
            var asserted = _tenantBridge?.Resolve(httpContext);
            using var credentialScope = LatticeCredentialContext.With(credential);
            using var tenantScope = asserted is null ? null : LatticeActiveTenantContext.With(asserted);

            var tenant = await ResolveTenantAsync(cancellationToken).ConfigureAwait(false);
            var apps = catalog.GetTenantApps(tenant);
            if (apps.Count == 0)
                return [];

            var subject = await LatticeAccessGateSubjectResolver.ResolveAsync(_membership, cancellationToken)
                .ConfigureAwait(false);
            List<McpServerTool>? permitted = null;
            for (var i = 0; i < apps.Count; i++)
            {
                var app = apps[i];
                var tools = app.Activation.Tools;

                // Memoise each role's verdict within the app: bit r of evaluated/held
                // records role r. Manifests with more than 32 roles are re-evaluated.
                uint evaluated = 0, held = 0;
                for (var j = 0; j < tools.Length; j++)
                {
                    var tool = tools[j];
                    var role = tool.RoleIndex;
                    bool allowed;
                    if (role < 32 && (evaluated & (1u << role)) != 0)
                    {
                        allowed = (held & (1u << role)) != 0;
                    }
                    else
                    {
                        allowed = AppMcpRoleGate.IsHeld(app.Roles[role], subject);
                        if (role < 32)
                        {
                            evaluated |= 1u << role;
                            if (allowed)
                                held |= 1u << role;
                        }
                    }

                    if (allowed)
                        (permitted ??= []).Add(tool);
                }
            }

            return permitted is null ? [] : permitted;
        }
        catch (LatticeTenantAccessDeniedException)
        {
            return [];
        }
        catch (Exception ex) when (ex is not OperationCanceledException
            && ex is not LatticeApiMcpDiscoveryUnavailableException
            && LatticeApiMcpDiscoveryFaultClassifier.IsTransientBackendFault(ex))
        {
            _logger.LogWarning(
                ex,
                "Resolving the caller's app MCP tools hit a transient backend fault; surfacing a retryable "
                + "discovery error rather than a falsely narrow tool set.");
            throw new LatticeApiMcpDiscoveryUnavailableException(
                "MCP tool discovery could not resolve the caller's app tools because a backend was transiently "
                + "unavailable. Retry the session.",
                ex);
        }
        catch (Exception ex) when (ex is not OperationCanceledException
            && ex is not LatticeApiMcpDiscoveryUnavailableException)
        {
            _logger.LogWarning(ex, "Resolving the caller's app MCP tools failed; offering no app tools.");
            return [];
        }
    }

    /// <summary>
    /// Re-runs the advertisement decision for <paramref name="tool"/> against the current
    /// registry snapshot, under the credential and active tenant the invocation stamped.
    /// </summary>
    /// <param name="tool">The tool being invoked.</param>
    /// <param name="cancellationToken">Cancels the check.</param>
    /// <returns>
    /// <c>true</c> when the caller's tenant still runs the tool's app version, the
    /// activation still carries the tool, and the caller holds its role.
    /// </returns>
    internal async ValueTask<bool> IsInvocationPermittedAsync(AppMcpNamespacedTool tool, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(tool);
        if (!CanServe)
            return false;

        try
        {
            var catalog = await GetCatalogAsync(cancellationToken).ConfigureAwait(false);
            var tenant = await ResolveTenantAsync(cancellationToken).ConfigureAwait(false);
            if (!catalog.TryGetApp(tenant, tool.Slug, out var app)
                || app.Activation.Version != tool.Version
                || !app.Activation.TryGetTool(tool.LocalName, out var current))
            {
                return false;
            }

            var subject = await LatticeAccessGateSubjectResolver.ResolveAsync(_membership, cancellationToken)
                .ConfigureAwait(false);
            return AppMcpRoleGate.IsHeld(app.Roles[current.RoleIndex], subject);
        }
        catch (LatticeTenantAccessDeniedException)
        {
            return false;
        }
    }

    /// <summary>Returns the catalog for the projection's current epoch, rebuilding it when the epoch advanced.</summary>
    /// <param name="cancellationToken">Cancels the wait or the rebuild.</param>
    /// <returns>The current catalog.</returns>
    internal async ValueTask<AppMcpToolCatalog> GetCatalogAsync(CancellationToken cancellationToken)
    {
        var projection = _projection!;
        if (projection.CurrentEpoch == 0)
            await projection.EnsureWarmAsync(cancellationToken).ConfigureAwait(false);

        var catalog = _catalog;
        var snapshot = projection.Current;
        if (catalog.Epoch == snapshot.Epoch)
            return catalog;

        await _rebuildLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            catalog = _catalog;
            snapshot = projection.Current;
            if (catalog.Epoch == snapshot.Epoch)
                return catalog;

            catalog = await BuildAsync(snapshot, catalog, cancellationToken).ConfigureAwait(false);
            _catalog = catalog;
            foreach (var failure in catalog.Failures)
            {
                _logger.LogWarning(
                    "MCP tools of app '{AppSlug}' version '{AppVersion}' were not activated: {Reason}",
                    failure.Slug.Value,
                    failure.Version.Value,
                    failure.Failure);
            }

            return catalog;
        }
        finally
        {
            _rebuildLock.Release();
        }
    }

    private async ValueTask<AppMcpToolCatalog> BuildAsync(
        CompiledAppRegistrySnapshot snapshot,
        AppMcpToolCatalog previous,
        CancellationToken cancellationToken)
    {
        var activations = new Dictionary<(AppSlug Slug, AppVersion Version, string Source), AppMcpToolActivation>();
        var byTenant = new Dictionary<TenantId, List<AppMcpInstalledApp>>();
        foreach (var record in snapshot.Records)
        {
            if (!AppRoleGrantEvaluator.IsEvaluated(record))
                continue;

            var key = (record.Slug, record.Version, record.Provenance.Source);
            if (!activations.TryGetValue(key, out var activation))
            {
                activation = previous.Activations.TryGetValue(key, out var prior) && prior.Succeeded
                    ? prior
                    : await ActivateAsync(record, cancellationToken).ConfigureAwait(false);
                activations.Add(key, activation);
            }

            if (!activation.Succeeded || activation.Tools.Length == 0)
                continue;

            var roles = AppRoleGrantEvaluator.CompileRoles(record, activation.Manifest!);

            if (!byTenant.TryGetValue(record.Tenant, out var apps))
                byTenant.Add(record.Tenant, apps = []);
            apps.Add(new AppMcpInstalledApp(record.Tenant, activation, roles));
        }

        var frozen = new Dictionary<TenantId, AppMcpInstalledApp[]>(byTenant.Count);
        foreach (var (tenant, apps) in byTenant)
            frozen.Add(tenant, apps.ToArray());

        return new AppMcpToolCatalog(snapshot.Epoch, frozen, activations);
    }

    private async ValueTask<AppMcpToolActivation> ActivateAsync(AppRegistryRecord record, CancellationToken cancellationToken)
    {
        var slug = record.Slug;
        var version = record.Version;
        AppSourceResult result;
        try
        {
            result = await _appSource!.ResolveInstalledAsync(record, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException
            && !LatticeApiMcpDiscoveryFaultClassifier.IsTransientBackendFault(ex))
        {
            return AppMcpToolActivation.Failed(slug, version, $"Resolving the manifest failed: {ex.GetType().Name}.");
        }

        if (!result.IsResolved || result.Manifest is not { } manifest)
        {
            var reason = result.Errors.Count > 0 ? result.Errors[0].Message : result.Status.ToString();
            return AppMcpToolActivation.Failed(slug, version, $"The manifest did not resolve ({result.Status}): {reason}");
        }

        if (manifest.Identity.Slug != slug || manifest.Identity.Version != version)
        {
            return AppMcpToolActivation.Failed(
                slug, version, $"The resolved manifest identifies as '{manifest.Identity.Slug}' version '{manifest.Identity.Version}'.");
        }

        var providers = _providersBySlug.TryGetValue(slug, out var registered) ? registered : [];
        return AppMcpToolActivation.Pair(manifest, providers, this);
    }

    private ValueTask<TenantId> ResolveTenantAsync(CancellationToken cancellationToken)
    {
        if (_tenantResolver is null)
            return new ValueTask<TenantId>(TenantId.Default);
        if (_tenantResolver.TryResolveCurrent(out var tenant))
            return new ValueTask<TenantId>(tenant);
        return _tenantResolver.ResolveCurrentAsync(cancellationToken);
    }
}
