using Microsoft.Extensions.Options;
using Orleans.Configuration;

namespace Orleans.Lattice.Apps;

/// <summary>
/// The default <see cref="IAppRegistry"/>: the lifecycle transitions over the reserved
/// <c>sys-app-registry</c> tree. Every transition authorizes the caller for
/// <see cref="LatticeOperation.AppInstall"/> first, then runs a bounded
/// optimistic-concurrency read-decide-write: read the record and its version, decide the
/// transition with the pure <see cref="AppLifecycle"/> table, and write the next record
/// conditionally on the version read. A competing writer that advanced the version makes
/// the write lose, so the transition is re-decided against the now-current record rather
/// than overwriting it.
/// </summary>
internal sealed class AppRegistry : IAppRegistry
{
    /// <summary>
    /// The bounded retry budget for one transition. A retry is needed only when a
    /// competing writer changed the same install between this call's read and write.
    /// </summary>
    internal const int MaxTransitionAttempts = 8;

    private readonly IAppRegistryStore _store;
    private readonly AppInstallAuthorizer _authorizer;
    private readonly string _clusterId;
    private readonly TimeProvider _time;

    /// <summary>Initializes a new <see cref="AppRegistry"/>.</summary>
    /// <param name="store">The record store.</param>
    /// <param name="authorizer">The <c>AppInstall</c> authorizer every transition consults.</param>
    /// <param name="clusterOptions">The cluster options, whose id is recorded in each install's isolation context.</param>
    /// <param name="timeProvider">The clock stamping transitions; defaults to <see cref="TimeProvider.System"/>.</param>
    /// <exception cref="ArgumentNullException">A required argument is <c>null</c>.</exception>
    public AppRegistry(
        IAppRegistryStore store,
        AppInstallAuthorizer authorizer,
        IOptions<ClusterOptions> clusterOptions,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(authorizer);
        ArgumentNullException.ThrowIfNull(clusterOptions);
        _store = store;
        _authorizer = authorizer;
        _clusterId = clusterOptions.Value.ClusterId ?? string.Empty;
        _time = timeProvider ?? TimeProvider.System;
    }

    /// <inheritdoc />
    public async Task<AppRegistryRecord?> GetAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default)
    {
        var key = AppRegistryTreeNames.ComposeKey(tenant, slug);
        var read = await _store.GetAsync(key, cancellationToken).ConfigureAwait(false);
        return read.Record;
    }

    /// <inheritdoc />
    public IAsyncEnumerable<AppRegistryRecord> ListAsync(CancellationToken cancellationToken = default) =>
        _store.ScanAsync(null, null, cancellationToken);

    /// <inheritdoc />
    public IAsyncEnumerable<AppRegistryRecord> ListForTenantAsync(TenantId tenant, CancellationToken cancellationToken = default)
    {
        // Validate eagerly, before the iterator is first advanced.
        var start = AppRegistryTreeNames.TenantRangeStart(tenant);
        var end = AppRegistryTreeNames.TenantRangeEnd(tenant);
        return _store.ScanAsync(start, end, cancellationToken);
    }

    /// <inheritdoc />
    public Task<AppRegistryTransitionResult> InstallAsync(AppRegistryInstallRequest request, CancellationToken cancellationToken = default)
    {
        ValidateRequest(request);
        return TransitionAsync(request.Tenant, request.Identity.Slug, AppLifecycleAction.Install, request, cancellationToken);
    }

    /// <inheritdoc />
    public Task<AppRegistryTransitionResult> UpgradeAsync(AppRegistryInstallRequest request, CancellationToken cancellationToken = default)
    {
        ValidateRequest(request);
        return TransitionAsync(request.Tenant, request.Identity.Slug, AppLifecycleAction.Upgrade, request, cancellationToken);
    }

    /// <inheritdoc />
    public Task<AppRegistryTransitionResult> EnableAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default) =>
        TransitionAsync(tenant, slug, AppLifecycleAction.Enable, request: null, cancellationToken);

    /// <inheritdoc />
    public Task<AppRegistryTransitionResult> DisableAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default) =>
        TransitionAsync(tenant, slug, AppLifecycleAction.Disable, request: null, cancellationToken);

    /// <inheritdoc />
    public Task<AppRegistryTransitionResult> UninstallAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken = default) =>
        TransitionAsync(tenant, slug, AppLifecycleAction.Uninstall, request: null, cancellationToken);

    private async Task<AppRegistryTransitionResult> TransitionAsync(
        TenantId tenant,
        AppSlug slug,
        AppLifecycleAction action,
        AppRegistryInstallRequest? request,
        CancellationToken cancellationToken)
    {
        // Programmer errors surface before authorization; authorization precedes every
        // storage access, so a denied caller learns nothing about the registry's state.
        var key = AppRegistryTreeNames.ComposeKey(tenant, slug);
        var actor = await _authorizer.AuthorizeAsync(cancellationToken).ConfigureAwait(false);

        AppRegistryRecord? current = null;
        for (var attempt = 1; attempt <= MaxTransitionAttempts; attempt++)
        {
            var read = await _store.GetAsync(key, cancellationToken).ConfigureAwait(false);
            current = read.Record;

            // Compared on every attempt against the record this attempt will write over, so a
            // competing upgrade that lands between the caller's read and this write is detected.
            if (request?.ExpectedVersion is { } expected
                && (current is null || current.State == AppRegistryLifecycleState.Uninstalled || current.Version != expected))
            {
                return AppRegistryTransitionResult.Rejected(
                    current,
                    AppRegistryTransitionError.ConcurrencyConflict,
                    $"The installed version is no longer '{expected}'; re-read the app and retry the transition.");
            }

            var decision = AppLifecycle.Evaluate(current, action);
            switch (decision.Kind)
            {
                case AppLifecycleDecisionKind.Reject:
                    return AppRegistryTransitionResult.Rejected(current, decision.Error, decision.Message!);
                case AppLifecycleDecisionKind.NoOp:
                    return AppRegistryTransitionResult.Success(current!, changed: false);
            }

            var next = BuildNext(tenant, slug, current, action, decision.NextState, request, actor);
            if (await _store.TrySetAsync(key, next, read.Version, cancellationToken).ConfigureAwait(false))
            {
                return AppRegistryTransitionResult.Success(next, changed: true);
            }
        }

        return AppRegistryTransitionResult.Rejected(
            current,
            AppRegistryTransitionError.ConcurrencyConflict,
            $"The app registry record was changed concurrently {MaxTransitionAttempts} times; retry the transition.");
    }

    /// <summary>
    /// Builds the record an applied transition writes. Install and upgrade take the
    /// identity, ceiling and bindings from the request and pin the ceiling to the
    /// request's version; enable, disable and uninstall carry them over unchanged.
    /// </summary>
    private AppRegistryRecord BuildNext(
        TenantId tenant,
        AppSlug slug,
        AppRegistryRecord? current,
        AppLifecycleAction action,
        AppRegistryLifecycleState nextState,
        AppRegistryInstallRequest? request,
        string? actor)
    {
        var now = _time.GetUtcNow();
        var revision = (current?.Revision ?? 0) + 1;
        var stateChangedAt = current is not null && current.State == nextState ? current.StateChangedAtUtc : now;

        if (action is AppLifecycleAction.Install or AppLifecycleAction.Upgrade)
        {
            var identity = request!.Identity;
            return new AppRegistryRecord
            {
                // An install records the isolation context afresh; an upgrade keeps the
                // context the install was recorded in.
                Isolation = action == AppLifecycleAction.Upgrade && current is not null
                    ? current.Isolation
                    : new AppIsolationContext { Tenant = tenant, ClusterId = _clusterId },
                Slug = slug,
                Version = identity.Version,
                Provenance = identity.Provenance,
                Ceiling = request.Ceiling,
                CeilingVersion = identity.Version,
                // Copied so a caller mutating its list after the call cannot alter the
                // record it was handed back.
                RoleBindings = request.RoleBindings.ToArray(),
                State = nextState,
                Revision = revision,
                InstalledAtUtc = action == AppLifecycleAction.Install ? now : current!.InstalledAtUtc,
                StateChangedAtUtc = stateChangedAt,
                ConsentedAtUtc = now,
                ConsentedBy = actor,
            };
        }

        return current! with
        {
            State = nextState,
            Revision = revision,
            StateChangedAtUtc = stateChangedAt,
        };
    }

    /// <summary>Rejects a malformed install or upgrade request.</summary>
    private static void ValidateRequest(AppRegistryInstallRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.Identity, nameof(request));
        ArgumentNullException.ThrowIfNull(request.Ceiling, nameof(request));
        ArgumentNullException.ThrowIfNull(request.RoleBindings, nameof(request));
        ArgumentNullException.ThrowIfNull(request.Identity.Provenance, nameof(request));
        ArgumentNullException.ThrowIfNull(request.Ceiling.ApprovedExceptionScopes, nameof(request));

        AppRegistryTreeNames.RequireTenant(request.Tenant);
        AppRegistryTreeNames.RequireSlug(request.Identity.Slug);
        if (request.Identity.Version.Value is null)
        {
            throw new ArgumentException("The install request must name an initialised app version.", nameof(request));
        }

        var bindings = request.RoleBindings;
        for (var i = 0; i < bindings.Count; i++)
        {
            var binding = bindings[i];
            if (binding is null || string.IsNullOrEmpty(binding.RoleName) || string.IsNullOrEmpty(binding.GroupId))
            {
                throw new ArgumentException(
                    $"Role binding {i} must name a non-empty role and membership group.", nameof(request));
            }

            for (var j = 0; j < i; j++)
            {
                if (string.Equals(bindings[j].RoleName, binding.RoleName, StringComparison.Ordinal))
                {
                    throw new ArgumentException(
                        $"Role '{binding.RoleName}' is bound more than once; each role binds to exactly one group.",
                        nameof(request));
                }
            }
        }
    }
}
