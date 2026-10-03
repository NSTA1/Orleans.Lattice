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
    private readonly AppTreeOwnershipLedger _ownership;
    private readonly IAppSource _source;
    private readonly string _clusterId;
    private readonly TimeProvider _time;

    /// <summary>Initializes a new <see cref="AppRegistry"/>.</summary>
    /// <param name="store">The record store.</param>
    /// <param name="authorizer">The <c>AppInstall</c> authorizer every transition consults.</param>
    /// <param name="clusterOptions">The cluster options, whose id is recorded in each install's isolation context.</param>
    /// <param name="ownership">The tree ownership ledger install and upgrade claim through.</param>
    /// <param name="source">The app source an install's manifest is resolved from to plan its claims.</param>
    /// <param name="timeProvider">The clock stamping transitions; defaults to <see cref="TimeProvider.System"/>.</param>
    /// <exception cref="ArgumentNullException">A required argument is <c>null</c>.</exception>
    public AppRegistry(
        IAppRegistryStore store,
        AppInstallAuthorizer authorizer,
        IOptions<ClusterOptions> clusterOptions,
        AppTreeOwnershipLedger ownership,
        IAppSource source,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(authorizer);
        ArgumentNullException.ThrowIfNull(clusterOptions);
        ArgumentNullException.ThrowIfNull(ownership);
        ArgumentNullException.ThrowIfNull(source);
        _store = store;
        _authorizer = authorizer;
        _ownership = ownership;
        _source = source;
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

        // Install and upgrade claim the manifest's trees in the ownership ledger. The plan is null
        // when the source cannot supply the exact version; activation, which needs the manifest
        // anyway, is authoritative and claims then.
        var claims = request is not null
            ? await PlanClaimsAsync(tenant, slug, request.Identity.Version, request.Identity.Provenance.Source, cancellationToken).ConfigureAwait(false)
            : null;
        var claimant = request is not null ? new AppTreeOwner(tenant, slug, request.Identity.Provenance.Publisher) : default;

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

            if (request?.ExpectedRevision is { } expectedRevision
                && (current is null || current.Revision != expectedRevision))
            {
                return AppRegistryTransitionResult.Rejected(
                    current,
                    AppRegistryTransitionError.ConcurrencyConflict,
                    "The app's install record changed after it was read; re-read the app and retry the transition.");
            }

            var decision = AppLifecycle.Evaluate(current, action);
            switch (decision.Kind)
            {
                case AppLifecycleDecisionKind.Reject:
                    return AppRegistryTransitionResult.Rejected(current, decision.Error, decision.Message!);
                case AppLifecycleDecisionKind.NoOp:
                    if (action == AppLifecycleAction.Uninstall)
                        await _ownership.ReleaseAdoptedAsync(AppTreeOwner.Of(current!), keep: null, cancellationToken).ConfigureAwait(false);
                    return AppRegistryTransitionResult.Success(current!, changed: false);
            }

            // A conflict visible before the write refuses the transition without recording anything.
            if (claims is not null && attempt == 1)
            {
                var conflicts = await _ownership.DescribeAsync(claimant, claims, cancellationToken).ConfigureAwait(false);
                if (conflicts.Count > 0)
                    return AppRegistryTransitionResult.Rejected(current, AppRegistryTransitionError.TreeOwnershipConflict, conflicts[0].Message);
            }

            var next = BuildNext(tenant, slug, current, action, decision.NextState, request, actor);
            if (await _store.TrySetAsync(key, next, read.Version, cancellationToken).ConfigureAwait(false))
            {
                if (claims is not null)
                    return await ClaimAsync(key, current, next, action, claimant, claims, cancellationToken).ConfigureAwait(false);
                if (action == AppLifecycleAction.Uninstall)
                    await _ownership.ReleaseAdoptedAsync(AppTreeOwner.Of(current!), keep: null, cancellationToken).ConfigureAwait(false);
                return AppRegistryTransitionResult.Success(next, changed: true);
            }
        }

        return AppRegistryTransitionResult.Rejected(
            current,
            AppRegistryTransitionError.ConcurrencyConflict,
            $"The app registry record was changed concurrently {MaxTransitionAttempts} times; retry the transition.");
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<AppTreeOwnershipConflict>> GetTreeOwnershipConflictsAsync(
        TenantId tenant,
        AppManifest manifest,
        AppProvenance provenance,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        ArgumentNullException.ThrowIfNull(manifest.Identity, nameof(manifest));
        ArgumentNullException.ThrowIfNull(provenance);
        AppRegistryTreeNames.RequireTenant(tenant);
        var slug = manifest.Identity.Slug;
        AppRegistryTreeNames.RequireSlug(slug);
        return _ownership.DescribeAsync(
            new AppTreeOwner(tenant, slug, provenance.Publisher),
            AppTreeOwnershipLedger.Plan(manifest, tenant),
            cancellationToken);
    }

    /// <summary>
    /// Resolves the exact version being consented to, from the source its provenance names, and plans its tree claims, or returns
    /// <c>null</c> when the source cannot supply that version.
    /// </summary>
    private async Task<AppTreeClaimPlan[]?> PlanClaimsAsync(
        TenantId tenant,
        AppSlug slug,
        AppVersion version,
        string? sourceKey,
        CancellationToken cancellationToken)
    {
        var resolved = await _source.ResolveFromAsync(slug, version, sourceKey, cancellationToken).ConfigureAwait(false);
        return resolved.IsResolved
            && resolved.Manifest is { } manifest
            && manifest.Identity.Slug == slug
            && manifest.Identity.Version == version
                ? AppTreeOwnershipLedger.Plan(manifest, tenant)
                : null;
    }

    /// <summary>
    /// Claims an applied install's or upgrade's trees. The record is written first, so a concurrent
    /// claimant always sees an installed owner behind a claim; the first claimant of a tree wins and
    /// the loser releases what it took and rolls its own record back. An upgrade then releases the
    /// adopted claims its new version no longer declares.
    /// </summary>
    private async Task<AppRegistryTransitionResult> ClaimAsync(
        string key,
        AppRegistryRecord? previous,
        AppRegistryRecord written,
        AppLifecycleAction action,
        AppTreeOwner claimant,
        AppTreeClaimPlan[] claims,
        CancellationToken cancellationToken)
    {
        var acquired = new List<string>();
        var conflict = await _ownership.ClaimAsync(claimant, written.Revision, claims, acquired, cancellationToken).ConfigureAwait(false);
        if (conflict is null)
        {
            if (action == AppLifecycleAction.Upgrade)
            {
                var keep = new HashSet<string>(StringComparer.Ordinal);
                foreach (var claim in claims)
                    keep.Add(claim.Key);
                await _ownership.ReleaseAdoptedAsync(claimant, keep, cancellationToken).ConfigureAwait(false);
            }

            return AppRegistryTransitionResult.Success(written, changed: true);
        }

        await _ownership.ReleaseAsync(claimant, acquired, cancellationToken).ConfigureAwait(false);
        var restored = await RollBackAsync(key, previous, written, cancellationToken).ConfigureAwait(false);
        return AppRegistryTransitionResult.Rejected(restored, AppRegistryTransitionError.TreeOwnershipConflict, conflict.Message);
    }

    /// <summary>
    /// Undoes a written install or upgrade that lost a concurrent ownership claim: restores the
    /// previous record (a fresh install becomes an uninstalled record), under a new revision so the
    /// revision never regresses. A record some other writer already moved on is left as it is.
    /// </summary>
    private async Task<AppRegistryRecord?> RollBackAsync(
        string key,
        AppRegistryRecord? previous,
        AppRegistryRecord written,
        CancellationToken cancellationToken)
    {
        for (var attempt = 1; attempt <= MaxTransitionAttempts; attempt++)
        {
            var read = await _store.GetAsync(key, cancellationToken).ConfigureAwait(false);
            if (read.Record is not { } stored || stored.Revision != written.Revision)
                return read.Record;

            var restored = previous is not null
                ? previous with { Revision = written.Revision + 1 }
                : written with
                {
                    State = AppRegistryLifecycleState.Uninstalled,
                    Revision = written.Revision + 1,
                    StateChangedAtUtc = _time.GetUtcNow(),
                };
            if (await _store.TrySetAsync(key, restored, read.Version, cancellationToken).ConfigureAwait(false))
                return restored;
        }

        return written;
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
                ConsentedBridge = request.BridgeConsent
                    ?? (action == AppLifecycleAction.Upgrade ? current?.ConsentedBridge : null),
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

            // D4 confinement: refuse at write time so an install or re-binding naming another tenant's
            // group fails at the call; activation refuses a stored one again (AppRoleBindingTenantMismatch).
            if (!AppRoleCompiler.IsBindableGroup(request.Tenant, binding.GroupId))
            {
                throw new ArgumentException(
                    $"Role '{binding.RoleName}' is bound to group '{binding.GroupId}', which is not a group of the installing tenant; "
                    + "a role may be bound only to a cluster group or one of the tenant's own groups.",
                    nameof(request));
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
