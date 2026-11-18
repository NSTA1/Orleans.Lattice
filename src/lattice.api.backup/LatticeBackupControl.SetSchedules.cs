using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup;

/// <summary>
/// The backup-set schedule verbs of the facade (issue #6067). A schedule's name is
/// tenant local and composed under the caller's tenant; its trees are composed the
/// same way and authorized fail-closed - every tree it captures, plus the
/// tenant-wide (or cluster-wide) scope when it also captures trees created later -
/// before the schedule's grain is reached, because a reminder-driven run has no
/// caller to authorize.
/// </summary>
internal sealed partial class LatticeBackupControl
{
    // The authorization sentinels a rule uses to stand for every tree: the whole
    // cluster ("*"), or a whole tenant ("t/{tenant}/*"). They mirror
    // Orleans.Lattice.Auth LatticeScope.ClusterWide / TenantWide, which the core
    // reserves as uncreatable tree ids.
    private const string ClusterWideSentinel = "*";
    private const string TenantWideSentinelSuffix = "*";

    /// <inheritdoc />
    public async Task<BackupSetScheduleStatus> ScheduleBackupSetAsync(
        LatticeBackupSetScheduleRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        // A request decoded from the wire (or copied with `with`) never ran its
        // constructor; rebuild it through the constructor so every branch below
        // sees a validated request.
        request = new LatticeBackupSetScheduleRequest(
            request.Name, request.Membership, request.Scopes, request.Interval,
            request.RetentionKeepLast, request.RetentionMaxAge, request.CrossTreeConsistent,
            request.IncludeNewTrees, request.ExcludedTreePrefixes);
        var prefix = await ResolveTenantTreePrefixAsync(cancellationToken).ConfigureAwait(false);

        // Re-registering a name replaces that schedule, and its next run applies
        // the new retention to the sets it already retains - deleting backups of
        // trees the new request may not name. The caller must therefore hold the
        // grant over the existing schedule (its trees and every retained set's
        // members) as well as over the new request.
        var existing = await SetScheduleGrain(prefix, request.Name).GetStatusAsync().ConfigureAwait(false);
        if (existing is not null)
        {
            foreach (var scope in AuthorizationScopes(existing, prefix))
            {
                await _authorizer.AuthorizeBackupAsync(scope, cancellationToken).ConfigureAwait(false);
            }
        }

        LatticeBackupSetScheduleRequest effective;
        BackupSetScheduleTarget target;
        if (request.Membership == BackupSetMembershipMode.Explicit)
        {
            var scopes = new List<BackupScopeSelector>(request.Scopes.Count);
            foreach (var scope in request.Scopes)
            {
                var composed = await ResolveEffectiveScopeAsync(scope, cancellationToken).ConfigureAwait(false);
                await _authorizer.AuthorizeBackupAsync(composed, cancellationToken).ConfigureAwait(false);
                scopes.Add(composed);
            }

            // Rebuilt through the constructor so the one-scope-per-tree guard
            // re-runs over the composed ids.
            effective = new LatticeBackupSetScheduleRequest(
                request.Name, request.Membership, scopes, request.Interval,
                request.RetentionKeepLast, request.RetentionMaxAge, request.CrossTreeConsistent);
            target = new BackupSetScheduleTarget(prefix, scopes, Array.Empty<string>());
        }
        else
        {
            var excluded = request.ExcludedTreePrefixes.Select(p => (prefix ?? string.Empty) + p).ToList();
            var resolved = await BackupSetMembership.ResolveAllTreesAsync(_grainFactory, prefix, excluded, cancellationToken)
                .ConfigureAwait(false);
            if (request.IncludeNewTrees)
            {
                // Trees created later are captured with no caller to authorize them
                // against, so the grant must already cover every tree the tenant (or
                // cluster) can hold.
                await _authorizer.AuthorizeBackupAsync(WideScope(prefix), cancellationToken).ConfigureAwait(false);
            }

            foreach (var scope in resolved)
            {
                await _authorizer.AuthorizeBackupAsync(scope, cancellationToken).ConfigureAwait(false);
            }

            effective = request;
            target = new BackupSetScheduleTarget(
                prefix, request.IncludeNewTrees ? Array.Empty<BackupScopeSelector>() : resolved, excluded);
        }

        return await SetScheduleGrain(prefix, request.Name)
            .ScheduleAsync(effective, target)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<bool> CancelBackupSetScheduleAsync(
        string name,
        CancellationToken cancellationToken = default)
    {
        var (grain, status, _) = await AuthorizeExistingScheduleAsync(name, cancellationToken).ConfigureAwait(false);
        return status is not null && await grain.PauseAsync().ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<bool> RemoveBackupSetScheduleAsync(
        string name,
        bool deleteRetainedSets,
        CancellationToken cancellationToken = default)
    {
        var (grain, status, _) = await AuthorizeExistingScheduleAsync(name, cancellationToken).ConfigureAwait(false);
        return status is not null && await grain.RemoveAsync(deleteRetainedSets).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<BackupSetScheduleStatus?> GetBackupSetScheduleStatusAsync(
        string name,
        CancellationToken cancellationToken = default)
    {
        var (_, status, _) = await AuthorizeExistingScheduleAsync(name, cancellationToken).ConfigureAwait(false);
        return status;
    }

    /// <inheritdoc />
    public async Task<BackupSetScheduleRunProgress?> GetBackupSetScheduleProgressAsync(
        string name,
        CancellationToken cancellationToken = default)
    {
        var (_, status, _) = await AuthorizeExistingScheduleAsync(name, cancellationToken).ConfigureAwait(false);
        if (status is null)
        {
            throw new KeyNotFoundException($"No backup-set schedule is named '{name}'.");
        }

        return status.CurrentRun;
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<BackupSetScheduleStatus>> ListBackupSetSchedulesAsync(
        CancellationToken cancellationToken = default)
    {
        var prefix = await ResolveTenantTreePrefixAsync(cancellationToken).ConfigureAwait(false);
        var registry = _grainFactory.GetGrain<IBackupSetScheduleRegistryGrain>(IBackupSetScheduleRegistryGrain.SingletonKey);
        var listed = new List<BackupSetScheduleStatus>();
        foreach (var (scheduleKey, effectiveName) in await registry.ListAsync().ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!IsInTenant(effectiveName, prefix))
            {
                continue;
            }

            var status = await _grainFactory.GetGrain<ILatticeBackupSetSchedulerGrain>(scheduleKey)
                .GetStatusAsync().ConfigureAwait(false);
            if (status is not null && await IsScheduleAuthorizedAsync(status, prefix, cancellationToken).ConfigureAwait(false))
            {
                listed.Add(status);
            }
        }

        return listed;
    }

    /// <inheritdoc />
    public async Task<bool> RunBackupSetScheduleNowAsync(
        string name,
        CancellationToken cancellationToken = default)
    {
        var (grain, status, _) = await AuthorizeExistingScheduleAsync(name, cancellationToken).ConfigureAwait(false);
        if (status is null)
        {
            throw new KeyNotFoundException($"No backup-set schedule is named '{name}'.");
        }

        return await grain.RunNowAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Resolves the named schedule in the caller's tenant and authorizes the caller
    /// over it fail-closed, returning a <see langword="null"/> status when no
    /// schedule has the name.
    /// </summary>
    private async Task<(ILatticeBackupSetSchedulerGrain Grain, BackupSetScheduleStatus? Status, string? Prefix)> AuthorizeExistingScheduleAsync(
        string name,
        CancellationToken cancellationToken)
    {
        LatticeBackupSetScheduleRequest.ValidateName(name);
        var prefix = await ResolveTenantTreePrefixAsync(cancellationToken).ConfigureAwait(false);
        var grain = SetScheduleGrain(prefix, name);
        var status = await grain.GetStatusAsync().ConfigureAwait(false);
        if (status is not null)
        {
            foreach (var scope in AuthorizationScopes(status, prefix))
            {
                await _authorizer.AuthorizeBackupAsync(scope, cancellationToken).ConfigureAwait(false);
            }
        }

        return (grain, status, prefix);
    }

    private async ValueTask<bool> IsScheduleAuthorizedAsync(
        BackupSetScheduleStatus status, string? prefix, CancellationToken cancellationToken)
    {
        foreach (var scope in AuthorizationScopes(status, prefix))
        {
            if (!await IsBackupAuthorizedAsync(scope, cancellationToken).ConfigureAwait(false))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// The scopes a caller must be authorized over to act on a schedule: every
    /// scope it captures, the tenant-wide (or cluster-wide) scope when it
    /// re-resolves its trees, and every member of every set it retains - which can
    /// name trees an earlier registration captured and the current one does not,
    /// and which retention or removal may delete. Read back from the schedule's own
    /// state, which holds effective ids, so none is re-composed; each tree is
    /// yielded once.
    /// </summary>
    private static IEnumerable<BackupScopeSelector> AuthorizationScopes(BackupSetScheduleStatus status, string? prefix)
    {
        if (status.Request.IncludeNewTrees)
        {
            yield return WideScope(prefix);
        }

        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var scope in status.MemberScopes)
        {
            if (seen.Add(scope.TreeId))
            {
                yield return scope;
            }
        }

        foreach (var set in status.RetainedSets)
        {
            foreach (var member in set.Members)
            {
                if (seen.Add(member.TreeId))
                {
                    yield return member.Scope;
                }
            }
        }
    }

    private static BackupScopeSelector WideScope(string? prefix) =>
        BackupScopeSelector.WholeTree(prefix is null ? ClusterWideSentinel : prefix + TenantWideSentinelSuffix);

    private ILatticeBackupSetSchedulerGrain SetScheduleGrain(string? prefix, string name) =>
        _grainFactory.GetGrain<ILatticeBackupSetSchedulerGrain>(BackupSetScheduleKey.For((prefix ?? string.Empty) + name));

    /// <summary>
    /// The caller's tenant namespace prefix (<c>t/{tenant}/</c>), or <see langword="null"/>
    /// for the default tenant, whose trees and schedules carry no prefix.
    /// </summary>
    private async ValueTask<string?> ResolveTenantTreePrefixAsync(CancellationToken cancellationToken)
    {
        var tenant = await ResolveActiveTenantAsync(cancellationToken).ConfigureAwait(false);
        return tenant.IsDefault ? null : LatticeTenantTrees.ComposePrefix(tenant);
    }

    private static bool IsInTenant(string effectiveName, string? prefix) =>
        prefix is null
            ? !LatticeTenantTrees.IsTenantScoped(effectiveName)
            : effectiveName.StartsWith(prefix, StringComparison.Ordinal)
                && effectiveName.IndexOf('/', prefix.Length) < 0;
}
