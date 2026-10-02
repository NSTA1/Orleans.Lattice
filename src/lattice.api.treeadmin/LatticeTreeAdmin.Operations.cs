using System.Globalization;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Operations;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// The accept-then-poll half of the tree-administration facade (#4124): the
/// <see cref="ILatticeTreeAdminOperations"/> verbs, and the start cores the
/// deprecated blocking verbs wrap. Every start composes the caller's names and
/// authorizes exactly as the blocking verb always has, on the same effective
/// tree, before the work is handed to the shared coordinator.
/// </summary>
/// <remarks>
/// A view rebuild or reconcile, a tag-index sweep and a WAL move each run as one
/// tracked grain call that relays its own progress to the operation; an
/// orphaned-leaf audit or repair is driven batch by batch from here, so each call
/// stays inside its bounded work budget and the shards walked are the progress.
/// </remarks>
internal sealed partial class LatticeTreeAdmin
{
    private static readonly IReadOnlyList<string> ViewRebuildPhases =
        [TreeAdminOperationPhases.Scanning, TreeAdminOperationPhases.Projecting, TreeAdminOperationPhases.Swapping];

    private static readonly IReadOnlyList<string> ViewReconcilePhases =
    [
        TreeAdminOperationPhases.Digesting,
        TreeAdminOperationPhases.Scanning,
        TreeAdminOperationPhases.Projecting,
        TreeAdminOperationPhases.Comparing,
        TreeAdminOperationPhases.Swapping,
    ];

    private static readonly IReadOnlyList<string> TagIndexReconcilePhases =
        [TreeAdminOperationPhases.Probing, TreeAdminOperationPhases.Repairing];

    private static readonly IReadOnlyList<string> WalMovePhases =
        [TreeAdminOperationPhases.Copying, TreeAdminOperationPhases.Verifying, TreeAdminOperationPhases.Flipping];

    private static readonly IReadOnlyList<string> OrphanedLeafPhases = [TreeAdminOperationPhases.Walking];

    /// <summary>A started view operation and the resolution its blocking twin reports from.</summary>
    private readonly record struct ViewStart(
        string TenantId,
        LatticeOperationLaunch<bool> Launch,
        string EffectiveViewName,
        (string SourceTreeId, bool IsAggregation, string? ProviderKey, string? ProjectionVersion) Resolved);

    /// <inheritdoc />
    public async Task<LatticeOperationHandle> StartViewRebuildAsync(
        string viewName, string? operationId = null, CancellationToken cancellationToken = default)
    {
        var start = await StartViewCoreAsync(rebuild: true, viewName, ResolveOperationId(operationId), cancellationToken)
            .ConfigureAwait(false);
        return ToHandle(start.Launch);
    }

    /// <inheritdoc />
    public async Task<LatticeOperationHandle> StartViewReconcileAsync(
        string viewName, string? operationId = null, CancellationToken cancellationToken = default)
    {
        var start = await StartViewCoreAsync(rebuild: false, viewName, ResolveOperationId(operationId), cancellationToken)
            .ConfigureAwait(false);
        return ToHandle(start.Launch);
    }

    /// <inheritdoc />
    public async Task<LatticeOperationHandle> StartTagIndexReconcileAsync(
        string indexName, string? operationId = null, CancellationToken cancellationToken = default)
    {
        var (_, launch, _) = await StartTagIndexReconcileCoreAsync(indexName, ResolveOperationId(operationId), cancellationToken)
            .ConfigureAwait(false);
        return ToHandle(launch);
    }

    /// <inheritdoc />
    public async Task<LatticeOperationHandle> StartWalMoveAsync(
        string treeId,
        int partition,
        string targetProviderKey,
        TreeWalMoveOptions? options = null,
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        var (_, launch) = await StartWalMoveCoreAsync(
            treeId, partition, targetProviderKey, options, ResolveOperationId(operationId), cancellationToken)
            .ConfigureAwait(false);
        return ToHandle(launch);
    }

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartOrphanedLeavesAuditAsync(
        string treeId, string? operationId = null, CancellationToken cancellationToken = default) =>
        StartOrphanedLeafPassAsync(repair: false, treeId, operationId, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartOrphanedLeavesRepairAsync(
        string treeId, string? operationId = null, CancellationToken cancellationToken = default) =>
        StartOrphanedLeafPassAsync(repair: true, treeId, operationId, cancellationToken);

    /// <inheritdoc />
    public async Task<LatticeOperationStatus?> GetOperationStatusAsync(
        string operationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        var runner = RequireRunner();

        var tenantId = await ResolveOperationTenantAsync(cancellationToken).ConfigureAwait(false);
        var record = await runner.GetAsync(tenantId, operationId).ConfigureAwait(false);
        return record is not null && await IsVisibleAsync(record, cancellationToken).ConfigureAwait(false)
            ? LatticeOperationMapping.ToStatus(record)
            : null;
    }

    /// <inheritdoc />
    public async Task<LatticeOperationPage> ListOperationsAsync(
        LatticeOperationListRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        var runner = RequireRunner();

        var tenantId = await ResolveOperationTenantAsync(cancellationToken).ConfigureAwait(false);
        var (records, next) = await runner
            .ListAsync(tenantId, TreeAdminOperationKinds.Prefix, request.PageToken, request.EffectivePageSize)
            .ConfigureAwait(false);

        var visible = new List<LatticeOperationStatus>(records.Count);
        foreach (var record in records)
        {
            if (await IsVisibleAsync(record, cancellationToken).ConfigureAwait(false))
            {
                visible.Add(LatticeOperationMapping.ToStatus(record));
            }
        }

        return new LatticeOperationPage { Operations = visible, NextPageToken = next };
    }

    /// <inheritdoc />
    public async Task<LatticeOperationStatus?> CancelOperationAsync(
        string operationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        var runner = RequireRunner();

        var tenantId = await ResolveOperationTenantAsync(cancellationToken).ConfigureAwait(false);
        var record = await runner.GetAsync(tenantId, operationId).ConfigureAwait(false);
        if (record is null || !await IsVisibleAsync(record, cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        // Visible, so its existence is no secret; cancelling needs the grant that
        // starting it needed, and a caller without it is refused outright.
        foreach (var treeId in record.TreeIds)
        {
            await AuthorizeStartGrantAsync(record.Kind, treeId, cancellationToken).ConfigureAwait(false);
        }

        var cancelled = await runner.RequestCancelAsync(tenantId, operationId).ConfigureAwait(false);
        return cancelled is null ? null : LatticeOperationMapping.ToStatus(cancelled);
    }

    private async Task<ViewStart> StartViewCoreAsync(
        bool rebuild, string viewName, string operationId, CancellationToken cancellationToken)
    {
        RequireViews();
        var runner = RequireRunner();
        var effectiveViewName = await EffectiveViewNameAsync(viewName, cancellationToken).ConfigureAwait(false);
        var resolved = await ResolveViewAsync(effectiveViewName, cancellationToken).ConfigureAwait(false);
        await _authorizer.AuthorizeTreeAdminAsync(resolved.SourceTreeId, cancellationToken).ConfigureAwait(false);

        var tenantId = await ResolveOperationTenantAsync(cancellationToken).ConfigureAwait(false);
        var ticket = LatticeOperationTicket.For(tenantId, operationId);
        var maintainer = _grainFactory.GetGrain<IViewMaintainerGrain>(effectiveViewName);
        var sourceTreeId = resolved.SourceTreeId;

        var launch = await runner.StartAsync<bool>(
            new LatticeOperationStart
            {
                TenantId = tenantId,
                OperationId = operationId,
                Kind = rebuild ? TreeAdminOperationKinds.ViewRebuild : TreeAdminOperationKinds.ViewReconcile,
                TreeIds = [sourceTreeId],
                Phases = rebuild ? ViewRebuildPhases : ViewReconcilePhases,
            },
            async (_, token) =>
            {
                if (rebuild)
                {
                    await maintainer.RebuildTrackedAsync(ticket, token).ConfigureAwait(false);
                    return true;
                }

                return await maintainer.ReconcileTrackedAsync(ticket, token).ConfigureAwait(false);
            },
            repaired => LatticeOperationCompletion.Succeeded(viewName, ViewResult(viewName, sourceTreeId, rebuild ? null : repaired)))
            .ConfigureAwait(false);

        return new ViewStart(tenantId, launch, effectiveViewName, resolved);
    }

    private async Task<(string TenantId, LatticeOperationLaunch<TagReconcileReport> Launch, string TreeId)> StartTagIndexReconcileCoreAsync(
        string indexName, string operationId, CancellationToken cancellationToken)
    {
        RequireTagIndex();
        ArgumentException.ThrowIfNullOrEmpty(indexName);
        var runner = RequireRunner();

        // Derived, not caller-supplied: see GetTagIndexStatusAsync - nothing here
        // is a tenant-local tree name, so nothing is composed.
        var treeId = ResolveTagIndexTreeId(indexName);
        await _authorizer.AuthorizeTreeAdminAsync(treeId, cancellationToken).ConfigureAwait(false);
        await ResolveTagIndexEntryAsync(treeId, cancellationToken).ConfigureAwait(false);

        var tenantId = await ResolveOperationTenantAsync(cancellationToken).ConfigureAwait(false);
        var ticket = LatticeOperationTicket.For(tenantId, operationId);
        var coordinator = _grainFactory.GetGrain<ITagIndexReconcileGrain>(indexName);

        var launch = await runner.StartAsync(
            new LatticeOperationStart
            {
                TenantId = tenantId,
                OperationId = operationId,
                Kind = TreeAdminOperationKinds.TagIndexReconcile,
                TreeIds = [treeId],
                Phases = TagIndexReconcilePhases,
            },
            (_, token) => coordinator.RunTrackedSweepAsync(ticket, token),
            report => LatticeOperationCompletion.Succeeded(indexName, new Dictionary<string, string>(StringComparer.Ordinal)
            {
                [TreeAdminOperationResultKeys.IndexName] = indexName,
                [TreeAdminOperationResultKeys.TreeId] = treeId,
                [TreeAdminOperationResultKeys.TreesCovered] = Invariant(report.TreesCovered),
                [TreeAdminOperationResultKeys.KeysScanned] = Invariant(report.KeysScanned),
                [TreeAdminOperationResultKeys.MembershipRowsScanned] = Invariant(report.MembershipRowsScanned),
                [TreeAdminOperationResultKeys.OrphanRowsRemoved] = Invariant(report.OrphanRowsRemoved),
            }))
            .ConfigureAwait(false);

        return (tenantId, launch, treeId);
    }

    private async Task<(string TenantId, LatticeOperationLaunch<TreeWalMoveReceipt> Launch)> StartWalMoveCoreAsync(
        string treeId,
        int partition,
        string targetProviderKey,
        TreeWalMoveOptions? options,
        string operationId,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(targetProviderKey);
        var runner = RequireRunner();
        var effectiveTreeId = await EffectiveTreeIdAsync(treeId, cancellationToken).ConfigureAwait(false);
        ThrowIfReserved(effectiveTreeId);
        await _authorizer.AuthorizeTreeLifecycleAsync(effectiveTreeId, cancellationToken).ConfigureAwait(false);

        var tenantId = await ResolveOperationTenantAsync(cancellationToken).ConfigureAwait(false);
        var ticket = LatticeOperationTicket.For(tenantId, operationId);
        var admin = _grainFactory.GetGrain<ILatticeAdminTrackedGrain>(LatticeConstants.AdminGrainKey);
        var coreOptions = ToWalMoveOptions(options);

        var launch = await runner.StartAsync(
            new LatticeOperationStart
            {
                TenantId = tenantId,
                OperationId = operationId,
                Kind = TreeAdminOperationKinds.WalMove,
                TreeIds = [effectiveTreeId],
                Phases = WalMovePhases,
            },
            async (_, token) =>
            {
                var receipt = await admin
                    .ExecuteWalMoveTrackedAsync(effectiveTreeId, partition, targetProviderKey, coreOptions, ticket, token)
                    .ConfigureAwait(false);
                return ToWalMoveReceipt(receipt, treeId);
            },
            static receipt => LatticeOperationCompletion.Succeeded(receipt.TreeId, WalMoveResult(receipt)))
            .ConfigureAwait(false);

        return (tenantId, launch);
    }

    private async Task<LatticeOperationHandle> StartOrphanedLeafPassAsync(
        bool repair, string treeId, string? operationId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        var id = ResolveOperationId(operationId);
        var runner = RequireRunner();
        var effectiveTreeId = await EffectiveTreeIdAsync(treeId, cancellationToken).ConfigureAwait(false);
        if (repair)
        {
            ThrowIfReserved(effectiveTreeId);
            await _authorizer.AuthorizeTreeLifecycleAsync(effectiveTreeId, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await _authorizer.AuthorizeTreeReadAsync(effectiveTreeId, cancellationToken).ConfigureAwait(false);
        }

        var tenantId = await ResolveOperationTenantAsync(cancellationToken).ConfigureAwait(false);
        var lattice = _grainFactory.GetGrain<ILattice>(effectiveTreeId);

        var launch = await runner.StartAsync(
            new LatticeOperationStart
            {
                TenantId = tenantId,
                OperationId = id,
                Kind = repair ? TreeAdminOperationKinds.OrphanedLeavesRepair : TreeAdminOperationKinds.OrphanedLeavesAudit,
                TreeIds = [effectiveTreeId],
                Phases = OrphanedLeafPhases,
            },
            (progress, token) => RunOrphanedLeafPassAsync(lattice, repair, progress, token),
            totals => LatticeOperationCompletion.Succeeded(treeId, totals.ToResult(treeId, repair)))
            .ConfigureAwait(false);

        return ToHandle(launch);
    }

    /// <summary>
    /// Drives an orphaned-leaf pass to the end of the tree one bounded batch at a
    /// time, reporting the physical shards whose chains are fully walked. Each batch
    /// is a separate call inside its own work budget, so no call outruns a response
    /// deadline however damaged the tree.
    /// </summary>
    private static async Task<OrphanedLeafPassTotals> RunOrphanedLeafPassAsync(
        ILattice lattice, bool repair, ILatticeOperationProgress progress, CancellationToken cancellationToken)
    {
        // Forced: the walk reports progress per physical shard, so a worker activation's
        // pre-reshard cached map would name shards the tree no longer has (#4180).
        var routing = await lattice.GetRoutingAsync(forceRefresh: true, cancellationToken).ConfigureAwait(false);
        var shards = routing.Map.GetPhysicalShardIndices().ToArray();
        Array.Sort(shards);

        var totals = new OrphanedLeafPassTotals();
        string? resumeFrom = null;
        do
        {
            await progress.ReportAsync(
                TreeAdminOperationPhases.Walking,
                ShardsWalked(shards, resumeFrom),
                shards.Length,
                TreeAdminOperationUnits.Shards).ConfigureAwait(false);

            var report = repair
                ? await lattice.RepairOrphanedLeavesAsync(resumeFrom, cancellationToken).ConfigureAwait(false)
                : await lattice.InspectOrphanedLeavesAsync(resumeFrom, cancellationToken).ConfigureAwait(false);
            totals.Add(report);
            resumeFrom = report.ResumeFrom;
        }
        while (resumeFrom is not null);

        await progress.ReportAsync(
            TreeAdminOperationPhases.Walking, shards.Length, shards.Length, TreeAdminOperationUnits.Shards)
            .ConfigureAwait(false);
        return totals;
    }

    /// <summary>
    /// The physical shards a pass has walked to the end of, read from its resume
    /// cursor: every shard below the one the cursor names. Clamped, so a reshard
    /// between batches cannot report more shards than the routing read at the start.
    /// </summary>
    internal static long ShardsWalked(int[] sortedShards, string? resumeFrom)
    {
        if (resumeFrom is null)
        {
            return 0;
        }

        var cursor = OrphanedLeafPassCursor.Decode(resumeFrom);
        var walked = 0L;
        foreach (var shard in sortedShards)
        {
            if (shard < cursor.ShardIndex)
            {
                walked++;
            }
        }

        return walked;
    }

    /// <summary>
    /// The deprecated blocking verbs' wait: awaits the in-process work this call
    /// started and returns the engine's own result or rethrows its own exception.
    /// Cancelling <paramref name="cancellationToken"/> cancels the operation, as
    /// cancelling a blocking call always cancelled its work.
    /// </summary>
    private async Task<TResult> AwaitOperationAsync<TResult>(
        string tenantId, LatticeOperationLaunch<TResult> launch, CancellationToken cancellationToken)
    {
        // A wrapper always starts under a freshly generated id, so it always owns
        // the work; a launch without it would mean the id collided.
        var completion = launch.Completion ?? throw new InvalidOperationException(
            $"Operation '{launch.Record.OperationId}' already existed, so there is no work to wait for.");

        try
        {
            return await completion.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested && !completion.IsCompleted)
        {
            await RequireRunner().RequestCancelAsync(tenantId, launch.Record.OperationId).ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Whether the caller may see an operation: it must be a tree-administration
    /// kind and the caller must hold the read grant over every tree it targets.
    /// Anything else is reported as not found, so an operation's existence is never
    /// disclosed.
    /// </summary>
    private async ValueTask<bool> IsVisibleAsync(LatticeOperationRecord record, CancellationToken cancellationToken)
    {
        if (!record.Kind.StartsWith(TreeAdminOperationKinds.Prefix, StringComparison.Ordinal) || record.TreeIds.Count == 0)
        {
            return false;
        }

        foreach (var treeId in record.TreeIds)
        {
            if (!await _authorizer.IsTreeReadAuthorizedAsync(treeId, cancellationToken).ConfigureAwait(false))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Authorizes, fail-closed, the grant starting an operation of <paramref name="kind"/> needed.</summary>
    private ValueTask AuthorizeStartGrantAsync(string kind, string treeId, CancellationToken cancellationToken) => kind switch
    {
        TreeAdminOperationKinds.WalMove or TreeAdminOperationKinds.OrphanedLeavesRepair =>
            _authorizer.AuthorizeTreeLifecycleAsync(treeId, cancellationToken),
        TreeAdminOperationKinds.OrphanedLeavesAudit => _authorizer.AuthorizeTreeReadAsync(treeId, cancellationToken),
        _ => _authorizer.AuthorizeTreeAdminAsync(treeId, cancellationToken),
    };

    private async ValueTask<string> ResolveOperationTenantAsync(CancellationToken cancellationToken)
    {
        var tenant = _tenantResolver.TryResolveCurrent(out var warm)
            ? warm
            : await _tenantResolver.ResolveCurrentAsync(cancellationToken).ConfigureAwait(false);
        return tenant.Value ?? throw new LatticeTenantAccessDeniedException();
    }

    private LatticeOperationRunner RequireRunner() => _operationRunner ?? throw new InvalidOperationException(
        "The accept-then-poll tree-administration operations require the core operation coordinator, which AddLattice registers.");

    private static string ResolveOperationId(string? operationId)
    {
        if (operationId is null)
        {
            return LatticeOperationKey.NewId();
        }

        LatticeOperationKey.ThrowIfInvalid(operationId, nameof(operationId));
        return operationId;
    }

    private static LatticeOperationHandle ToHandle<TResult>(LatticeOperationLaunch<TResult> launch) =>
        LatticeOperationMapping.ToHandle(launch.Record, created: launch.Completion is not null);

    private static Dictionary<string, string> ViewResult(string viewName, string sourceTreeId, bool? driftRepaired)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [TreeAdminOperationResultKeys.ViewName] = viewName,
            [TreeAdminOperationResultKeys.SourceTreeId] = sourceTreeId,
        };
        if (driftRepaired is { } repaired)
        {
            result[TreeAdminOperationResultKeys.DriftRepaired] = repaired ? "true" : "false";
        }

        return result;
    }

    private static Dictionary<string, string> WalMoveResult(TreeWalMoveReceipt receipt) =>
        new(StringComparer.Ordinal)
        {
            [TreeAdminOperationResultKeys.TreeId] = receipt.TreeId,
            [TreeAdminOperationResultKeys.Partition] = Invariant(receipt.Partition),
            [TreeAdminOperationResultKeys.FromProviderKey] = receipt.FromProviderKey,
            [TreeAdminOperationResultKeys.ToProviderKey] = receipt.ToProviderKey,
            [TreeAdminOperationResultKeys.Outcome] = receipt.Outcome.ToString(),
            [TreeAdminOperationResultKeys.PreviousPlacementVersion] = Invariant(receipt.PreviousPlacementVersion),
            [TreeAdminOperationResultKeys.NewPlacementVersion] = Invariant(receipt.NewPlacementVersion),
            [TreeAdminOperationResultKeys.CopiedFromOffset] = Invariant(receipt.CopiedFromOffset),
            [TreeAdminOperationResultKeys.CopiedThroughOffset] = Invariant(receipt.CopiedThroughOffset),
            [TreeAdminOperationResultKeys.SourceHighestOffset] = Invariant(receipt.SourceHighestOffset),
            [TreeAdminOperationResultKeys.TargetHighestOffset] = Invariant(receipt.TargetHighestOffset),
            [TreeAdminOperationResultKeys.SourceRetained] = receipt.SourceRetained ? "true" : "false",
        };

    private static string Invariant(long value) => value.ToString(CultureInfo.InvariantCulture);

    /// <summary>The running totals of an orphaned-leaf pass, summed across its batches.</summary>
    private sealed class OrphanedLeafPassTotals
    {
        private long _leavesWalked;
        private long _orphanedLeaves;
        private long _repaired;
        private long _repairable;
        private long _refused;
        private long _gaps;

        public void Add(OrphanedLeafRepairReport report)
        {
            _leavesWalked += report.LeavesWalked;
            _gaps += report.Gaps?.Count ?? 0;
            if (report.Findings is not { } findings)
            {
                return;
            }

            foreach (var finding in findings)
            {
                _orphanedLeaves++;
                switch (finding.Disposition)
                {
                    case OrphanedLeafDisposition.Repaired:
                        _repaired++;
                        break;
                    case OrphanedLeafDisposition.Repairable:
                        _repairable++;
                        break;
                    default:
                        _refused++;
                        break;
                }
            }
        }

        public Dictionary<string, string> ToResult(string treeId, bool repair)
        {
            var result = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                [TreeAdminOperationResultKeys.TreeId] = treeId,
                [TreeAdminOperationResultKeys.LeavesWalked] = Invariant(_leavesWalked),
                [TreeAdminOperationResultKeys.OrphanedLeaves] = Invariant(_orphanedLeaves),
                [TreeAdminOperationResultKeys.Refused] = Invariant(_refused),
                [TreeAdminOperationResultKeys.Gaps] = Invariant(_gaps),
            };
            result[repair ? TreeAdminOperationResultKeys.Repaired : TreeAdminOperationResultKeys.Repairable] =
                Invariant(repair ? _repaired : _repairable);
            return result;
        }
    }
}
