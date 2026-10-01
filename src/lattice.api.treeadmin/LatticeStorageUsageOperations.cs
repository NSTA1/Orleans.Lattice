using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Operations;

namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// Default <see cref="ILatticeStorageUsageOperations"/> (#4126): runs the deep
/// storage-usage re-measure on the shared <see cref="LatticeOperationRunner"/>
/// instead of inside one request, so a caller never holds a call open across the
/// whole leaf walk. It measures every registered tree through the same per-tree
/// aggregator the cluster roll-up uses, with the same bounded concurrency, and
/// reports one unit per tree measured.
/// </summary>
/// <remarks>
/// A tree that fails to answer is reported as a partial reading rather than
/// failing the refresh, exactly as the blocking roll-up does; only a cancellation
/// of the operation stops it. Because the work runs in the background, no
/// wall-clock budget truncates it.
/// </remarks>
internal sealed class LatticeStorageUsageOperations : ILatticeStorageUsageOperations
{
    private static readonly IReadOnlyList<string> Phases = [StorageUsageRefreshOperation.MeasuringPhase];

    private readonly LatticeOperationRunner _runner;
    private readonly IGrainFactory _grainFactory;
    private readonly TreeAdminAccessAuthorizer _authorizer;
    private readonly ITenantContextResolver _tenantResolver;
    private readonly IOptionsMonitor<LatticeOptions> _options;
    private readonly ILogger<LatticeStorageUsageOperations> _logger;

    /// <summary>Initializes a new <see cref="LatticeStorageUsageOperations"/>.</summary>
    /// <param name="runner">The shared operation coordinator. Must not be <c>null</c>.</param>
    /// <param name="grainFactory">The grain factory. Must not be <c>null</c>.</param>
    /// <param name="authorizer">The fail-closed tree-admin authorization seam. Must not be <c>null</c>.</param>
    /// <param name="tenantResolver">The active-tenant context resolver. Must not be <c>null</c>.</param>
    /// <param name="options">The core options, for the per-tree fan-out bound. Must not be <c>null</c>.</param>
    /// <param name="logger">The logger. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">A dependency is <c>null</c>.</exception>
    public LatticeStorageUsageOperations(
        LatticeOperationRunner runner,
        IGrainFactory grainFactory,
        TreeAdminAccessAuthorizer authorizer,
        ITenantContextResolver tenantResolver,
        IOptionsMonitor<LatticeOptions> options,
        ILogger<LatticeStorageUsageOperations> logger)
    {
        ArgumentNullException.ThrowIfNull(runner);
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(authorizer);
        ArgumentNullException.ThrowIfNull(tenantResolver);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);
        _runner = runner;
        _grainFactory = grainFactory;
        _authorizer = authorizer;
        _tenantResolver = tenantResolver;
        _options = options;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task<LatticeOperationHandle> StartStorageUsageRefreshAsync(
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        var id = ResolveOperationId(operationId);
        await _authorizer.AuthorizeClusterTelemetryAsync(cancellationToken).ConfigureAwait(false);

        var tenant = await ResolveActiveTenantAsync(cancellationToken).ConfigureAwait(false);
        var launch = await _runner.StartAsync(
            new LatticeOperationStart
            {
                TenantId = tenant,
                OperationId = id,
                Kind = StorageUsageRefreshOperation.Kind,
                Phases = Phases,
            },
            RefreshAsync,
            static summary => LatticeOperationCompletion.Succeeded(
                null, StorageUsageRefreshResults.ToResultMap(summary))).ConfigureAwait(false);

        return LatticeOperationMapping.ToHandle(launch.Record, created: launch.Completion is not null);
    }

    /// <inheritdoc />
    public async Task<LatticeOperationStatus?> GetOperationStatusAsync(
        string operationId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);

        var tenant = await ResolveActiveTenantAsync(cancellationToken).ConfigureAwait(false);
        var record = await _runner.GetAsync(tenant, operationId).ConfigureAwait(false);
        return record is not null && await IsVisibleAsync(record, cancellationToken).ConfigureAwait(false)
            ? LatticeOperationMapping.ToStatus(record)
            : null;
    }

    /// <inheritdoc />
    public async Task<LatticeOperationPage> ListOperationsAsync(
        LatticeOperationListRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var tenant = await ResolveActiveTenantAsync(cancellationToken).ConfigureAwait(false);
        var (records, next) = await _runner
            .ListAsync(tenant, StorageUsageRefreshOperation.Kind, request.PageToken, request.EffectivePageSize)
            .ConfigureAwait(false);

        // Every listed record shares one visibility test (the kind and the caller's
        // cluster telemetry authority), so the gate is consulted once, not per record.
        if (records.Count == 0 || !await IsTelemetryAuthorizedAsync(cancellationToken).ConfigureAwait(false))
        {
            return new LatticeOperationPage { Operations = [], NextPageToken = next };
        }

        var visible = new List<LatticeOperationStatus>(records.Count);
        foreach (var record in records)
        {
            if (IsRefreshKind(record))
            {
                visible.Add(LatticeOperationMapping.ToStatus(record));
            }
        }

        return new LatticeOperationPage { Operations = visible, NextPageToken = next };
    }

    /// <inheritdoc />
    public async Task<LatticeOperationStatus?> CancelOperationAsync(
        string operationId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);

        var tenant = await ResolveActiveTenantAsync(cancellationToken).ConfigureAwait(false);
        var record = await _runner.GetAsync(tenant, operationId).ConfigureAwait(false);
        if (record is null || !await IsVisibleAsync(record, cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        var cancelled = await _runner.RequestCancelAsync(tenant, operationId).ConfigureAwait(false);
        return cancelled is null ? null : LatticeOperationMapping.ToStatus(cancelled);
    }

    /// <summary>
    /// The refresh itself: measures every registered tree with its cache bypassed,
    /// one unit per tree, under the cluster roll-up's concurrency bound.
    /// </summary>
    private async Task<ClusterStorageUsageSummary> RefreshAsync(
        ILatticeOperationProgress progress,
        CancellationToken cancellationToken)
    {
        var treeIds = await _grainFactory.GetLatticeRegistry().GetAllTreeIdsAsync().ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();

        var total = treeIds.Count;
        await progress.ReportAsync(StorageUsageRefreshOperation.MeasuringPhase, 0, total, StorageUsageRefreshOperation.TreesUnit)
            .ConfigureAwait(false);

        var measured = 0;
        var reports = await BoundedFanOut.RunAsync(
            total,
            _options.Get(Options.DefaultName).MaxConcurrentStorageUsageTrees,
            async slot =>
            {
                var report = await MeasureAsync(treeIds[slot], cancellationToken).ConfigureAwait(false);
                await progress.ReportAsync(
                    StorageUsageRefreshOperation.MeasuringPhase,
                    Interlocked.Increment(ref measured),
                    total,
                    StorageUsageRefreshOperation.TreesUnit).ConfigureAwait(false);
                return report;
            },
            cancellationToken).ConfigureAwait(false);

        cancellationToken.ThrowIfCancellationRequested();

        // Only the cluster totals are recorded (the result map stays small however
        // many trees there are); the refreshed per-tree figures are read back with
        // the cheap roll-up, so no per-tree rows are built here.
        long wal = 0, snapshot = 0, leafState = 0, totalBytes = 0;
        var partial = false;
        foreach (var report in reports)
        {
            wal += report.WalRetainedBytes;
            snapshot += report.SnapshotBytes;
            leafState += report.LeafStateBytes;
            totalBytes += report.TotalBytes;
            partial |= report.Partial;
        }

        return new ClusterStorageUsageSummary
        {
            TreeCount = reports.Length,
            WalRetainedBytes = wal,
            SnapshotBytes = snapshot,
            LeafStateBytes = leafState,
            TotalBytes = totalBytes,
            Partial = partial,
            Deep = true,
            SampledAt = DateTimeOffset.UtcNow,
        };
    }

    private async Task<TreeStorageUsageReport> MeasureAsync(string treeId, CancellationToken cancellationToken)
    {
        try
        {
            return await _grainFactory.GetGrain<ILatticeStorageUsage>(treeId)
                .GetReportAsync(forceRefresh: true, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Cancelling the operation stops the refresh; it is never absorbed into
            // a summary of partial zeroes that would read as a real answer.
            throw;
        }
        catch (Exception ex)
        {
            // One tree that cannot be measured contributes a flagged partial
            // reading, as it does in the blocking roll-up, rather than failing all.
            _logger.LogWarning(ex, "Storage-usage refresh could not measure tree {TreeId}.", treeId);
            return new TreeStorageUsageReport { TreeId = treeId, Partial = true, SampledAt = DateTimeOffset.UtcNow };
        }
    }

    private async ValueTask<bool> IsVisibleAsync(LatticeOperationRecord record, CancellationToken cancellationToken) =>
        IsRefreshKind(record) && await IsTelemetryAuthorizedAsync(cancellationToken).ConfigureAwait(false);

    private static bool IsRefreshKind(LatticeOperationRecord record) =>
        string.Equals(record.Kind, StorageUsageRefreshOperation.Kind, StringComparison.Ordinal);

    private async ValueTask<bool> IsTelemetryAuthorizedAsync(CancellationToken cancellationToken)
    {
        try
        {
            await _authorizer.AuthorizeClusterTelemetryAsync(cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (LatticeAuthorizationDeniedException)
        {
            return false;
        }
    }

    private async ValueTask<string> ResolveActiveTenantAsync(CancellationToken cancellationToken)
    {
        var tenant = _tenantResolver.TryResolveCurrent(out var warm)
            ? warm
            : await _tenantResolver.ResolveCurrentAsync(cancellationToken).ConfigureAwait(false);
        return tenant.Value ?? throw new LatticeTenantAccessDeniedException();
    }

    private static string ResolveOperationId(string? operationId)
    {
        if (operationId is null)
        {
            return LatticeOperationKey.NewId();
        }

        LatticeOperationKey.ThrowIfInvalid(operationId, nameof(operationId));
        return operationId;
    }
}
