using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tenancy;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Reminder-anchored per-tenant work pump. Its durable intent and pinned source
/// survive silo restarts; each retry delegates idempotent tree bootstrap to the
/// replication bootstrap coordinator.
/// </summary>
internal sealed class TenantRegionBackfillCoordinatorGrain(
    IGrainContext context,
    IReminderRegistry reminderRegistry,
    ILogger<TenantRegionBackfillCoordinatorGrain> logger,
    [PersistentState("tenant-region-backfill", LatticeOptions.StorageProviderName)]
    IPersistentState<TenantRegionBackfillCoordinatorState> state,
    ITenantRegistry registry,
    TenantRegionBackfillService backfill,
    IOptions<ClusterOptions> clusterOptions)
    : CoordinatorGrain<TenantRegionBackfillCoordinatorGrain>(context, reminderRegistry, logger),
        ITenantRegionBackfillCoordinatorGrain
{
    private const string ReminderName = "tenant-region-backfill-keepalive";

    private TenantId Tenant => TenantId.Parse(Context.GrainId.Key.ToString()!);
    private string LocalRegionId => string.IsNullOrEmpty(clusterOptions.Value.ClusterId)
        ? "default"
        : clusterOptions.Value.ClusterId;

    protected override string KeepaliveReminderName => ReminderName;
    protected override bool InProgress => state.State.InProgress;
    protected override string LogContext => $"tenant {Tenant.Value} region {LocalRegionId}";
    protected override string MetricsTreeId => LatticeTenantTrees.ComposePrefix(Tenant);

    protected override Task OnActivateCoreAsync(CancellationToken cancellationToken)
    {
        if (InProgress)
        {
            StartPhaseTimer();
        }

        return Task.CompletedTask;
    }

    public async Task EnsureRunningAsync()
    {
        var record = await registry.GetAsync(Tenant, PhaseTickToken);
        var status = record?.GetRegionStatus(LocalRegionId) ?? TenantRegionStatus.None;
        if (status is not (TenantRegionStatus.Provisioning or TenantRegionStatus.Backfilling))
        {
            if (InProgress)
            {
                state.State.InProgress = false;
                state.State.SourceClusterId = null;
                await state.WriteStateAsync();
                await CompleteCoordinatorAsync();
            }

            return;
        }

        if (!InProgress)
        {
            state.State.InProgress = true;
            state.State.SourceClusterId = SelectOnlineSource(record!);
            await state.WriteStateAsync();
            await StartCoordinatorAsync();
            return;
        }

        await RefreshPinnedSourceAsync(record!);
        StartPhaseTimer();
    }

    protected internal override async Task ProcessNextPhaseAsync()
    {
        if (!InProgress)
        {
            return;
        }

        var record = await registry.GetAsync(Tenant, PhaseTickToken);
        if (record is null)
        {
            await StopAsync();
            return;
        }

        var status = record.GetRegionStatus(LocalRegionId);
        if (status is not (TenantRegionStatus.Provisioning or TenantRegionStatus.Backfilling))
        {
            await StopAsync();
            return;
        }

        await RefreshPinnedSourceAsync(record);

        var advanced = await backfill.AdvanceTenantAsync(
            Tenant, state.State.SourceClusterId, PhaseTickToken);
        if (advanced is not (TenantRegionStatus.Provisioning or TenantRegionStatus.Backfilling))
        {
            await StopAsync();
        }
    }

    private string? SelectOnlineSource(TenantRecord record) =>
        record.RegionStatusEntries
            .Where(entry => entry.Value == TenantRegionStatus.Online
                && !string.Equals(entry.Key, LocalRegionId, StringComparison.Ordinal))
            .Select(entry => entry.Key)
            .Order(StringComparer.Ordinal)
            .FirstOrDefault();

    private async Task RefreshPinnedSourceAsync(TenantRecord record)
    {
        var currentSource = state.State.SourceClusterId;
        if (currentSource is not null
            && record.RegionStatusEntries.Any(entry =>
                string.Equals(entry.Key, currentSource, StringComparison.Ordinal)
                && entry.Value == TenantRegionStatus.Online))
        {
            return;
        }

        var nextSource = SelectOnlineSource(record);
        if (string.Equals(currentSource, nextSource, StringComparison.Ordinal))
        {
            return;
        }

        state.State.SourceClusterId = nextSource;
        await state.WriteStateAsync();
    }

    private async Task StopAsync()
    {
        state.State.InProgress = false;
        state.State.SourceClusterId = null;
        await state.WriteStateAsync();
        await CompleteCoordinatorAsync();
    }
}
