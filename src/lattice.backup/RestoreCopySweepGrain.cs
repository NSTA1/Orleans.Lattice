using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Backup;

/// <summary>
/// The default <see cref="IRestoreCopySweepGrain"/> (issue #6103). A sweep reads
/// only the registry: the tree ids, and the rows of the ids carrying the restore
/// shadow infix, a bounded batch at a time, grouping them by their restore
/// provenance. It retires nothing itself - each tree's own grain does, a pass at a
/// time - and it spaces the trees it schedules one retirement pass apart, so a
/// sweep over an estate holding many unreachable copies never starts their
/// retirement in one burst.
/// </summary>
internal sealed class RestoreCopySweepGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IReminderRegistry reminderRegistry,
    IOptionsMonitor<LatticeBackupOptions> optionsMonitor,
    ILogger<RestoreCopySweepGrain> logger,
    [PersistentState("restore-copy-sweep", LatticeOptions.StorageProviderName)]
    IPersistentState<RestoreCopySweepState> state)
    : IGrainBase, IRemindable, IRestoreCopySweepGrain
{
    private const string ReminderName = "restore-copy-sweep";
    private const int EntryBatchSize = 64;
    private static readonly TimeSpan MaximumReminderPeriod = TimeSpan.FromDays(49);

    // Activation-local overlap guard: a crash clears it, so it can never wedge the sweep.
    private bool _sweepInFlight;

    /// <inheritdoc />
    public IGrainContext GrainContext => context;

    private LatticeBackupOptions Options => optionsMonitor.CurrentValue;

    /// <inheritdoc />
    public async Task EnsureStartedAsync()
    {
        var reminder = await reminderRegistry.GetReminder(context.GrainId, ReminderName);
        if (Options.RestoreCopySweepInterval is not { } interval)
        {
            if (reminder is not null)
            {
                await reminderRegistry.UnregisterReminder(context.GrainId, reminder);
            }

            if (state.State.ArmedPeriod is not null)
            {
                state.State.ArmedPeriod = null;
                await state.WriteStateAsync();
            }

            return;
        }

        var period = interval > MaximumReminderPeriod ? MaximumReminderPeriod : interval;
        if (reminder is not null && state.State.ArmedPeriod == period)
        {
            return;
        }

        await BackupReminderResilience.RunWithRetryAsync(
            () => reminderRegistry.RegisterOrUpdateReminder(
                context.GrainId, ReminderName, Options.RestoreCopySweepInitialDelay, period),
            logger, nameof(reminderRegistry.RegisterOrUpdateReminder), ReminderName, IRestoreCopySweepGrain.SingletonKey);
        state.State.ArmedPeriod = period;
        await state.WriteStateAsync();
    }

    /// <inheritdoc />
    public Task<int> SweepAsync()
    {
        LatticeInternalOriginContext.EnsureInternalGrainOrigin(
            context.ActivationServices, IRestoreCopySweepGrain.SingletonKey, LatticeOperation.TreeLifecycle);
        return SweepCoreAsync();
    }

    /// <inheritdoc />
    public async Task ReceiveReminder(string reminderName, TickStatus status)
    {
        if (reminderName != ReminderName)
        {
            return;
        }

        try
        {
            await SweepCoreAsync();
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "The restore-copy sweep failed; the next sweep retries.");
        }
    }

    private async Task<int> SweepCoreAsync()
    {
        if (_sweepInFlight)
        {
            return 0;
        }

        _sweepInFlight = true;
        try
        {
            HashSet<string>? trees = null;
            using (LatticeAccessGateContext.EnterSystemOrigin())
            {
                var registry = grainFactory.GetLatticeRegistry();
                var ids = await registry.GetAllTreeIdsAsync();
                var batch = new List<string>(EntryBatchSize);
                foreach (var id in ids)
                {
                    if (!id.Contains(RestoreCopyPlanner.ShadowInfix, StringComparison.Ordinal))
                    {
                        continue;
                    }

                    batch.Add(id);
                    if (batch.Count == EntryBatchSize)
                    {
                        await CollectAsync(registry, batch);
                    }
                }

                if (batch.Count > 0)
                {
                    await CollectAsync(registry, batch);
                }
            }

            var spacing = Options.RestoreCopyRetirementPassInterval;
            var scheduled = 0;
            if (trees is not null)
            {
                foreach (var tree in trees)
                {
                    await grainFactory.GetGrain<IRestoreCopyRetirementGrain>(tree)
                        .ScheduleAsync(spacing * (scheduled + 1));
                    scheduled++;
                }
            }

            state.State.LastSweepUtc = DateTimeOffset.UtcNow;
            state.State.LastSweepTreeCount = scheduled;
            await state.WriteStateAsync();
            logger.LogInformation("The restore-copy sweep scheduled {TreeCount} tree(s) for retirement.", scheduled);
            return scheduled;

            async Task CollectAsync(ILatticeRegistry registry, List<string> ids)
            {
                var entries = await registry.GetEntriesAsync(ids.ToArray());
                foreach (var entry in entries.Values)
                {
                    if (entry.RestoreShadowOfTreeId is { Length: > 0 } logical)
                    {
                        (trees ??= new HashSet<string>(StringComparer.Ordinal)).Add(logical);
                    }
                }

                ids.Clear();
            }
        }
        finally
        {
            _sweepInFlight = false;
        }
    }
}
