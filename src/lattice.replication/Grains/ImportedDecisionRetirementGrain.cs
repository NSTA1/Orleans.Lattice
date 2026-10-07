using System.Collections.Immutable;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>Default <see cref="IImportedDecisionRetirementGrain"/>.</summary>
internal sealed class ImportedDecisionRetirementGrain(
    [PersistentState("replication-imported-decisions", LatticeOptions.StorageProviderName)]
    IPersistentState<ImportedDecisionRetirementState> state,
    IGrainFactory grainFactory,
    ILogger<ImportedDecisionRetirementGrain> logger) : Grain, IImportedDecisionRetirementGrain, IRemindable
{
    /// <summary>The reminder that re-checks retained rows until none remain.</summary>
    internal const string RetireReminderName = "replication-imported-decisions-retire";

    /// <summary>How often the reminder re-checks retained rows.</summary>
    internal static TimeSpan RetirePeriod { get; set; } = TimeSpan.FromMinutes(1);

    private bool? _reminderArmed;

    private string TreeName => this.GetPrimaryKeyString();

    /// <inheritdoc />
    public async Task RegisterAsync(string sourceClusterId, CrossTreeSiblingBoundary? exportBoundary, ImmutableArray<Guid> transactionIds)
    {
        ArgumentException.ThrowIfNullOrEmpty(sourceClusterId);
        if (transactionIds.IsDefaultOrEmpty)
        {
            return;
        }

        if (!state.State.Sources.TryGetValue(sourceClusterId, out var set))
        {
            set = new ImportedDecisionSet();
            state.State.Sources[sourceClusterId] = set;
        }

        // The newest import's boundary covers every older one on the same log;
        // an import without one retains everything recorded from the source.
        set.ExportBoundary = exportBoundary;
        set.TransactionIds.UnionWith(transactionIds);
        await state.WriteStateAsync();
        await RetireAsync();
    }

    /// <inheritdoc />
    public async Task<int> RetireAsync()
    {
        var changed = false;
        foreach (var (source, set) in state.State.Sources.ToList())
        {
            if (set.TransactionIds.Count == 0)
            {
                state.State.Sources.Remove(source);
                changed = true;
                continue;
            }

            if (!await HasPassedAsync(source, set.ExportBoundary))
            {
                continue;
            }

            foreach (var txid in set.TransactionIds)
            {
                await TxRegistryRouting.GetRegistry(grainFactory, TreeName, txid).ForgetAsync(txid);
            }

            logger.LogDebug(
                "Retired {Count} imported saga decision row(s) of tree '{TreeName}' from '{SourceClusterId}': the incremental stream passed the export's cut.",
                set.TransactionIds.Count, TreeName, source);
            state.State.Sources.Remove(source);
            changed = true;
        }

        if (changed)
        {
            await state.WriteStateAsync();
        }

        var remaining = state.State.Sources.Values.Sum(static s => s.TransactionIds.Count);
        await SetReminderAsync(armed: remaining > 0);
        return remaining;
    }

    /// <inheritdoc />
    public async Task ReceiveReminder(string reminderName, TickStatus status)
    {
        if (string.Equals(reminderName, RetireReminderName, StringComparison.Ordinal))
        {
            await RetireAsync();
        }
    }

    /// <summary>
    /// Whether the incremental stream from <paramref name="sourceClusterId"/>
    /// has passed <paramref name="boundary"/>: its shipper vouched acknowledged
    /// positions on the exported log at or past every captured tail. Positions
    /// pushed before the import began were cleared when it replaced the tree's
    /// contents, so only the post-import stream vouches. A log that held nothing
    /// at capture has no pre-cut prepare to re-ship.
    /// </summary>
    private async Task<bool> HasPassedAsync(string sourceClusterId, CrossTreeSiblingBoundary? boundary)
    {
        if (boundary is null)
        {
            return false;
        }

        if (boundary.IsEmpty)
        {
            return true;
        }

        var frontier = await grainFactory.GetGrain<IReplicationTreeFrontierGrain>(TreeName).GetAsync();
        return frontier.AckedPositions.TryGetValue(sourceClusterId, out var acked)
            && acked.CoversTails(boundary.PhysicalTreeId, boundary.Tails);
    }

    private async Task SetReminderAsync(bool armed)
    {
        if (_reminderArmed == armed)
        {
            return;
        }

        try
        {
            if (armed)
            {
                await this.RegisterOrUpdateReminder(RetireReminderName, RetirePeriod, RetirePeriod);
            }
            else if (await this.GetReminder(RetireReminderName) is { } reminder)
            {
                await this.UnregisterReminder(reminder);
            }

            _reminderArmed = armed;
        }
        catch (Exception ex)
        {
            // Non-fatal: the next import from any source re-checks.
            logger.LogWarning(ex, "Failed to update the imported-decision retire reminder for tree '{TreeName}'.", TreeName);
        }
    }
}
