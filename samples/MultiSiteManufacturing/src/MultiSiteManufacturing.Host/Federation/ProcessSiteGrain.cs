using Microsoft.Extensions.Logging;
using MultiSiteManufacturing.Host.Domain;
using Orleans.Runtime;

namespace MultiSiteManufacturing.Host.Federation;

/// <summary>
/// Implementation of <see cref="IProcessSiteGrain"/>. Each site grain is
/// a thin state-machine around <see cref="ProcessSiteState"/>: admit,
/// reconfigure, drain.
/// </summary>
internal sealed class ProcessSiteGrain(
    [PersistentState("state", "msmfgGrainState")] IPersistentState<ProcessSiteState> state,
    ILogger<ProcessSiteGrain> logger) : Grain, IProcessSiteGrain
{
    public Task<SiteState> GetStateAsync() =>
        Task.FromResult(Snapshot());

    public async Task<SiteConfigureResult> ConfigureAsync(SiteConfig config)
    {
        var wasPaused = state.State.Config.IsPaused;
        state.State.Config = config;

        IReadOnlyList<Fact> drained;
        if (wasPaused && !config.IsPaused && state.State.Pending.Count > 0)
        {
            drained = state.State.Pending.ToList();
            state.State.Pending.Clear();
            logger.LogInformation(
                "Site {Site} unpaused; draining {Count} queued facts",
                this.GetPrimaryKeyString(), drained.Count);
        }
        else
        {
            drained = Array.Empty<Fact>();
        }

        await state.WriteStateAsync();
        return new SiteConfigureResult { Config = config, Drained = drained };
    }

    public async Task<SiteAdmission> AdmitAsync(Fact fact)
    {
        ArgumentNullException.ThrowIfNull(fact);

        state.State.AdmittedCount++;

        if (state.State.Config.IsPaused)
        {
            state.State.Pending.Add(fact);
            await state.WriteStateAsync();
            return SiteAdmission.Hold;
        }

        // Only the counter changed; still persist so restart surfaces it.
        await state.WriteStateAsync();

        var delay = state.State.Config.DelayMs;
        return delay > 0 ? SiteAdmission.Delayed(delay) : SiteAdmission.Pass;
    }

    private SiteState Snapshot()
    {
        var key = this.GetPrimaryKeyString();
        var site = Enum.TryParse<ProcessSite>(key, out var parsed)
            ? parsed
            : throw new InvalidOperationException($"ProcessSiteGrain activated with unknown site key '{key}'.");

        return new SiteState
        {
            Site = site,
            Config = state.State.Config,
            PendingCount = state.State.Pending.Count,
            AdmittedCount = state.State.AdmittedCount,
        };
    }
}
