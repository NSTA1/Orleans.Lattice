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

        if (state.State.Config.ReorderEnabled)
        {
            state.State.Pending.Add(fact);
            if (state.State.Pending.Count >= SiteConfig.ReorderWindowSize)
            {
                var shuffled = Shuffle(state.State.Pending);
                state.State.Pending.Clear();
                await state.WriteStateAsync();
                logger.LogDebug(
                    "Site {Site} reorder window full; flushing {Count} facts in shuffled order",
                    this.GetPrimaryKeyString(), shuffled.Count);
                return SiteAdmission.ReorderFlush(shuffled);
            }
            await state.WriteStateAsync();
            return SiteAdmission.Hold;
        }

        // Only the counter changed; still persist so restart surfaces it.
        await state.WriteStateAsync();

        var delay = state.State.Config.DelayMs;
        return delay > 0 ? SiteAdmission.Delayed(delay) : SiteAdmission.Pass;
    }

    private static IReadOnlyList<Fact> Shuffle(IReadOnlyList<Fact> source)
    {
        var buffer = source.ToArray();
        // Fisher–Yates with Random.Shared; deterministic order is not required —
        // the sample only needs the arrival order to differ from the emission order
        // so the baseline backend can demonstrably diverge from the HLC-ordered lattice.
        for (var i = buffer.Length - 1; i > 0; i--)
        {
            var j = Random.Shared.Next(i + 1);
            (buffer[i], buffer[j]) = (buffer[j], buffer[i]);
        }
        return buffer;
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
