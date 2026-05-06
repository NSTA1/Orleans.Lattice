using Microsoft.Extensions.Logging;
using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Lattice;

namespace MultiSiteManufacturing.Host.Dashboard;

/// <summary>
/// Per-fact fan-out path for <see cref="DashboardBroadcaster"/>.
/// Receives a fact from <see cref="OnBroadcastReceived"/>, derives
/// the corresponding <see cref="PartSummaryUpdate"/> /
/// <see cref="DivergenceEvent"/> / <see cref="SiteActivityIndexEntry"/>
/// values, and writes them to every active per-circuit channel.
/// Errors are logged but never thrown — the cluster-wide stream
/// agent must keep moving even if a single fact's fan-out fails.
/// </summary>
public sealed partial class DashboardBroadcaster
{
    private void OnChaosConfigChanged(object? sender, EventArgs e) => _ = PublishChaosAsync();

    /// <summary>
    /// Builds a <see cref="SiteActivityIndexEntry"/> from the in-memory
    /// fact and fans it out to every site-activity subscriber. Exposed
    /// as a standalone helper so <see cref="OnBroadcastReceived"/> can
    /// share the same logic as tests that invoke it directly.
    /// </summary>
    private void FanOutSiteActivity(Fact fact)
    {
        try
        {
            var entry = new SiteActivityIndexEntry(
                fact.Site,
                fact.Serial,
                fact.Hlc,
                SiteActivityIndex.DescribeActivity(fact));
            foreach (var sub in _activitySubs.Values)
            {
                sub.Writer.TryWrite(entry);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to fan out site-activity entry for fact {FactId}", fact.FactId);
        }
    }

    private async Task PublishPartAsync(Fact fact)
    {
        try
        {
            var update = await BuildSummaryAsync(fact.Serial, CancellationToken.None);
            foreach (var sub in _partSubs.Values)
            {
                sub.Writer.TryWrite(update);
            }

            // Derive a divergence transition, if any, and fan that out on
            // the divergence channel. We publish on:
            //   - entry into divergence (previous absent or agreed; now disagrees)
            //   - state change while still divergent (both backends' states
            //     have shifted but they still disagree)
            //   - resolution (previously disagreed; now agrees)
            var newStates = (update.BaselineState, update.LatticeState);
            _lastStates.TryGetValue(update.Serial, out var oldStates);
            _lastStates[update.Serial] = newStates;

            var nowDiverges = update.Diverges;
            var wasDiverging = oldStates != default && oldStates.Baseline != oldStates.Lattice;

            if (!nowDiverges && !wasDiverging)
            {
                return;
            }

            DivergenceEvent? evt = null;
            if (nowDiverges && (!wasDiverging || oldStates != newStates))
            {
                evt = new DivergenceEvent
                {
                    Serial = update.Serial,
                    BaselineState = update.BaselineState,
                    LatticeState = update.LatticeState,
                    Resolved = false,
                };
            }
            else if (!nowDiverges && wasDiverging)
            {
                evt = new DivergenceEvent
                {
                    Serial = update.Serial,
                    BaselineState = update.BaselineState,
                    LatticeState = update.LatticeState,
                    Resolved = true,
                };
            }

            if (evt is not null)
            {
                foreach (var sub in _divSubs.Values)
                {
                    sub.Writer.TryWrite(evt);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to build dashboard update for fact {FactId}", fact.FactId);
        }
    }

    private async Task PublishChaosAsync()
    {
        try
        {
            var overview = await GetChaosOverviewAsync();
            foreach (var sub in _chaosSubs.Values)
            {
                sub.Writer.TryWrite(overview);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to build chaos overview update");
        }
    }
}
