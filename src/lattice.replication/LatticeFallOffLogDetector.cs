using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="ILatticeFallOffLogDetector"/> implementation.
/// Resolves the per-tree
/// <see cref="IReplicationHighWaterMarkGrain"/> to read the local
/// per-origin HWM, compares it to the supplied sender oldest-HLC,
/// records the
/// <see cref="LatticeReplicationMetrics.PeerFellOffLog"/> counter on
/// detection, and (when
/// <see cref="LatticeReplicationOptions.AutoBootstrapOnFallOffLog"/>
/// is enabled) calls
/// <see cref="ILatticeBootstrapCoordinator.BootstrapAsync"/>.
/// </summary>
internal sealed class LatticeFallOffLogDetector(
    IGrainFactory grainFactory,
    ILatticeBootstrapCoordinator bootstrapCoordinator,
    IOptionsMonitor<LatticeReplicationOptions> optionsMonitor,
    ILogger<LatticeFallOffLogDetector> logger)
    : ILatticeFallOffLogDetector
{
    private readonly IGrainFactory _grainFactory =
        grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));
    private readonly ILatticeBootstrapCoordinator _bootstrapCoordinator =
        bootstrapCoordinator ?? throw new ArgumentNullException(nameof(bootstrapCoordinator));
    private readonly IOptionsMonitor<LatticeReplicationOptions> _optionsMonitor =
        optionsMonitor ?? throw new ArgumentNullException(nameof(optionsMonitor));
    private readonly ILogger<LatticeFallOffLogDetector> _logger =
        logger ?? throw new ArgumentNullException(nameof(logger));

    /// <inheritdoc />
    public async Task<FallOffLogDecision> CheckAndTriggerAsync(
        string treeName,
        string sourceClusterId,
        HybridLogicalClock senderOldestAvailableHlc,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeName);
        ArgumentException.ThrowIfNullOrEmpty(sourceClusterId);
        cancellationToken.ThrowIfCancellationRequested();

        var hwmGrain = _grainFactory.GetGrain<IReplicationHighWaterMarkGrain>(treeName);
        var localHwm = await hwmGrain.GetAsync(sourceClusterId, cancellationToken).ConfigureAwait(false);

        // Fall-off iff the receiver's last applied HLC for this origin is
        // strictly less than the oldest entry the sender still has on its
        // WAL: every entry between localHwm (exclusive) and
        // senderOldestAvailableHlc (exclusive) has already been GC'd on the
        // sender, so incremental replication cannot bridge the gap.
        var fellOff = localHwm.CompareTo(senderOldestAvailableHlc) < 0;
        if (!fellOff)
        {
            return new FallOffLogDecision(false, localHwm, false);
        }

        var options = _optionsMonitor.Get(treeName);
        LatticeReplicationMetrics.PeerFellOffLog.Add(
            1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeName),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOrigin, sourceClusterId));

        _logger.LogWarning(
            "Receiver fell off sender WAL for tree {Tree} origin {Origin}: localHwm={LocalHwm} senderOldest={SenderOldest}; auto-bootstrap={AutoBootstrap}",
            treeName, sourceClusterId, localHwm, senderOldestAvailableHlc, options.AutoBootstrapOnFallOffLog);

        if (!options.AutoBootstrapOnFallOffLog)
        {
            return new FallOffLogDecision(true, localHwm, false);
        }

        await _bootstrapCoordinator
            .BootstrapAsync(treeName, sourceClusterId, cancellationToken)
            .ConfigureAwait(false);
        return new FallOffLogDecision(true, localHwm, true);
    }
}