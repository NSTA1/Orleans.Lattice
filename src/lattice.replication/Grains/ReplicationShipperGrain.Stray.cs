using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Stray-shipper retirement (#5676). The driver activates shippers only for
/// enrolled trees, but an older alias observer activated one for every aliased
/// logical name. Such a shipper tails the alias target's log and ships every
/// record under the alias id, so rows of a tree the cluster may keep local leave
/// it under a name no receiver enrols. Its offset-consumer registration also
/// holds the target's log.
/// <para>
/// A stray is a shipper whose tree resolves no merge mode, has no static or
/// runtime replication configuration, and is an alias or not a registered tree.
/// It ships nothing. It withdraws from the log through the peer-removal path
/// (<see cref="DetachFromLogAsync"/>), drops its peer's cross-tree enrolment,
/// stops its timer and deactivates. A runtime-disabled or ambiguous tree still
/// has configuration, so it is not a stray and behaves as before.
/// </para>
/// </summary>
internal sealed partial class ReplicationShipperGrain
{
    private readonly ILatticeReplicationContext? _replicationContext = replicationContext;
    private readonly CompiledReplicationConfigSnapshotMaintainer? _runtimeReplicationConfig = runtimeReplicationConfig;

    private bool _isStray;
    private bool _strayWithdrawn;
    private DateTime _nextStrayCheckUtc = DateTime.MinValue;

    /// <summary>
    /// Retires this shipper when it is a stray. Returns <see langword="true"/>
    /// when the tick must not read or ship. Without a replication context (the
    /// direct-construction unit-test path) nothing is a stray.
    /// </summary>
    private async Task<bool> RetireIfStrayAsync()
    {
        if (_isStray)
        {
            await WithdrawStrayAsync();
            return true;
        }

        // The steady-state check: one cached lookup, no I/O.
        if (_replicationContext is null || _replicationContext.ResolveMergeMode(_treeName) is not null)
        {
            return false;
        }

        var now = _cursorFlushClock.GetUtcNow().UtcDateTime;
        if (now < _nextStrayCheckUtc)
        {
            return false;
        }

        _nextStrayCheckUtc = now + _optionsMonitor.Get(_treeName).ShipSourceIdentityBackstopInterval;
        if (!await ReplicationTreeConfiguration.IsUnconfiguredAsync(
                _replicationContext, _optionsMonitor, _runtimeReplicationConfig, _treeName))
        {
            return false;
        }

        var entry = await _grainFactory.GetLatticeRegistry().GetEntryAsync(_treeName);
        if (entry is not null && entry.PhysicalTreeId is null)
        {
            return false;
        }

        _isStray = true;
        Logger.LogWarning(
            "{Context}: tree '{Tree}' has no replication configuration on this cluster and is {Shape}; this stray "
            + "shipper ships nothing, withdraws from the log it was bound to, and deactivates.",
            LogContext, _treeName, entry is null ? "not a registered tree" : $"an alias of '{entry.PhysicalTreeId}'");
        await WithdrawStrayAsync();
        return true;
    }

    /// <summary>
    /// Withdraws a stray from the log and the cross-tree enrolment, then stops
    /// it. A failed step throws out of the tick and is retried on the next one;
    /// the stray ships nothing in between.
    /// </summary>
    private async Task WithdrawStrayAsync()
    {
        if (_strayWithdrawn)
        {
            return;
        }

        await DetachFromLogAsync(CancellationToken.None);
        try
        {
            await _grainFactory.GetGrain<ICrossTreePeerEnrolmentGrain>(_treeName).DecommissionAsync(_peerClusterId);
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "{Context}: dropping the stray's cross-tree enrolment failed; the next activation retries.",
                LogContext);
        }

        _strayWithdrawn = true;
        await CompleteCoordinatorAsync();
    }
}
