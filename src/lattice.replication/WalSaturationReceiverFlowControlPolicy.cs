using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Replication;

/// <summary>
/// <see cref="IReceiverFlowControlPolicy"/> that bridges the core library's
/// WAL saturation back-pressure signal to the replication sender. On every
/// successful push the policy reads
/// <see cref="IWalSaturationSignal.GetCurrentState(string)"/> for the
/// just-applied tree and maps the regime onto a
/// <see cref="ReceiverFlowControlHint"/>:
/// <list type="bullet">
///   <item><description><see cref="WalSaturationState.Healthy"/> -
///   <see cref="ReceiverFlowControlHint.None"/>; the sender resumes at its
///   configured <see cref="LatticeReplicationOptions.ShipBatchSize"/>.</description></item>
///   <item><description><see cref="WalSaturationState.Throttled"/> - a
///   reduced batch (a configurable fraction of <c>ShipBatchSize</c>) and a
///   short pause, so the sender slows without stalling.</description></item>
///   <item><description><see cref="WalSaturationState.Saturated"/> - a
///   minimal drip-feed batch and a longer pause, so the sender stops piling
///   work onto a writer-side admission gate that is already at its cap and
///   would otherwise fault the apply with
///   <see cref="LatticeSaturatedException"/>.</description></item>
/// </list>
/// <para>
/// The hint rides the existing additive <see cref="ReplicationAck.SuggestedBatchSize"/>
/// / <see cref="ReplicationAck.PauseForMs"/> slots; there are no wire or
/// serialisation changes. When no <see cref="IWalSaturationSignal"/> is
/// registered (the core signal is produced by <c>AddLattice</c>), the policy
/// degrades to <see cref="ReceiverFlowControlHint.None"/> so the receiver
/// keeps the existing blind-push behaviour rather than failing.
/// </para>
/// <para>
/// This policy is the default <see cref="IReceiverFlowControlPolicy"/>
/// installed by <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication(Orleans.Hosting.ISiloBuilder, System.Action{LatticeReplicationOptions}, bool)"/>;
/// call
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddWalSaturationReceiverFlowControl(Orleans.Hosting.ISiloBuilder, System.Action{WalSaturationReceiverFlowControlOptions}?)"/>
/// to tune the mapping. A host that wants the old blind-push behaviour
/// pre-registers <see cref="NoOpReceiverFlowControlPolicy"/> before
/// <c>AddLatticeReplication</c>.
/// </para>
/// </summary>
public sealed class WalSaturationReceiverFlowControlPolicy : IReceiverFlowControlPolicy
{
    private readonly IWalSaturationSignal? _signal;
    private readonly IOptionsMonitor<LatticeReplicationOptions> _replicationOptions;
    private readonly IOptionsMonitor<WalSaturationReceiverFlowControlOptions> _flowControlOptions;

    /// <summary>
    /// Creates the policy.
    /// </summary>
    /// <param name="signal">The per-silo WAL saturation signal registered by
    /// <c>AddLattice</c>. May be <see langword="null"/>; when absent the
    /// policy returns <see cref="ReceiverFlowControlHint.None"/> for every
    /// push.</param>
    /// <param name="replicationOptions">Monitor used to read the per-tree
    /// <see cref="LatticeReplicationOptions.ShipBatchSize"/> that anchors the
    /// throttled batch ratio.</param>
    /// <param name="flowControlOptions">Monitor used to read the per-tree
    /// throttled / saturated batch-size and pause tuning.</param>
    public WalSaturationReceiverFlowControlPolicy(
        IWalSaturationSignal? signal,
        IOptionsMonitor<LatticeReplicationOptions> replicationOptions,
        IOptionsMonitor<WalSaturationReceiverFlowControlOptions> flowControlOptions)
    {
        ArgumentNullException.ThrowIfNull(replicationOptions);
        ArgumentNullException.ThrowIfNull(flowControlOptions);

        _signal = signal;
        _replicationOptions = replicationOptions;
        _flowControlOptions = flowControlOptions;
    }

    /// <inheritdoc />
    public ValueTask<ReceiverFlowControlHint> EvaluateAsync(
        ReceiverFlowControlContext context,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // No signal registered (e.g. a host that wired replication without
        // the core saturation sampler): degrade to the canonical "no
        // preference" hint and preserve blind-push behaviour.
        if (_signal is null || string.IsNullOrEmpty(context.TreeName))
        {
            return ValueTask.FromResult(ReceiverFlowControlHint.None);
        }

        var state = _signal.GetCurrentState(context.TreeName);
        if (state == WalSaturationState.Healthy)
        {
            return ValueTask.FromResult(ReceiverFlowControlHint.None);
        }

        var shipBatchSize = Math.Max(1, _replicationOptions.Get(context.TreeName).ShipBatchSize);
        var tuning = _flowControlOptions.Get(context.TreeName);

        var hint = state switch
        {
            WalSaturationState.Throttled => new ReceiverFlowControlHint
            {
                SuggestedBatchSize = ThrottledBatchSize(shipBatchSize, tuning.ThrottledBatchRatio),
                PauseForMs = NormalisePause(tuning.ThrottledPauseMs),
            },
            _ => new ReceiverFlowControlHint
            {
                SuggestedBatchSize = Math.Clamp(tuning.SaturatedBatchSize, 1, shipBatchSize),
                PauseForMs = NormalisePause(tuning.SaturatedPauseMs),
            },
        };

        return ValueTask.FromResult(hint);
    }

    private static int ThrottledBatchSize(int shipBatchSize, double ratio)
    {
        var clampedRatio = Math.Clamp(ratio, 0d, 1d);
        var scaled = (int)Math.Ceiling(shipBatchSize * clampedRatio);
        return Math.Clamp(scaled, 1, shipBatchSize);
    }

    private static int? NormalisePause(int pauseMs) => pauseMs > 0 ? pauseMs : null;
}
