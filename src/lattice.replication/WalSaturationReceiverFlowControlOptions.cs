namespace Orleans.Lattice.Replication;

/// <summary>
/// Tuning surface for <see cref="WalSaturationReceiverFlowControlPolicy"/> -
/// the receiver-side <see cref="IReceiverFlowControlPolicy"/> that maps the
/// core library's <see cref="IWalSaturationSignal"/> regime onto the
/// <see cref="ReceiverFlowControlHint"/> stamped on each
/// <see cref="ReplicationAck"/>.
/// <para>
/// Bound per-tree via <c>IOptionsMonitor&lt;WalSaturationReceiverFlowControlOptions&gt;.Get(treeName)</c>;
/// hosts configure the cluster-wide baseline (and optional per-tree
/// overrides) through
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddWalSaturationReceiverFlowControl(Orleans.Hosting.ISiloBuilder, System.Action{WalSaturationReceiverFlowControlOptions}?)"/>.
/// </para>
/// <para>
/// The defaults err on the side of gentle back-pressure: a
/// <see cref="WalSaturationState.Throttled"/> tree halves the suggested
/// batch size and asks for a short pause; a
/// <see cref="WalSaturationState.Saturated"/> tree drip-feeds a single
/// entry per tick and asks for a longer pause so the sender stops piling
/// work onto an admission gate that is already at its cap.
/// </para>
/// </summary>
public sealed class WalSaturationReceiverFlowControlOptions
{
    /// <summary>
    /// Default value for <see cref="ThrottledBatchRatio"/>: ship half of
    /// the configured <see cref="LatticeReplicationOptions.ShipBatchSize"/>
    /// while the tree is <see cref="WalSaturationState.Throttled"/>.
    /// </summary>
    public const double DefaultThrottledBatchRatio = 0.5d;

    /// <summary>
    /// Default value for <see cref="ThrottledPauseMs"/>: ask the sender to
    /// pause 50 ms before the next pump tick while the tree is
    /// <see cref="WalSaturationState.Throttled"/>.
    /// </summary>
    public const int DefaultThrottledPauseMs = 50;

    /// <summary>
    /// Default value for <see cref="SaturatedBatchSize"/>: drip-feed one
    /// entry per tick while the tree is
    /// <see cref="WalSaturationState.Saturated"/>.
    /// </summary>
    public const int DefaultSaturatedBatchSize = 1;

    /// <summary>
    /// Default value for <see cref="SaturatedPauseMs"/>: ask the sender to
    /// pause 500 ms before the next pump tick while the tree is
    /// <see cref="WalSaturationState.Saturated"/>.
    /// </summary>
    public const int DefaultSaturatedPauseMs = 500;

    /// <summary>
    /// Fraction of the configured
    /// <see cref="LatticeReplicationOptions.ShipBatchSize"/> the receiver
    /// suggests while the tree is <see cref="WalSaturationState.Throttled"/>.
    /// The policy computes <c>ceil(ShipBatchSize * ratio)</c> and clamps the
    /// result to the closed interval <c>[1, ShipBatchSize]</c>; the supplied
    /// ratio is itself clamped to <c>[0, 1]</c> so an out-of-range value can
    /// never inflate the batch above the sender's configured cap. Defaults to
    /// <see cref="DefaultThrottledBatchRatio"/>.
    /// </summary>
    public double ThrottledBatchRatio { get; set; } = DefaultThrottledBatchRatio;

    /// <summary>
    /// Milliseconds the receiver asks the sender to pause before its next
    /// pump tick while the tree is <see cref="WalSaturationState.Throttled"/>.
    /// A value less than or equal to zero is surfaced as "no pause requested"
    /// (the hint's <see cref="ReceiverFlowControlHint.PauseForMs"/> stays
    /// <see langword="null"/>). Defaults to
    /// <see cref="DefaultThrottledPauseMs"/>.
    /// </summary>
    public int ThrottledPauseMs { get; set; } = DefaultThrottledPauseMs;

    /// <summary>
    /// Absolute per-tick batch size the receiver suggests while the tree is
    /// <see cref="WalSaturationState.Saturated"/>. Clamped to the closed
    /// interval <c>[1, ShipBatchSize]</c>. Defaults to
    /// <see cref="DefaultSaturatedBatchSize"/> (a single-entry drip-feed).
    /// </summary>
    public int SaturatedBatchSize { get; set; } = DefaultSaturatedBatchSize;

    /// <summary>
    /// Milliseconds the receiver asks the sender to pause before its next
    /// pump tick while the tree is <see cref="WalSaturationState.Saturated"/>.
    /// A value less than or equal to zero is surfaced as "no pause requested"
    /// (the hint's <see cref="ReceiverFlowControlHint.PauseForMs"/> stays
    /// <see langword="null"/>). Defaults to
    /// <see cref="DefaultSaturatedPauseMs"/>.
    /// </summary>
    public int SaturatedPauseMs { get; set; } = DefaultSaturatedPauseMs;
}
