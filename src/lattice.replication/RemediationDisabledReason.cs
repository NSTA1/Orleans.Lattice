namespace Orleans.Lattice.Replication;

/// <summary>
/// Reason automatic anti-entropy remediation is currently disabled for a
/// <c>(tree, peer)</c> pair. Carried on the <see cref="LatticeReplicationMetrics.TagReason"/>
/// tag of the <see cref="LatticeReplicationMetrics.DigestRemediationDisabledName"/>
/// observable gauge and the
/// <see cref="LatticeReplicationMetrics.DigestRemediationSkippedName"/> counter.
/// </summary>
/// <remarks>
/// This is an in-process telemetry classification only - it is never sent over
/// the wire nor persisted in grain state, so it carries no Orleans
/// serialization attributes (mirroring <see cref="LeafReReplaySkipReason"/> /
/// <see cref="BootstrapFallbackSkipReason"/>).
/// </remarks>
public enum RemediationDisabledReason
{
    /// <summary>
    /// The host has not opted into automatic remediation:
    /// <see cref="LatticeReplicationOptions.AutoRemediateOnDigestMismatch"/> is
    /// <see langword="false"/>. Drift is still detected and probed; only the
    /// repair action is suppressed. Maps to the
    /// <see cref="LatticeReplicationMetrics.DigestRemediationReasonOptOut"/>
    /// reason tag.
    /// </summary>
    OptOut = 0,

    /// <summary>
    /// The per-tree, per-peer remediation traffic budget for the current
    /// accounting window has been spent, so further remediation passes are
    /// skipped until the window rolls over. Maps to the
    /// <see cref="LatticeReplicationMetrics.DigestRemediationReasonBudgetExhausted"/>
    /// reason tag.
    /// </summary>
    BudgetExhausted = 1,

    /// <summary>
    /// The remediation circuit breaker for the tree/peer is open after
    /// <see cref="LatticeReplicationOptions.RemediationFailureThreshold"/>
    /// consecutive failures and has not yet cooled down. Maps to the
    /// <see cref="LatticeReplicationMetrics.DigestRemediationReasonCircuitOpen"/>
    /// reason tag.
    /// </summary>
    CircuitOpen = 2,
}
