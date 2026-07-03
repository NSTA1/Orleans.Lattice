namespace Orleans.Lattice.Auth;

/// <summary>
/// Controls which gated authorization decisions are dispatched to the
/// <see cref="ILatticeAuthAuditSink"/> seam when auditing is enabled.
/// </summary>
public enum LatticeAuthAuditVerbosity
{
    /// <summary>
    /// Only denied decisions are audited (the default). The lowest-volume,
    /// highest-signal setting: an audit trail of every refusal without the noise
    /// of every allowed read.
    /// </summary>
    DenyOnly = 0,

    /// <summary>
    /// Every gated decision - allow and deny - is audited. Use for a complete
    /// access record; expect materially higher event volume.
    /// </summary>
    AllDecisions = 1,
}
