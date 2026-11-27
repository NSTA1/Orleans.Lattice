namespace Orleans.Lattice;

/// <summary>
/// How an audited call ended, as observed by the facade that ran it.
/// </summary>
/// <param name="Outcome">
/// <see cref="LatticeAdminAuditOutcome.Committed"/> when the commit returned
/// successfully and reported a change; otherwise <see cref="LatticeAdminAuditOutcome.Failed"/>
/// or <see cref="LatticeAdminAuditOutcome.Denied"/>.
/// </param>
/// <param name="ReasonCode">The reason code for a non-committed outcome, or <see langword="null"/>.</param>
internal readonly record struct LatticeAdminAuditCompletion(LatticeAdminAuditOutcome Outcome, string? ReasonCode)
{
    /// <summary>The commit returned successfully and changed state.</summary>
    public static LatticeAdminAuditCompletion Committed { get; } = new(LatticeAdminAuditOutcome.Committed, null);

    /// <summary>The commit returned successfully without changing state.</summary>
    public static LatticeAdminAuditCompletion NoChange { get; } = new(LatticeAdminAuditOutcome.Failed, LatticeAdminAuditReasonCodes.FailedNoChange);
}
