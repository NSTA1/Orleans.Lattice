namespace Orleans.Lattice;

/// <summary>
/// The durable admin audit trail seam the administration facades write through.
/// The core registers none: a facade constructed without a trail runs unaudited,
/// exactly as before. <c>Orleans.Lattice.Auth</c> registers the durable
/// implementation, which writes to the reserved <c>sys-admin-audit</c> tree.
/// </summary>
/// <remarks>
/// Facades do not call this directly; they go through <see cref="LatticeAdminAudit"/>,
/// which fixes the protocol: denial records before the refusal returns, an intent
/// before the commit (fail closed), a finalize after it.
/// </remarks>
internal interface ILatticeAdminAuditTrail
{
    /// <summary>Whether the trail is enabled; when <see langword="false"/> no record is written and no call is refused for audit reasons.</summary>
    bool IsEnabled { get; }

    /// <summary>
    /// Writes one <see cref="LatticeAdminAuditOutcome.Denied"/> record for a refused
    /// call. Never throws: a failed write is logged and counted, and the caller still
    /// returns its refusal.
    /// </summary>
    /// <param name="request">The refused call.</param>
    /// <param name="reasonCode">The denial reason code.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    ValueTask RecordDeniedAsync(LatticeAdminAuditRequest request, string reasonCode, CancellationToken cancellationToken);

    /// <summary>
    /// Writes the call's <see cref="LatticeAdminAuditOutcome.Pending"/> intent before
    /// the commit. Throws <see cref="LatticeAdminAuditUnavailableException"/> when the
    /// intent cannot be written, so the caller refuses the call before changing
    /// anything.
    /// </summary>
    /// <param name="request">The call about to commit.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>The intent to finalize.</returns>
    ValueTask<LatticeAdminAuditRecord> BeginAsync(LatticeAdminAuditRequest request, CancellationToken cancellationToken);

    /// <summary>
    /// Finalizes an intent. Never throws: a failed finalize leaves the intent for the
    /// reconciler.
    /// </summary>
    /// <param name="intent">The intent returned by <see cref="BeginAsync"/>.</param>
    /// <param name="completion">How the call ended.</param>
    ValueTask CompleteAsync(LatticeAdminAuditRecord intent, LatticeAdminAuditCompletion completion);
}
