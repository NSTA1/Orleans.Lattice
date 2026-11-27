namespace Orleans.Lattice;

/// <summary>
/// The anchor record an audited administrative action writes: the single key
/// whose write is the action's commit point. The audit trail reads the anchor's
/// version before the commit so a call interrupted between commit and finalize can
/// later be resolved from the anchor's history, and points the final record at the
/// anchor revision instead of copying the changed state.
/// </summary>
/// <param name="TreeId">The tree holding the anchor record.</param>
/// <param name="Key">The anchor record's key.</param>
internal readonly record struct LatticeAdminAuditAnchor(string TreeId, string Key);
