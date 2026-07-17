namespace Orleans.Lattice.Schema;

/// <summary>
/// The read-only compliance-audit control plane: for a given tree, scan its current
/// values against its current compiled enforcement policy and report per-tree counts
/// of compliant vs non-compliant values, broken down by failing rule reason. It is
/// the diagnostic, non-mutating sibling of <see cref="ILatticeSchemaRemediationAdmin"/>:
/// where remediation rewrites data, the audit only observes it. Inspecting compliance
/// stays on ordinary read authority.
/// </summary>
public interface ILatticeSchemaComplianceAdmin
{
    /// <summary>
    /// Scans every current value of <paramref name="treeId"/> against the tree's
    /// current compiled policy and returns a <see cref="LatticeSchemaComplianceReport"/>
    /// summarizing how many values are compliant, how many are not, and - grouped by
    /// failure reason - which rules the non-compliant values break. When the tree is
    /// ungoverned (no policy) the audit is a no-op that returns an ungoverned report.
    /// The scan is a pure read: it never mutates data. It is cancellable and reports
    /// best-effort progress via <see cref="LatticeSchemaComplianceReport.ScannedCount"/>.
    /// (Read authority.)
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the scan.</param>
    /// <returns>The compliance report.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
    Task<LatticeSchemaComplianceReport> ScanComplianceAsync(
        string treeId, CancellationToken cancellationToken = default);
}
