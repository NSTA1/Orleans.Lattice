using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Schema;

/// <summary>
/// The Schema area's read-only inspection operations for a single governed tree: the
/// compliance audit (how many current values satisfy the tree's policy, and the
/// non-compliant breakdown by reason) and the strict-mode dead-letter queue (count
/// plus a bounded page of entries). Both fold a server denial or a transport failure
/// into a non-success envelope rather than throwing, so the panel degrades cleanly
/// and always has a message to show. Neither operation mutates any data.
/// </summary>
public interface ISchemaComplianceService
{
    /// <summary>Scans <paramref name="treeId"/> against its current policy and returns the audit.</summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<SchemaReadView<LatticeSchemaComplianceReport>> ScanComplianceAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>Reads the dead-letter count and a bounded page of entries for <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="maxEntries">The maximum number of entries to page in. Must be greater than zero.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<SchemaDeadLetterView> ListDeadLettersAsync(string treeId, int maxEntries, CancellationToken cancellationToken = default);
}
