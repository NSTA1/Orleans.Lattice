namespace Orleans.Lattice.Schema;

/// <summary>
/// The <see cref="LatticeOperation.SchemaAdmin"/>-gated control plane for
/// background schema remediation: trigger a shadow-build-and-cutover that rewrites
/// a tree's existing values to satisfy a new policy, and observe its status. It is
/// the operator-facing surface over the internal durable remediation coordinator.
/// <para>
/// Remediation runs a read-only dry-run gate first: if any existing value cannot be
/// rewritten to satisfy the target policy, the build aborts with the first
/// offending key and reason and the original tree is left completely untouched (no
/// alias change, no policy change). Only a fully successful build cuts the logical
/// tree over to the remediated destination and installs the target policy.
/// </para>
/// </summary>
public interface ILatticeSchemaRemediationAdmin
{
    /// <summary>
    /// Starts (or idempotently resumes) a background remediation of
    /// <paramref name="treeId"/>: rewrite every existing value with
    /// <paramref name="transform"/>, revalidate it against
    /// <paramref name="targetPolicy"/>, and - only if every value remediates - cut
    /// the tree over to the remediated data and install the policy. Returns the
    /// terminal report. (<see cref="LatticeOperation.SchemaAdmin"/>.)
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c>, empty, or reserved.</param>
    /// <param name="transform">The per-value remediation transform.</param>
    /// <param name="targetPolicy">The policy the transformed values must satisfy. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the operation.</param>
    /// <returns>The terminal report: <see cref="LatticeSchemaRemediationReport.Succeeded"/> on cutover, or <see cref="LatticeSchemaRemediationReport.DidAbort"/> with the first offending entry.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c>, empty, or reserved, or <paramref name="targetPolicy"/> carries an uncompilable regex rule.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="targetPolicy"/> is <c>null</c>.</exception>
    /// <exception cref="InvalidOperationException">A remediation with different parameters is already in flight for the tree.</exception>
    Task<LatticeSchemaRemediationReport> RemediateAsync(
        string treeId,
        LatticeValueTransform transform,
        LatticeSchemaPolicy targetPolicy,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the current or last-known remediation status for
    /// <paramref name="treeId"/>. (Read authority.)
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    Task<LatticeSchemaRemediationReport> GetRemediationStatusAsync(
        string treeId, CancellationToken cancellationToken = default);
}
