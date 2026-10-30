namespace Orleans.Lattice.Schema;

/// <summary>
/// The schema-remediation control plane: trigger a shadow-build-and-cutover that rewrites
/// a tree's existing values to satisfy a new policy, and observe its status. It is
/// the operator-facing surface over the internal durable remediation coordinator.
/// <para>
/// Remediation runs a read-only dry-run gate first: if any existing value cannot be
/// rewritten to satisfy the target policy, the build aborts with the first
/// offending key and reason and the original tree is left completely untouched (no
/// alias change, no policy change). Only a fully successful build cuts the logical
/// tree over to the remediated destination and installs the target policy.
/// </para>
/// <para>
/// On a versioning-enabled tree the transform sees each value as a client read
/// returns it (envelope stripped, upcast to the tree's target version), and each
/// remediated value is written back enveloped at that target version - the version
/// the transform ran on - so the tree cuts over to self-describing values: a later
/// target advance upcasts them, and an eager version migration upcasts them from
/// that stamp. A value of another schema family keeps its own stamp. An unversioned
/// tree's remediated values are written plain. A stored
/// value that cannot be upcast to the target - no registered upcaster hop, or a
/// value stamped newer than the target - is an offending value like any other: the
/// abort report is returned, not thrown, and the remediation releases its alias
/// reservation, so the tree is not left in flight.
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
    /// <returns>The terminal report: <see cref="LatticeSchemaRemediationReport.Succeeded"/> on cutover, or <see cref="LatticeSchemaRemediationReport.DidAbort"/> with the first offending entry, or with no offending entry and a reason naming the supersession when another alias change superseded the cutover.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c>, empty, or reserved, or <paramref name="targetPolicy"/> carries an uncompilable regex rule.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="targetPolicy"/> is <c>null</c>.</exception>
    /// <exception cref="InvalidOperationException">A remediation with different parameters is already in flight for the tree, or the call joined a remediation already in flight that then ended under another operation and was replaced by a later remediation before its outcome could be read.</exception>
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
