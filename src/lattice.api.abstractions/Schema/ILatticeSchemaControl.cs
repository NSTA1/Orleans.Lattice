using Orleans.Lattice;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Schema;

/// <summary>
/// Transport-agnostic schema-management control facade. Every transport binding
/// (the gRPC service, and any future MCP surface) is a thin adapter over this single
/// surface, so the control semantics - authorization and delegation to the
/// in-process schema admin plane - are written and tested once and no transport
/// concern leaks into the control logic.
/// </summary>
/// <remarks>
/// <para>
/// The facade wraps the four in-process schema admin surfaces
/// (<see cref="ILatticeSchemaAdmin"/>, <see cref="ILatticeSchemaVersionAdmin"/>,
/// <see cref="ILatticeSchemaRemediationAdmin"/>, and
/// <see cref="ILatticeSchemaComplianceAdmin"/>). Every operation authorizes
/// fail-closed <i>before</i> it touches the admin plane: a mutation (set / clear
/// policy, version-config change, remediation) authorizes on
/// <see cref="LatticeOperation.SchemaAdmin"/>, while a read (inspect policy /
/// version config / dead letters / remediation status, or the compliance audit)
/// authorizes on ordinary <see cref="LatticeOperation.Read"/> authority. Dead-letter
/// listing is streamed as <see cref="IAsyncEnumerable{T}"/> so a large queue
/// enumerates with bounded memory.
/// </para>
/// </remarks>
public interface ILatticeSchemaControl
{
    /// <summary>
    /// Sets or replaces the enforcement policy for <paramref name="treeId"/>, after
    /// authorizing schema-management on the tree fail-closed.
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c>, empty, or reserved.</param>
    /// <param name="policy">The policy to apply. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c>, empty, or reserved, or a rule is invalid.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="policy"/> is <c>null</c>.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to manage the tree's schema.</exception>
    Task SetPolicyAsync(string treeId, LatticeSchemaPolicy policy, CancellationToken cancellationToken = default);

    /// <summary>
    /// Clears the enforcement policy for <paramref name="treeId"/>, after authorizing
    /// schema-management on the tree fail-closed. Returns <c>true</c> when a policy
    /// was removed.
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><c>true</c> when a policy was removed; otherwise <c>false</c>.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to manage the tree's schema.</exception>
    Task<bool> ClearPolicyAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the enforcement policy for <paramref name="treeId"/>, or <c>null</c> when
    /// none exists, after authorizing read on the tree fail-closed.
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The policy, or <c>null</c> when none exists.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to read the tree's schema.</exception>
    Task<LatticeSchemaPolicy?> GetPolicyAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Streams the strict-mode dead-letter entries retained for
    /// <paramref name="treeId"/> with bounded memory, after authorizing read on the
    /// tree fail-closed.
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An async stream of dead-letter entries.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to read the tree's schema.</exception>
    IAsyncEnumerable<LatticeSchemaDeadLetterEntry> ListDeadLettersAsync(
        string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Counts the strict-mode dead-letter entries retained for
    /// <paramref name="treeId"/>, after authorizing read on the tree fail-closed.
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The dead-letter entry count.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to read the tree's schema.</exception>
    Task<int> CountDeadLettersAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Opts <paramref name="treeId"/> in to envelope versioning (or replaces its
    /// existing config), after authorizing schema-management on the tree fail-closed.
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c>, empty, or reserved.</param>
    /// <param name="config">The version configuration to install.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c>, empty, or reserved.</exception>
    /// <exception cref="InvalidOperationException">Schema versioning is not registered.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to manage the tree's schema.</exception>
    Task SetVersionConfigAsync(
        string treeId, LatticeSchemaVersionConfig config, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the current version config for <paramref name="treeId"/>, or <c>null</c>
    /// when the tree is unversioned, after authorizing read on the tree fail-closed.
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The version config, or <c>null</c> when the tree is unversioned.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="InvalidOperationException">Schema versioning is not registered.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to read the tree's schema.</exception>
    Task<LatticeSchemaVersionConfig?> GetVersionConfigAsync(
        string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Advances <paramref name="treeId"/>'s target schema version to
    /// <paramref name="newTargetVersion"/>, after authorizing schema-management on the
    /// tree fail-closed.
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c>, empty, or reserved.</param>
    /// <param name="newTargetVersion">The new target version. Must be greater than the current target.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The updated config.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c>, empty, or reserved.</exception>
    /// <exception cref="InvalidOperationException">The tree is unversioned, the target does not advance, or schema versioning is not registered.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to manage the tree's schema.</exception>
    Task<LatticeSchemaVersionConfig> AdvanceTargetVersionAsync(
        string treeId, uint newTargetVersion, CancellationToken cancellationToken = default);

    /// <summary>
    /// Advances <paramref name="treeId"/>'s target schema version to
    /// <paramref name="newTargetVersion"/> and kicks off a background eager migration,
    /// after authorizing schema-management on the tree fail-closed.
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c>, empty, or reserved.</param>
    /// <param name="newTargetVersion">The new target version. Must be greater than the current target.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The terminal migration report.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c>, empty, or reserved.</exception>
    /// <exception cref="InvalidOperationException">The tree is unversioned, the target does not advance, or schema versioning is not registered.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to manage the tree's schema.</exception>
    Task<LatticeSchemaRemediationReport> AdvanceAndMigrateAsync(
        string treeId, uint newTargetVersion, CancellationToken cancellationToken = default);

    /// <summary>
    /// Runs (or idempotently resumes / no-ops) an eager migration that re-stamps every
    /// existing value of <paramref name="treeId"/> to the tree's current target
    /// version, after authorizing schema-management on the tree fail-closed.
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c>, empty, or reserved.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The terminal migration report.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c>, empty, or reserved.</exception>
    /// <exception cref="InvalidOperationException">The tree is unversioned, or schema versioning is not registered.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to manage the tree's schema.</exception>
    Task<LatticeSchemaRemediationReport> MigrateToTargetVersionAsync(
        string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Opts <paramref name="treeId"/> back out of envelope versioning, after
    /// authorizing schema-management on the tree fail-closed. Returns <c>true</c> when
    /// a config was removed.
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><c>true</c> when a config was removed; <c>false</c> when the tree was already unversioned.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="InvalidOperationException">Schema versioning is not registered.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to manage the tree's schema.</exception>
    Task<bool> ClearVersionConfigAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Starts (or idempotently resumes) a background remediation of
    /// <paramref name="treeId"/>, after authorizing schema-management on the tree
    /// fail-closed.
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c>, empty, or reserved.</param>
    /// <param name="transform">The per-value remediation transform.</param>
    /// <param name="targetPolicy">The policy the transformed values must satisfy. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The terminal remediation report.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c>, empty, or reserved, or <paramref name="targetPolicy"/> is invalid.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="targetPolicy"/> is <c>null</c>.</exception>
    /// <exception cref="InvalidOperationException">A remediation with different parameters is already in flight.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to manage the tree's schema.</exception>
    Task<LatticeSchemaRemediationReport> RemediateAsync(
        string treeId,
        LatticeValueTransform transform,
        LatticeSchemaPolicy targetPolicy,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the current or last-known remediation status for
    /// <paramref name="treeId"/>, after authorizing read on the tree fail-closed.
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The remediation status report.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to read the tree's schema.</exception>
    Task<LatticeSchemaRemediationReport> GetRemediationStatusAsync(
        string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Scans every current value of <paramref name="treeId"/> against its current
    /// compiled policy and returns a per-tree compliance report, after authorizing
    /// read on the tree fail-closed. A pure read: it never mutates data. Cancellable,
    /// with best-effort progress via <see cref="LatticeSchemaComplianceReport.ScannedCount"/>.
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The compliance report.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to read the tree's schema.</exception>
    Task<LatticeSchemaComplianceReport> ScanComplianceAsync(
        string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Probes which schema-management operations the current caller may perform over
    /// <paramref name="treeId"/>, evaluated through the same fail-closed schema access
    /// gate the real operations use but with <b>no side effects</b>. Each denied
    /// capability is reported as a <see langword="false"/> flag, default-deny, so a
    /// management UI can grey out controls the caller cannot use. The reported flags
    /// are advisory; the server still authorizes each real operation on attempt.
    /// </summary>
    /// <param name="treeId">The tree to probe. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The caller's allowed schema-management operation set for <paramref name="treeId"/>.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    Task<LatticeSchemaCapabilities> ProbeCapabilitiesAsync(
        string treeId, CancellationToken cancellationToken = default);
}
