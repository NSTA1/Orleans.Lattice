using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Schema;

/// <summary>
/// The explorer's transport-facing view of the schema control plane: the policy,
/// versioning, remediation, compliance, dead-letter, and capability-probe surface
/// the Schema area drives, over a gRPC channel built from the current endpoint and
/// sign-in. Shaped like the <c>ILatticeSchemaControl</c> facade (returning the same
/// facade model records) rather than the raw gRPC envelopes, so the policy,
/// versioning, and compliance services can be unit-tested against a fake without any
/// transport dependency.
/// </summary>
/// <remarks>
/// Every call may surface a <see cref="LatticeAuthorizationDeniedException"/> when
/// the server denies the caller: the control plane is access-gated, so a caller
/// without schema authority is refused fail-closed. The production client translates
/// the gRPC <c>PermissionDenied</c> / <c>Unauthenticated</c> status back to this
/// typed exception, so callers handle a single denial shape even when an advisory
/// capability flag suggested the action was allowed.
/// </remarks>
public interface ISchemaAdminClient
{
    // ----- Policy -----

    /// <summary>Reads the enforcement policy for <paramref name="treeId"/>, or <see langword="null"/> when none exists.</summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<LatticeSchemaPolicy?> GetPolicyAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>Sets or replaces the enforcement policy for <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="policy">The policy to apply. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task SetPolicyAsync(string treeId, LatticeSchemaPolicy policy, CancellationToken cancellationToken = default);

    /// <summary>Clears the enforcement policy for <paramref name="treeId"/>. Returns <see langword="true"/> when a policy was removed.</summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<bool> ClearPolicyAsync(string treeId, CancellationToken cancellationToken = default);

    // ----- Dead letters -----

    /// <summary>Counts the strict-mode dead-letter entries retained for <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<int> CountDeadLettersAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads up to <paramref name="maxEntries"/> strict-mode dead-letter entries
    /// retained for <paramref name="treeId"/>, buffered into a bounded list.
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="maxEntries">The maximum number of entries to buffer. Must be greater than zero.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<IReadOnlyList<LatticeSchemaDeadLetterEntry>> ListDeadLettersAsync(
        string treeId, int maxEntries, CancellationToken cancellationToken = default);

    // ----- Versioning -----

    /// <summary>Reads the current version config for <paramref name="treeId"/>, or <see langword="null"/> when unversioned.</summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<LatticeSchemaVersionConfig?> GetVersionConfigAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>Opts <paramref name="treeId"/> in to envelope versioning (or replaces its config).</summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="config">The version configuration to install.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task SetVersionConfigAsync(string treeId, LatticeSchemaVersionConfig config, CancellationToken cancellationToken = default);

    /// <summary>Advances <paramref name="treeId"/>'s target schema version. Returns the updated config.</summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="newTargetVersion">The new target version. Must be greater than the current target.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<LatticeSchemaVersionConfig> AdvanceTargetVersionAsync(
        string treeId, uint newTargetVersion, CancellationToken cancellationToken = default);

    /// <summary>Advances <paramref name="treeId"/>'s target version and eagerly migrates. Returns the terminal report.</summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="newTargetVersion">The new target version. Must be greater than the current target.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<LatticeSchemaRemediationReport> AdvanceAndMigrateAsync(
        string treeId, uint newTargetVersion, CancellationToken cancellationToken = default);

    /// <summary>Migrates <paramref name="treeId"/> to its current target version. Returns the terminal report.</summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<LatticeSchemaRemediationReport> MigrateToTargetVersionAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>Opts <paramref name="treeId"/> back out of envelope versioning. Returns <see langword="true"/> when a config was removed.</summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<bool> ClearVersionConfigAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>Reads the current or last-known remediation status for <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<LatticeSchemaRemediationReport> GetRemediationStatusAsync(string treeId, CancellationToken cancellationToken = default);

    // ----- Compliance -----

    /// <summary>
    /// Scans every current value of <paramref name="treeId"/> against its current
    /// compiled policy and returns a per-tree compliance report. A pure read.
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<LatticeSchemaComplianceReport> ScanComplianceAsync(string treeId, CancellationToken cancellationToken = default);

    // ----- Capability probe -----

    /// <summary>
    /// Probes which schema-management operations the current caller may perform over
    /// <paramref name="treeId"/>, with no side effects.
    /// </summary>
    /// <param name="treeId">The tree to probe. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<LatticeSchemaCapabilities> ProbeCapabilitiesAsync(string treeId, CancellationToken cancellationToken = default);
}
