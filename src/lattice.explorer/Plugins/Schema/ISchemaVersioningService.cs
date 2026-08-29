using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Schema;

/// <summary>
/// The Schema area's envelope-versioning and remediation operations for a single
/// governed tree: reading the version config, opting a tree in or out, advancing the
/// target version (with or without an eager migration), migrating to the current
/// target, and reading the remediation status. Every member folds a server denial or
/// a transport failure into a non-success envelope rather than throwing, so the
/// panel degrades cleanly and always has a message to show.
/// </summary>
public interface ISchemaVersioningService
{
    /// <summary>Reads the version config for <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<SchemaReadView<LatticeSchemaVersionConfig>> GetVersionConfigAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>Opts <paramref name="treeId"/> in to envelope versioning (or replaces its config).</summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="config">The version configuration to install.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<SchemaOperationResult> SetVersionConfigAsync(string treeId, LatticeSchemaVersionConfig config, CancellationToken cancellationToken = default);

    /// <summary>Advances <paramref name="treeId"/>'s target schema version.</summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="newTargetVersion">The new target version. Must be greater than the current target.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<SchemaOperationResult> AdvanceTargetVersionAsync(string treeId, uint newTargetVersion, CancellationToken cancellationToken = default);

    /// <summary>Advances <paramref name="treeId"/>'s target version and eagerly migrates.</summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="newTargetVersion">The new target version. Must be greater than the current target.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<SchemaOperationResult> AdvanceAndMigrateAsync(string treeId, uint newTargetVersion, CancellationToken cancellationToken = default);

    /// <summary>Migrates <paramref name="treeId"/> to its current target version.</summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<SchemaOperationResult> MigrateToTargetVersionAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>Opts <paramref name="treeId"/> back out of envelope versioning.</summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<SchemaOperationResult> ClearVersionConfigAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>Reads the current or last-known remediation status for <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<SchemaReadView<LatticeSchemaRemediationReport>> GetRemediationStatusAsync(string treeId, CancellationToken cancellationToken = default);
}
