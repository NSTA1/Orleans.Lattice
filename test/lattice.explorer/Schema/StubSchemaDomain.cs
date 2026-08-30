using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Schema;
using Orleans.Lattice.Explorer.Schema.Domain;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.Schema;

/// <summary>
/// A scripted <see cref="ISchemaPluginDomain"/>: every read answers from a
/// value handed in up front, so a render never reaches a cluster, a clock, or a
/// background task.
/// </summary>
/// <remarks>
/// The Schema plugin's whole reach is this one contract (epic decision D3), so
/// stubbing it is the complete substitution boundary - the component under test
/// is the real one and nothing behind it is.
/// </remarks>
internal sealed class StubSchemaDomain : ISchemaPluginDomain
{
    /// <summary>The policy the tab reads.</summary>
    public SchemaReadView<LatticeSchemaPolicy> Policy { get; set; } =
        SchemaReadView<LatticeSchemaPolicy>.Succeeded(null);

    /// <summary>The compliance report the audit returns.</summary>
    public SchemaReadView<LatticeSchemaComplianceReport> Compliance { get; set; } =
        SchemaReadView<LatticeSchemaComplianceReport>.Succeeded(default);

    /// <summary>The dead-letter page the queue reads.</summary>
    public SchemaDeadLetterView DeadLetters { get; set; } =
        new() { Status = SchemaOperationStatus.Succeeded };

    /// <inheritdoc />
    public Task<SchemaTreeCatalog> ListGovernableTreesAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult(SchemaTreeCatalog.Empty);

    /// <inheritdoc />
    public Task<SchemaTreeGrants> ProbeTreeAsync(string treeId, CancellationToken cancellationToken = default) =>
        Task.FromResult(SchemaTreeGrants.None);

    /// <inheritdoc />
    public Task<SchemaReadView<LatticeSchemaPolicy>> GetPolicyAsync(
        string treeId, CancellationToken cancellationToken = default) =>
        Task.FromResult(Policy);

    /// <inheritdoc />
    public Task<SchemaOperationResult> SetPolicyAsync(
        string treeId, LatticeSchemaPolicy policy, CancellationToken cancellationToken = default) =>
        Task.FromResult(SchemaOperationResult.Success("ok"));

    /// <inheritdoc />
    public Task<SchemaOperationResult> ClearPolicyAsync(
        string treeId, CancellationToken cancellationToken = default) =>
        Task.FromResult(SchemaOperationResult.Success("ok"));

    /// <inheritdoc />
    public Task<SchemaReadView<LatticeSchemaVersionConfig>> GetVersionConfigAsync(
        string treeId, CancellationToken cancellationToken = default) =>
        Task.FromResult(SchemaReadView<LatticeSchemaVersionConfig>.Succeeded(default));

    /// <inheritdoc />
    public Task<SchemaOperationResult> SetVersionConfigAsync(
        string treeId, LatticeSchemaVersionConfig config, CancellationToken cancellationToken = default) =>
        Task.FromResult(SchemaOperationResult.Success("ok"));

    /// <inheritdoc />
    public Task<SchemaOperationResult> AdvanceTargetVersionAsync(
        string treeId, uint newTargetVersion, CancellationToken cancellationToken = default) =>
        Task.FromResult(SchemaOperationResult.Success("ok"));

    /// <inheritdoc />
    public Task<SchemaOperationResult> AdvanceAndMigrateAsync(
        string treeId, uint newTargetVersion, CancellationToken cancellationToken = default) =>
        Task.FromResult(SchemaOperationResult.Success("ok"));

    /// <inheritdoc />
    public Task<SchemaOperationResult> MigrateToTargetVersionAsync(
        string treeId, CancellationToken cancellationToken = default) =>
        Task.FromResult(SchemaOperationResult.Success("ok"));

    /// <inheritdoc />
    public Task<SchemaOperationResult> ClearVersionConfigAsync(
        string treeId, CancellationToken cancellationToken = default) =>
        Task.FromResult(SchemaOperationResult.Success("ok"));

    /// <inheritdoc />
    public Task<SchemaReadView<LatticeSchemaRemediationReport>> GetRemediationStatusAsync(
        string treeId, CancellationToken cancellationToken = default) =>
        Task.FromResult(SchemaReadView<LatticeSchemaRemediationReport>.Succeeded(default));

    /// <inheritdoc />
    public Task<SchemaReadView<LatticeSchemaComplianceReport>> ScanComplianceAsync(
        string treeId, CancellationToken cancellationToken = default) =>
        Task.FromResult(Compliance);

    /// <inheritdoc />
    public Task<SchemaDeadLetterView> ListDeadLettersAsync(
        string treeId, int maxEntries, CancellationToken cancellationToken = default) =>
        Task.FromResult(DeadLetters);
}
