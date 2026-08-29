using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Schema.Domain;
using Orleans.Lattice.Explorer.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.Plugins.Schema;

/// <summary>
/// A scriptable <see cref="ISchemaPluginDomain"/> for the Schema plugin's
/// component tests: every call returns a canned answer immediately, so a test
/// drives a transition and reads the result with no delay, no polling, and no
/// dependence on timing.
/// </summary>
internal sealed class FakeSchemaPluginDomain : ISchemaPluginDomain
{
    private readonly ExplorerPluginAccessStore _access = new();

    public SchemaTreeCatalog Catalog { get; set; } = SchemaTreeCatalog.Empty;

    public SchemaCapabilitySnapshot Capabilities { get; set; } = SchemaCapabilitySnapshot.None;

    public SchemaReadView<LatticeSchemaPolicy> Policy { get; set; } =
        SchemaReadView<LatticeSchemaPolicy>.Succeeded(null);

    public SchemaReadView<LatticeSchemaVersionConfig> VersionConfig { get; set; } =
        SchemaReadView<LatticeSchemaVersionConfig>.Succeeded(default);

    public SchemaReadView<LatticeSchemaRemediationReport> Remediation { get; set; } =
        SchemaReadView<LatticeSchemaRemediationReport>.Succeeded(LatticeSchemaRemediationReport.Idle);

    public SchemaReadView<LatticeSchemaComplianceReport> Compliance { get; set; } =
        SchemaReadView<LatticeSchemaComplianceReport>.Succeeded(LatticeSchemaComplianceReport.Ungoverned("t"));

    public SchemaDeadLetterView DeadLetters { get; set; } =
        new() { Status = SchemaOperationStatus.Succeeded };

    public SchemaOperationResult MutationResult { get; set; } = SchemaOperationResult.Success("done");

    public int ListCallCount { get; private set; }

    public int ProbeCallCount { get; private set; }

    public int PolicyReadCount { get; private set; }

    public int ComplianceScanCount { get; private set; }

    public int DeadLetterReadCount { get; private set; }

    public string? LastProbedTreeId { get; private set; }

    public string? LastMutatedTreeId { get; private set; }

    public LatticeSchemaPolicy? LastSetPolicy { get; private set; }

    public LatticeSchemaVersionConfig? LastSetVersionConfig { get; private set; }

    public uint LastAdvanceTargetVersion { get; private set; }

    public int LastDeadLetterPageSize { get; private set; }

    public Task<SchemaTreeCatalog> ListGovernableTreesAsync(CancellationToken cancellationToken = default)
    {
        ListCallCount++;
        return Task.FromResult(Catalog);
    }

    public Task<SchemaTreeGrants> ProbeTreeAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ProbeCallCount++;
        LastProbedTreeId = treeId;

        Publish(treeId, SchemaCapability.ViewPolicy, Capabilities.CanViewPolicy);
        Publish(treeId, SchemaCapability.ManagePolicy, Capabilities.CanManagePolicy);
        Publish(treeId, SchemaCapability.ViewVersionConfig, Capabilities.CanViewVersionConfig);
        Publish(treeId, SchemaCapability.ManageVersion, Capabilities.CanManageVersion);
        Publish(treeId, SchemaCapability.ViewRemediationStatus, Capabilities.CanViewRemediationStatus);
        Publish(treeId, SchemaCapability.Remediate, Capabilities.CanRemediate);
        Publish(treeId, SchemaCapability.ScanCompliance, Capabilities.CanScanCompliance);
        Publish(treeId, SchemaCapability.ViewDeadLetters, Capabilities.CanViewDeadLetters);

        return Task.FromResult(SchemaTreeGrants.For(_access, treeId));
    }

    public Task<SchemaReadView<LatticeSchemaPolicy>> GetPolicyAsync(string treeId, CancellationToken cancellationToken = default)
    {
        PolicyReadCount++;
        return Task.FromResult(Policy);
    }

    public Task<SchemaOperationResult> SetPolicyAsync(string treeId, LatticeSchemaPolicy policy, CancellationToken cancellationToken = default)
    {
        LastMutatedTreeId = treeId;
        LastSetPolicy = policy;
        return Task.FromResult(MutationResult);
    }

    public Task<SchemaOperationResult> ClearPolicyAsync(string treeId, CancellationToken cancellationToken = default)
    {
        LastMutatedTreeId = treeId;
        return Task.FromResult(MutationResult);
    }

    public Task<SchemaReadView<LatticeSchemaVersionConfig>> GetVersionConfigAsync(string treeId, CancellationToken cancellationToken = default) =>
        Task.FromResult(VersionConfig);

    public Task<SchemaOperationResult> SetVersionConfigAsync(string treeId, LatticeSchemaVersionConfig config, CancellationToken cancellationToken = default)
    {
        LastMutatedTreeId = treeId;
        LastSetVersionConfig = config;
        return Task.FromResult(MutationResult);
    }

    public Task<SchemaOperationResult> AdvanceTargetVersionAsync(string treeId, uint newTargetVersion, CancellationToken cancellationToken = default)
    {
        LastMutatedTreeId = treeId;
        LastAdvanceTargetVersion = newTargetVersion;
        return Task.FromResult(MutationResult);
    }

    public Task<SchemaOperationResult> AdvanceAndMigrateAsync(string treeId, uint newTargetVersion, CancellationToken cancellationToken = default)
    {
        LastMutatedTreeId = treeId;
        LastAdvanceTargetVersion = newTargetVersion;
        return Task.FromResult(MutationResult);
    }

    public Task<SchemaOperationResult> MigrateToTargetVersionAsync(string treeId, CancellationToken cancellationToken = default)
    {
        LastMutatedTreeId = treeId;
        return Task.FromResult(MutationResult);
    }

    public Task<SchemaOperationResult> ClearVersionConfigAsync(string treeId, CancellationToken cancellationToken = default)
    {
        LastMutatedTreeId = treeId;
        return Task.FromResult(MutationResult);
    }

    public Task<SchemaReadView<LatticeSchemaRemediationReport>> GetRemediationStatusAsync(string treeId, CancellationToken cancellationToken = default) =>
        Task.FromResult(Remediation);

    public Task<SchemaReadView<LatticeSchemaComplianceReport>> ScanComplianceAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ComplianceScanCount++;
        return Task.FromResult(Compliance);
    }

    public Task<SchemaDeadLetterView> ListDeadLettersAsync(string treeId, int maxEntries, CancellationToken cancellationToken = default)
    {
        DeadLetterReadCount++;
        LastDeadLetterPageSize = maxEntries;
        return Task.FromResult(DeadLetters);
    }

    private void Publish(string treeId, SchemaCapability capability, bool permitted) =>
        _access.Set(
            SchemaTreeGrants.KeyFor(treeId, capability),
            permitted ? ExplorerPluginAccess.Allowed : ExplorerPluginAccess.Denied);
}
