using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Explorer.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.Schema;

/// <summary>
/// A hand-rolled <see cref="ISchemaAdminClient"/> fake that lets a test script the
/// outcome of each call: a canned value, a translated
/// <see cref="LatticeAuthorizationDeniedException"/> (a server denial), or a residual
/// <see cref="Grpc.Core.RpcException"/> (a transport failure). It also records the
/// last inputs so a test can assert the service forwarded them.
/// </summary>
internal sealed class FakeSchemaAdminClient : ISchemaAdminClient
{
    public LatticeSchemaPolicy? PolicyResult { get; set; }
    public bool ClearPolicyResult { get; set; } = true;
    public int DeadLetterCountResult { get; set; }
    public IReadOnlyList<LatticeSchemaDeadLetterEntry> DeadLettersResult { get; set; } =
        Array.Empty<LatticeSchemaDeadLetterEntry>();
    public LatticeSchemaVersionConfig? VersionConfigResult { get; set; }
    public LatticeSchemaVersionConfig AdvanceResult { get; set; } = new(1, 2);
    public LatticeSchemaRemediationReport RemediationResult { get; set; } = LatticeSchemaRemediationReport.Idle;
    public bool ClearVersionConfigResult { get; set; } = true;
    public LatticeSchemaComplianceReport ComplianceResult { get; set; } =
        LatticeSchemaComplianceReport.Ungoverned("t");
    public LatticeSchemaCapabilities CapabilitiesResult { get; set; } = new() { TreeId = "t" };

    public Exception? ReadThrows { get; set; }
    public Exception? MutationThrows { get; set; }
    public Exception? ProbeThrows { get; set; }

    public string? LastTreeId { get; private set; }
    public LatticeSchemaPolicy? LastSetPolicy { get; private set; }
    public LatticeSchemaVersionConfig? LastSetVersionConfig { get; private set; }
    public uint LastAdvanceTargetVersion { get; private set; }
    public int LastMaxEntries { get; private set; }
    public string? LastProbeTreeId { get; private set; }
    public int ProbeCallCount { get; private set; }

    public Task<LatticeSchemaPolicy?> GetPolicyAsync(string treeId, CancellationToken cancellationToken = default)
    {
        LastTreeId = treeId;
        if (ReadThrows is not null)
        {
            throw ReadThrows;
        }

        return Task.FromResult(PolicyResult);
    }

    public Task SetPolicyAsync(string treeId, LatticeSchemaPolicy policy, CancellationToken cancellationToken = default)
    {
        LastTreeId = treeId;
        LastSetPolicy = policy;
        if (MutationThrows is not null)
        {
            throw MutationThrows;
        }

        return Task.CompletedTask;
    }

    public Task<bool> ClearPolicyAsync(string treeId, CancellationToken cancellationToken = default)
    {
        LastTreeId = treeId;
        if (MutationThrows is not null)
        {
            throw MutationThrows;
        }

        return Task.FromResult(ClearPolicyResult);
    }

    public Task<int> CountDeadLettersAsync(string treeId, CancellationToken cancellationToken = default)
    {
        LastTreeId = treeId;
        if (ReadThrows is not null)
        {
            throw ReadThrows;
        }

        return Task.FromResult(DeadLetterCountResult);
    }

    public Task<IReadOnlyList<LatticeSchemaDeadLetterEntry>> ListDeadLettersAsync(
        string treeId, int maxEntries, CancellationToken cancellationToken = default)
    {
        LastTreeId = treeId;
        LastMaxEntries = maxEntries;
        if (ReadThrows is not null)
        {
            throw ReadThrows;
        }

        return Task.FromResult(DeadLettersResult);
    }

    public Task<LatticeSchemaVersionConfig?> GetVersionConfigAsync(string treeId, CancellationToken cancellationToken = default)
    {
        LastTreeId = treeId;
        if (ReadThrows is not null)
        {
            throw ReadThrows;
        }

        return Task.FromResult(VersionConfigResult);
    }

    public Task SetVersionConfigAsync(string treeId, LatticeSchemaVersionConfig config, CancellationToken cancellationToken = default)
    {
        LastTreeId = treeId;
        LastSetVersionConfig = config;
        if (MutationThrows is not null)
        {
            throw MutationThrows;
        }

        return Task.CompletedTask;
    }

    public Task<LatticeSchemaVersionConfig> AdvanceTargetVersionAsync(
        string treeId, uint newTargetVersion, CancellationToken cancellationToken = default)
    {
        LastTreeId = treeId;
        LastAdvanceTargetVersion = newTargetVersion;
        if (MutationThrows is not null)
        {
            throw MutationThrows;
        }

        return Task.FromResult(AdvanceResult);
    }

    public Task<LatticeSchemaRemediationReport> AdvanceAndMigrateAsync(
        string treeId, uint newTargetVersion, CancellationToken cancellationToken = default)
    {
        LastTreeId = treeId;
        LastAdvanceTargetVersion = newTargetVersion;
        if (MutationThrows is not null)
        {
            throw MutationThrows;
        }

        return Task.FromResult(RemediationResult);
    }

    public Task<LatticeSchemaRemediationReport> MigrateToTargetVersionAsync(string treeId, CancellationToken cancellationToken = default)
    {
        LastTreeId = treeId;
        if (MutationThrows is not null)
        {
            throw MutationThrows;
        }

        return Task.FromResult(RemediationResult);
    }

    public Task<bool> ClearVersionConfigAsync(string treeId, CancellationToken cancellationToken = default)
    {
        LastTreeId = treeId;
        if (MutationThrows is not null)
        {
            throw MutationThrows;
        }

        return Task.FromResult(ClearVersionConfigResult);
    }

    public Task<LatticeSchemaRemediationReport> GetRemediationStatusAsync(string treeId, CancellationToken cancellationToken = default)
    {
        LastTreeId = treeId;
        if (ReadThrows is not null)
        {
            throw ReadThrows;
        }

        return Task.FromResult(RemediationResult);
    }

    public Task<LatticeSchemaComplianceReport> ScanComplianceAsync(string treeId, CancellationToken cancellationToken = default)
    {
        LastTreeId = treeId;
        if (ReadThrows is not null)
        {
            throw ReadThrows;
        }

        return Task.FromResult(ComplianceResult);
    }

    public Task<LatticeSchemaCapabilities> ProbeCapabilitiesAsync(string treeId, CancellationToken cancellationToken = default)
    {
        LastProbeTreeId = treeId;
        ProbeCallCount++;
        if (ProbeThrows is not null)
        {
            throw ProbeThrows;
        }

        return Task.FromResult(CapabilitiesResult);
    }
}
