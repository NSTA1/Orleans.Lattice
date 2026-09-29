using System.Runtime.CompilerServices;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

/// <summary>
/// A scripted <see cref="ILatticeSchemaControl"/>: per-tree policies, version
/// configs, dead letters, remediation status and capabilities, a fault per verb,
/// and gates that hold the long-running verbs until a test releases them, so no
/// test depends on timing.
/// </summary>
internal sealed class FakeSchemaControl : ILatticeSchemaControl
{
    /// <summary>The policies, by tree.</summary>
    public Dictionary<string, LatticeSchemaPolicy> Policies { get; } = new(StringComparer.Ordinal);

    /// <summary>The version configs, by tree.</summary>
    public Dictionary<string, LatticeSchemaVersionConfig> Versions { get; } = new(StringComparer.Ordinal);

    /// <summary>The dead letters, by tree.</summary>
    public Dictionary<string, List<LatticeSchemaDeadLetterEntry>> DeadLetters { get; } = new(StringComparer.Ordinal);

    /// <summary>The remediation status, by tree; absent reads as idle.</summary>
    public Dictionary<string, LatticeSchemaRemediationReport> Status { get; } = new(StringComparer.Ordinal);

    /// <summary>The compliance report a scan returns, by tree; absent reads as ungoverned.</summary>
    public Dictionary<string, LatticeSchemaComplianceReport> Compliance { get; } = new(StringComparer.Ordinal);

    /// <summary>Per-tree capabilities; a tree without an entry gets <see cref="DefaultCapabilities"/>.</summary>
    public Dictionary<string, Func<string, LatticeSchemaCapabilities>> Capabilities { get; } = new(StringComparer.Ordinal);

    /// <summary>The capabilities of a tree without its own entry; every flag granted by default.</summary>
    public Func<string, LatticeSchemaCapabilities> DefaultCapabilities { get; set; } = All;

    /// <summary>A fault a verb throws, by verb name (such as <c>GetPolicy</c>).</summary>
    public Dictionary<string, Exception> Faults { get; } = new(StringComparer.Ordinal);

    /// <summary>Whether the versioning add-on is registered; when not, every version verb throws <see cref="InvalidOperationException"/>.</summary>
    public bool VersioningRegistered { get; set; } = true;

    /// <summary>When set, a migration, an advance and migrate, and a remediation wait for it.</summary>
    public TaskCompletionSource<LatticeSchemaRemediationReport>? OperationGate { get; set; }

    /// <summary>When set, a compliance scan waits for it.</summary>
    public TaskCompletionSource<LatticeSchemaComplianceReport>? ScanGate { get; set; }

    /// <summary>Every call, as <c>Verb:tree</c>.</summary>
    public List<string> Calls { get; } = [];

    /// <summary>The last remediation's transform and target policy.</summary>
    public (LatticeValueTransform Transform, LatticeSchemaPolicy Policy)? LastRemediation { get; private set; }

    /// <summary>Every capability granted.</summary>
    /// <param name="treeId">The tree.</param>
    /// <returns>The capabilities.</returns>
    public static LatticeSchemaCapabilities All(string treeId) => new()
    {
        TreeId = treeId,
        CanViewPolicy = true,
        CanManagePolicy = true,
        CanViewVersionConfig = true,
        CanManageVersion = true,
        CanViewRemediationStatus = true,
        CanRemediate = true,
        CanScanCompliance = true,
        CanViewDeadLetters = true,
    };

    /// <summary>Read-only capabilities: every view and the scan, nothing that changes.</summary>
    /// <param name="treeId">The tree.</param>
    /// <returns>The capabilities.</returns>
    public static LatticeSchemaCapabilities ReadOnly(string treeId) => All(treeId) with
    {
        CanManagePolicy = false,
        CanManageVersion = false,
        CanRemediate = false,
    };

    /// <summary>No capability.</summary>
    /// <param name="treeId">The tree.</param>
    /// <returns>The capabilities.</returns>
    public static LatticeSchemaCapabilities None(string treeId) => new() { TreeId = treeId };

    /// <inheritdoc />
    public Task SetPolicyAsync(string treeId, LatticeSchemaPolicy policy, CancellationToken cancellationToken = default)
    {
        Record("SetPolicy", treeId);
        Policies[treeId] = policy;
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task<bool> ClearPolicyAsync(string treeId, CancellationToken cancellationToken = default)
    {
        Record("ClearPolicy", treeId);
        return Task.FromResult(Policies.Remove(treeId));
    }

    /// <inheritdoc />
    public Task<LatticeSchemaPolicy?> GetPolicyAsync(string treeId, CancellationToken cancellationToken = default)
    {
        Record("GetPolicy", treeId);
        return Task.FromResult(Policies.GetValueOrDefault(treeId));
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<LatticeSchemaDeadLetterEntry> ListDeadLettersAsync(
        string treeId, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        Record("ListDeadLetters", treeId);
        await Task.CompletedTask;
        foreach (var entry in DeadLetters.GetValueOrDefault(treeId) ?? [])
        {
            yield return entry;
        }
    }

    /// <inheritdoc />
    public Task<int> CountDeadLettersAsync(string treeId, CancellationToken cancellationToken = default)
    {
        Record("CountDeadLetters", treeId);
        return Task.FromResult(DeadLetters.GetValueOrDefault(treeId)?.Count ?? 0);
    }

    /// <inheritdoc />
    public Task SetVersionConfigAsync(string treeId, LatticeSchemaVersionConfig config, CancellationToken cancellationToken = default)
    {
        RecordVersion("SetVersionConfig", treeId);
        Versions[treeId] = config;
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task<LatticeSchemaVersionConfig?> GetVersionConfigAsync(string treeId, CancellationToken cancellationToken = default)
    {
        RecordVersion("GetVersionConfig", treeId);
        return Task.FromResult<LatticeSchemaVersionConfig?>(Versions.TryGetValue(treeId, out var config) ? config : null);
    }

    /// <inheritdoc />
    public Task<LatticeSchemaVersionConfig> AdvanceTargetVersionAsync(string treeId, uint newTargetVersion, CancellationToken cancellationToken = default)
    {
        RecordVersion("AdvanceTargetVersion", treeId);
        return Task.FromResult(Advance(treeId, newTargetVersion));
    }

    /// <inheritdoc />
    public async Task<LatticeSchemaRemediationReport> AdvanceAndMigrateAsync(string treeId, uint newTargetVersion, CancellationToken cancellationToken = default)
    {
        RecordVersion("AdvanceAndMigrate", treeId);
        Advance(treeId, newTargetVersion);
        return await RunAsync(treeId, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<LatticeSchemaRemediationReport> MigrateToTargetVersionAsync(string treeId, CancellationToken cancellationToken = default)
    {
        RecordVersion("MigrateToTargetVersion", treeId);
        return await RunAsync(treeId, cancellationToken);
    }

    /// <inheritdoc />
    public Task<bool> ClearVersionConfigAsync(string treeId, CancellationToken cancellationToken = default)
    {
        RecordVersion("ClearVersionConfig", treeId);
        return Task.FromResult(Versions.Remove(treeId));
    }

    /// <inheritdoc />
    public async Task<LatticeSchemaRemediationReport> RemediateAsync(
        string treeId,
        LatticeValueTransform transform,
        LatticeSchemaPolicy targetPolicy,
        CancellationToken cancellationToken = default)
    {
        Record("Remediate", treeId);
        LastRemediation = (transform, targetPolicy);
        return await RunAsync(treeId, cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeSchemaRemediationReport> GetRemediationStatusAsync(string treeId, CancellationToken cancellationToken = default)
    {
        Record("GetRemediationStatus", treeId);
        return Task.FromResult(Status.TryGetValue(treeId, out var report) ? report : LatticeSchemaRemediationReport.Idle);
    }

    /// <inheritdoc />
    public async Task<LatticeSchemaComplianceReport> ScanComplianceAsync(string treeId, CancellationToken cancellationToken = default)
    {
        Record("ScanCompliance", treeId);
        if (ScanGate is { } gate)
        {
            return await gate.Task.WaitAsync(cancellationToken);
        }

        return Compliance.TryGetValue(treeId, out var report) ? report : LatticeSchemaComplianceReport.Ungoverned(treeId);
    }

    /// <inheritdoc />
    public Task<LatticeSchemaCapabilities> ProbeCapabilitiesAsync(string treeId, CancellationToken cancellationToken = default)
    {
        Record("ProbeCapabilities", treeId);
        var capabilities = Capabilities.TryGetValue(treeId, out var own) ? own : DefaultCapabilities;
        return Task.FromResult(capabilities(treeId));
    }

    /// <summary>How many times <paramref name="verb"/> was called.</summary>
    /// <param name="verb">The verb, such as <c>GetPolicy</c>.</param>
    /// <returns>The count.</returns>
    public int CountOf(string verb) => Calls.Count(call => call.StartsWith(verb + ":", StringComparison.Ordinal));

    private void Record(string verb, string treeId)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        lock (Calls)
        {
            Calls.Add(verb + ":" + treeId);
        }

        if (Faults.TryGetValue(verb, out var fault))
        {
            throw fault;
        }
    }

    private void RecordVersion(string verb, string treeId)
    {
        Record(verb, treeId);
        if (!VersioningRegistered)
        {
            throw new InvalidOperationException("Schema versioning is not registered.");
        }
    }

    private LatticeSchemaVersionConfig Advance(string treeId, uint target)
    {
        if (!Versions.TryGetValue(treeId, out var current) || target <= current.TargetVersion)
        {
            throw new InvalidOperationException("The target version does not advance.");
        }

        var advanced = current with { TargetVersion = target };
        Versions[treeId] = advanced;
        return advanced;
    }

    private async Task<LatticeSchemaRemediationReport> RunAsync(string treeId, CancellationToken cancellationToken)
    {
        if (OperationGate is { } gate)
        {
            var report = await gate.Task.WaitAsync(cancellationToken);
            Status[treeId] = report;
            return report;
        }

        var completed = LatticeSchemaRemediationReport.Completed(10, "physical-shadow-of-" + treeId, "op-1");
        Status[treeId] = completed;
        return completed;
    }
}
