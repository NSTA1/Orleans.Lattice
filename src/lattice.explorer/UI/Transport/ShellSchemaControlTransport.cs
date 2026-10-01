using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.Schema.Grpc;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>
/// The Shell's <see cref="ILatticeSchemaControl"/> over gRPC: a per-circuit adapter
/// over <see cref="LatticeSchemaApiGrpcClient"/>, ported from the Schema plugin's
/// <c>GrpcSchemaAdminClient</c>. Faults map through <see cref="ShellTransportFaults"/>.
/// </summary>
/// <param name="channel">The circuit's transport channel.</param>
internal sealed partial class ShellSchemaControlTransport(ShellTransportChannel channel)
    : ShellTransportAdapter<LatticeSchemaApiGrpcClient>(channel, LatticeSchemaApiGrpcClient.Create), ILatticeSchemaControl
{
    /// <inheritdoc />
    public Task SetPolicyAsync(string treeId, LatticeSchemaPolicy policy, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(policy);
        return CallAsync(
            (TreeId: treeId, Policy: policy),
            static (client, state, ct) => client.SetPolicyAsync(state.TreeId, state.Policy, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<bool> ClearPolicyAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(treeId, static (client, state, ct) => client.ClearPolicyAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeSchemaPolicy?> GetPolicyAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(treeId, static (client, state, ct) => client.GetPolicyAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public IAsyncEnumerable<LatticeSchemaDeadLetterEntry> ListDeadLettersAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return StreamAsync(treeId, static (client, state, ct) => client.ListDeadLettersAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task<int> CountDeadLettersAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(treeId, static (client, state, ct) => client.CountDeadLettersAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task SetVersionConfigAsync(string treeId, LatticeSchemaVersionConfig config, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            (TreeId: treeId, Config: config),
            static (client, state, ct) => client.SetVersionConfigAsync(state.TreeId, state.Config, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeSchemaVersionConfig?> GetVersionConfigAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(treeId, static (client, state, ct) => client.GetVersionConfigAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeSchemaVersionConfig> AdvanceTargetVersionAsync(
        string treeId,
        uint newTargetVersion,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            (TreeId: treeId, Version: newTargetVersion),
            static (client, state, ct) => client.AdvanceTargetVersionAsync(state.TreeId, state.Version, ct),
            null,
            cancellationToken);
    }

    // The shipped ILatticeSchemaControl still carries the deprecated blocking verbs
    // (LATTICE0002), so this adapter forwards them to the client's deprecated calls.
    // Nothing in the Explorer calls them: it starts tracked operations instead.
#pragma warning disable LATTICE0002
    /// <inheritdoc />
    public Task<LatticeSchemaRemediationReport> AdvanceAndMigrateAsync(
        string treeId,
        uint newTargetVersion,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            (TreeId: treeId, Version: newTargetVersion),
            static (client, state, ct) => client.AdvanceAndMigrateAsync(state.TreeId, state.Version, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeSchemaRemediationReport> MigrateToTargetVersionAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(treeId, static (client, state, ct) => client.MigrateToTargetVersionAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task<bool> ClearVersionConfigAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(treeId, static (client, state, ct) => client.ClearVersionConfigAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeSchemaRemediationReport> RemediateAsync(
        string treeId,
        LatticeValueTransform transform,
        LatticeSchemaPolicy targetPolicy,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(targetPolicy);
        return CallAsync(
            (TreeId: treeId, Transform: transform, Policy: targetPolicy),
            static (client, state, ct) => client.RemediateAsync(state.TreeId, state.Transform, state.Policy, ct),
            null,
            cancellationToken);
    }
#pragma warning restore LATTICE0002

    /// <inheritdoc />
    public Task<LatticeSchemaRemediationReport> GetRemediationStatusAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(treeId, static (client, state, ct) => client.GetRemediationStatusAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeSchemaComplianceReport> ScanComplianceAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(treeId, static (client, state, ct) => client.ScanComplianceAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeSchemaCapabilities> ProbeCapabilitiesAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(treeId, static (client, state, ct) => client.ProbeCapabilitiesAsync(state, ct), null, cancellationToken);
    }
}
