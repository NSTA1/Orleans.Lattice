using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.Schema.Grpc;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Remote-host adapter that implements the schema-management control facade
/// (<see cref="ILatticeSchemaControl"/>) by delegating to the schema-API gRPC
/// client (<see cref="LatticeSchemaApiGrpcClient"/>), so the topology-agnostic
/// tree-administration schema tools work unchanged against a cluster reached over
/// gRPC. Every facade member has full gRPC parity, so this adapter is a pure
/// pass-through: each call forwards its arguments and returns the client result
/// verbatim, and cancellation flows through every call.
/// </summary>
/// <remarks>
/// The adapter adds no authorization of its own: the caller credential is stamped
/// onto every outbound request by the shared credential-forwarding interceptor on
/// the routing invoker, and the remote cluster re-runs the facade's own
/// fail-closed schema access gate (schema-admin authority for a mutation, read
/// authority for an inspect). The gRPC client already projects the wire messages
/// back onto the abstractions DTOs, so no per-member marshalling is needed here.
/// </remarks>
internal sealed class GrpcLatticeSchemaControl : ILatticeSchemaControl
{
    private readonly LatticeSchemaApiGrpcClient _client;

    /// <summary>Initialises the adapter over the supplied schema-API gRPC client.</summary>
    public GrpcLatticeSchemaControl(LatticeSchemaApiGrpcClient client)
    {
        ArgumentNullException.ThrowIfNull(client);
        _client = client;
    }

    /// <inheritdoc />
    public Task SetPolicyAsync(string treeId, LatticeSchemaPolicy policy, CancellationToken cancellationToken = default)
        => _client.SetPolicyAsync(treeId, policy, cancellationToken);

    /// <inheritdoc />
    public Task<bool> ClearPolicyAsync(string treeId, CancellationToken cancellationToken = default)
        => _client.ClearPolicyAsync(treeId, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeSchemaPolicy?> GetPolicyAsync(string treeId, CancellationToken cancellationToken = default)
        => _client.GetPolicyAsync(treeId, cancellationToken);

    /// <inheritdoc />
    public IAsyncEnumerable<LatticeSchemaDeadLetterEntry> ListDeadLettersAsync(
        string treeId, CancellationToken cancellationToken = default)
        => _client.ListDeadLettersAsync(treeId, cancellationToken);

    /// <inheritdoc />
    public Task<int> CountDeadLettersAsync(string treeId, CancellationToken cancellationToken = default)
        => _client.CountDeadLettersAsync(treeId, cancellationToken);

    /// <inheritdoc />
    public Task SetVersionConfigAsync(
        string treeId, LatticeSchemaVersionConfig config, CancellationToken cancellationToken = default)
        => _client.SetVersionConfigAsync(treeId, config, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeSchemaVersionConfig?> GetVersionConfigAsync(
        string treeId, CancellationToken cancellationToken = default)
        => _client.GetVersionConfigAsync(treeId, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeSchemaVersionConfig> AdvanceTargetVersionAsync(
        string treeId, uint newTargetVersion, CancellationToken cancellationToken = default)
        => _client.AdvanceTargetVersionAsync(treeId, newTargetVersion, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeSchemaRemediationReport> AdvanceAndMigrateAsync(
        string treeId, uint newTargetVersion, CancellationToken cancellationToken = default)
        => _client.AdvanceAndMigrateAsync(treeId, newTargetVersion, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeSchemaRemediationReport> MigrateToTargetVersionAsync(
        string treeId, CancellationToken cancellationToken = default)
        => _client.MigrateToTargetVersionAsync(treeId, cancellationToken);

    /// <inheritdoc />
    public Task<bool> ClearVersionConfigAsync(string treeId, CancellationToken cancellationToken = default)
        => _client.ClearVersionConfigAsync(treeId, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeSchemaRemediationReport> RemediateAsync(
        string treeId,
        LatticeValueTransform transform,
        LatticeSchemaPolicy targetPolicy,
        CancellationToken cancellationToken = default)
        => _client.RemediateAsync(treeId, transform, targetPolicy, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeSchemaRemediationReport> GetRemediationStatusAsync(
        string treeId, CancellationToken cancellationToken = default)
        => _client.GetRemediationStatusAsync(treeId, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeSchemaComplianceReport> ScanComplianceAsync(
        string treeId, CancellationToken cancellationToken = default)
        => _client.ScanComplianceAsync(treeId, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeSchemaCapabilities> ProbeCapabilitiesAsync(
        string treeId, CancellationToken cancellationToken = default)
        => _client.ProbeCapabilitiesAsync(treeId, cancellationToken);
}
