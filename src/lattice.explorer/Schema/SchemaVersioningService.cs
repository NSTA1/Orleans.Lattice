using Grpc.Core;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Schema;

/// <summary>
/// The default <see cref="ISchemaVersioningService"/> over an
/// <see cref="ISchemaAdminClient"/>. Every action catches a
/// <see cref="LatticeAuthorizationDeniedException"/> (the translated server denial)
/// and a residual <see cref="RpcException"/> and returns a non-success envelope, so
/// the Schema UI degrades cleanly and never leaks an exception.
/// </summary>
public sealed class SchemaVersioningService(ISchemaAdminClient client) : ISchemaVersioningService
{
    private readonly ISchemaAdminClient _client = client ?? throw new ArgumentNullException(nameof(client));

    /// <inheritdoc />
    public async Task<SchemaReadView<LatticeSchemaVersionConfig>> GetVersionConfigAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        try
        {
            var config = await _client.GetVersionConfigAsync(treeId, cancellationToken).ConfigureAwait(false);
            return config is { } value
                ? SchemaReadView<LatticeSchemaVersionConfig>.Succeeded(value)
                : SchemaReadView<LatticeSchemaVersionConfig>.Succeeded(default);
        }
        catch (LatticeAuthorizationDeniedException ex)
        {
            return SchemaReadView<LatticeSchemaVersionConfig>.Denied(SchemaAdminFault.DenialMessage(ex));
        }
        catch (RpcException ex)
        {
            return SchemaReadView<LatticeSchemaVersionConfig>.Failed(SchemaAdminFault.FailureMessage(ex));
        }
    }

    /// <inheritdoc />
    public Task<SchemaOperationResult> SetVersionConfigAsync(string treeId, LatticeSchemaVersionConfig config, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return SchemaOperation.RunAsync(
            async () =>
            {
                await _client.SetVersionConfigAsync(treeId, config, cancellationToken).ConfigureAwait(false);
                return SchemaOperationResult.Success(
                    $"Set the version config on tree '{treeId}' (schema {config.SchemaId}, target v{config.TargetVersion}).");
            });
    }

    /// <inheritdoc />
    public Task<SchemaOperationResult> AdvanceTargetVersionAsync(string treeId, uint newTargetVersion, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return SchemaOperation.RunAsync(
            async () =>
            {
                var config = await _client.AdvanceTargetVersionAsync(treeId, newTargetVersion, cancellationToken).ConfigureAwait(false);
                return SchemaOperationResult.Success($"Advanced tree '{treeId}' to target v{config.TargetVersion}.");
            });
    }

    /// <inheritdoc />
    public Task<SchemaOperationResult> AdvanceAndMigrateAsync(string treeId, uint newTargetVersion, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return SchemaOperation.RunAsync(
            async () =>
            {
                var report = await _client.AdvanceAndMigrateAsync(treeId, newTargetVersion, cancellationToken).ConfigureAwait(false);
                return SchemaOperationResult.Success($"Advanced tree '{treeId}' to v{newTargetVersion} and migrated: {DescribeReport(report)}");
            });
    }

    /// <inheritdoc />
    public Task<SchemaOperationResult> MigrateToTargetVersionAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return SchemaOperation.RunAsync(
            async () =>
            {
                var report = await _client.MigrateToTargetVersionAsync(treeId, cancellationToken).ConfigureAwait(false);
                return SchemaOperationResult.Success($"Migrated tree '{treeId}': {DescribeReport(report)}");
            });
    }

    /// <inheritdoc />
    public Task<SchemaOperationResult> ClearVersionConfigAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return SchemaOperation.RunAsync(
            async () =>
            {
                var removed = await _client.ClearVersionConfigAsync(treeId, cancellationToken).ConfigureAwait(false);
                return SchemaOperationResult.Success(removed
                    ? $"Cleared the version config on tree '{treeId}'."
                    : $"Tree '{treeId}' had no version config to clear.");
            });
    }

    /// <inheritdoc />
    public async Task<SchemaReadView<LatticeSchemaRemediationReport>> GetRemediationStatusAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        try
        {
            var report = await _client.GetRemediationStatusAsync(treeId, cancellationToken).ConfigureAwait(false);
            return SchemaReadView<LatticeSchemaRemediationReport>.Succeeded(report);
        }
        catch (LatticeAuthorizationDeniedException ex)
        {
            return SchemaReadView<LatticeSchemaRemediationReport>.Denied(SchemaAdminFault.DenialMessage(ex));
        }
        catch (RpcException ex)
        {
            return SchemaReadView<LatticeSchemaRemediationReport>.Failed(SchemaAdminFault.FailureMessage(ex));
        }
    }

    private static string DescribeReport(LatticeSchemaRemediationReport report)
    {
        if (report.Succeeded)
        {
            return $"completed, {report.ScannedCount} scanned.";
        }

        if (report.DidAbort)
        {
            return $"aborted on key '{report.OffendingKey}' ({report.Reason}).";
        }

        return $"{report.Phase}, {report.ScannedCount} scanned.";
    }
}
