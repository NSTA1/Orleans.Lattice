using Grpc.Core;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Schema;

/// <summary>
/// The default <see cref="ISchemaComplianceService"/> over an
/// <see cref="ISchemaAdminClient"/>. Both reads catch a
/// <see cref="LatticeAuthorizationDeniedException"/> (the translated server denial)
/// and a residual <see cref="RpcException"/> and return a non-success envelope, so
/// the Schema UI degrades cleanly and never leaks an exception.
/// </summary>
public sealed class SchemaComplianceService(ISchemaAdminClient client) : ISchemaComplianceService
{
    private readonly ISchemaAdminClient _client = client ?? throw new ArgumentNullException(nameof(client));

    /// <inheritdoc />
    public async Task<SchemaReadView<LatticeSchemaComplianceReport>> ScanComplianceAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        try
        {
            var report = await _client.ScanComplianceAsync(treeId, cancellationToken).ConfigureAwait(false);
            return SchemaReadView<LatticeSchemaComplianceReport>.Succeeded(report);
        }
        catch (LatticeAuthorizationDeniedException ex)
        {
            return SchemaReadView<LatticeSchemaComplianceReport>.Denied(SchemaAdminFault.DenialMessage(ex));
        }
        catch (RpcException ex)
        {
            return SchemaReadView<LatticeSchemaComplianceReport>.Failed(SchemaAdminFault.FailureMessage(ex));
        }
    }

    /// <inheritdoc />
    public async Task<SchemaDeadLetterView> ListDeadLettersAsync(string treeId, int maxEntries, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxEntries);
        try
        {
            var count = await _client.CountDeadLettersAsync(treeId, cancellationToken).ConfigureAwait(false);
            var entries = await _client.ListDeadLettersAsync(treeId, maxEntries, cancellationToken).ConfigureAwait(false);
            return new SchemaDeadLetterView
            {
                Status = SchemaOperationStatus.Succeeded,
                Count = count,
                Entries = entries,
            };
        }
        catch (LatticeAuthorizationDeniedException ex)
        {
            return new SchemaDeadLetterView { Status = SchemaOperationStatus.Denied, Message = SchemaAdminFault.DenialMessage(ex) };
        }
        catch (RpcException ex)
        {
            return new SchemaDeadLetterView { Status = SchemaOperationStatus.Failed, Message = SchemaAdminFault.FailureMessage(ex) };
        }
    }
}
