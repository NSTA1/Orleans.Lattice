using Grpc.Core;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Schema;

/// <summary>
/// The default <see cref="ISchemaPolicyService"/> over an
/// <see cref="ISchemaAdminClient"/>. Every action catches a
/// <see cref="LatticeAuthorizationDeniedException"/> (the translated server denial)
/// and a residual <see cref="RpcException"/> and returns a non-success envelope, so
/// the Schema UI degrades cleanly and never leaks an exception.
/// </summary>
public sealed class SchemaPolicyService(ISchemaAdminClient client) : ISchemaPolicyService
{
    private readonly ISchemaAdminClient _client = client ?? throw new ArgumentNullException(nameof(client));

    /// <inheritdoc />
    public async Task<SchemaReadView<LatticeSchemaPolicy>> GetPolicyAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        try
        {
            var policy = await _client.GetPolicyAsync(treeId, cancellationToken).ConfigureAwait(false);
            return SchemaReadView<LatticeSchemaPolicy>.Succeeded(policy);
        }
        catch (LatticeAuthorizationDeniedException ex)
        {
            return SchemaReadView<LatticeSchemaPolicy>.Denied(SchemaAdminFault.DenialMessage(ex));
        }
        catch (RpcException ex)
        {
            return SchemaReadView<LatticeSchemaPolicy>.Failed(SchemaAdminFault.FailureMessage(ex));
        }
    }

    /// <inheritdoc />
    public Task<SchemaOperationResult> SetPolicyAsync(string treeId, LatticeSchemaPolicy policy, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(policy);
        return SchemaOperation.RunAsync(
            async () =>
            {
                await _client.SetPolicyAsync(treeId, policy, cancellationToken).ConfigureAwait(false);
                return SchemaOperationResult.Success($"Saved the schema policy on tree '{treeId}'.");
            });
    }

    /// <inheritdoc />
    public Task<SchemaOperationResult> ClearPolicyAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return SchemaOperation.RunAsync(
            async () =>
            {
                var removed = await _client.ClearPolicyAsync(treeId, cancellationToken).ConfigureAwait(false);
                return SchemaOperationResult.Success(removed
                    ? $"Cleared the schema policy on tree '{treeId}'."
                    : $"Tree '{treeId}' had no schema policy to clear.");
            });
    }
}
