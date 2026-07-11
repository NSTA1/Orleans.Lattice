using System.Runtime.CompilerServices;

namespace Orleans.Lattice.Schema;

/// <summary>
/// The default <see cref="ILatticeSchemaPolicyStore"/>. Dogfoods the reserved
/// <c>sys-schema-policy</c> <c>ILattice</c> tree: each tree's policy is stored as a
/// JSON value under the governed tree id, so a policy read is a single point read
/// and <see cref="ListPoliciesAsync"/> is a full-tree scan. Every mutation runs
/// through the standard write path.
/// </summary>
/// <remarks>
/// The store is enforcement <b>infrastructure</b>: it reads and writes the policy
/// tree that feeds the enforcement interceptor, so every operation runs under
/// <see cref="LatticeAccessGateContext.EnterSystemOrigin"/>. This both avoids a
/// bootstrap paradox (the first policy write cannot itself be schema-validated)
/// and keeps the interceptor from re-entering itself when it caches a policy.
/// Authorizing <i>who</i> may edit policy is a higher-layer concern
/// (<see cref="ILatticeSchemaAdmin"/> / the <see cref="LatticeOperation.SchemaAdmin"/>
/// capability), not the store's.
/// </remarks>
internal sealed class LatticeSchemaPolicyStore(IGrainFactory grainFactory) : ILatticeSchemaPolicyStore
{
    private ILattice Policy => grainFactory.GetGrain<ILattice>(SchemaConstants.PolicyTree);

    /// <inheritdoc />
    public async Task SetPolicyAsync(string treeId, LatticeSchemaPolicy policy, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(policy);
        SchemaConstants.ThrowIfReservedTree(treeId, nameof(treeId));

        // Validate up front: compiling the policy rejects an uncompilable /
        // non-linear regex at policy-set time rather than on a later write.
        _ = CompiledSchemaPolicy.Compile(policy);

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await Policy.SetAsync(treeId, policy, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<LatticeSchemaPolicy?> GetPolicyAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            return await Policy.GetAsync<LatticeSchemaPolicy>(treeId, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<bool> ClearPolicyAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            return await Policy.DeleteAsync(treeId, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<KeyValuePair<string, LatticeSchemaPolicy>> ListPoliciesAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await foreach (var entry in Policy
                .ScanEntriesAsync<LatticeSchemaPolicy>(cancellationToken: cancellationToken)
                .ConfigureAwait(false))
            {
                if (entry.Value is { } policy)
                {
                    yield return new KeyValuePair<string, LatticeSchemaPolicy>(entry.Key, policy);
                }
            }
        }
    }
}
