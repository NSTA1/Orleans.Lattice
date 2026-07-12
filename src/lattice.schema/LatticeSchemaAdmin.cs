namespace Orleans.Lattice.Schema;

/// <summary>
/// The default <see cref="ILatticeSchemaAdmin"/>. It delegates policy mutations to
/// the durable <see cref="ILatticeSchemaPolicyStore"/> and dead-letter reads to the
/// <see cref="ILatticeSchemaDeadLetterStore"/>, and it eagerly evicts the local
/// <see cref="ILatticeSchemaPolicyProvider"/> cache on a policy change so the new
/// policy takes effect on this silo's next write without waiting for the mutation
/// observer to propagate the eviction.
/// </summary>
internal sealed class LatticeSchemaAdmin(
    ILatticeSchemaPolicyStore policyStore,
    ILatticeSchemaDeadLetterStore deadLetterStore,
    ILatticeSchemaPolicyProvider provider) : ILatticeSchemaAdmin
{
    /// <inheritdoc />
    public async Task SetPolicyAsync(string treeId, LatticeSchemaPolicy policy, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(policy);

        await policyStore.SetPolicyAsync(treeId, policy, cancellationToken).ConfigureAwait(false);
        provider.Invalidate(treeId);
    }

    /// <inheritdoc />
    public async Task<bool> ClearPolicyAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        var removed = await policyStore.ClearPolicyAsync(treeId, cancellationToken).ConfigureAwait(false);
        provider.Invalidate(treeId);
        return removed;
    }

    /// <inheritdoc />
    public Task<LatticeSchemaPolicy?> GetPolicyAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return policyStore.GetPolicyAsync(treeId, cancellationToken);
    }

    /// <inheritdoc />
    public IAsyncEnumerable<LatticeSchemaDeadLetterEntry> ListDeadLettersAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return deadLetterStore.ListAsync(treeId, cancellationToken);
    }

    /// <inheritdoc />
    public Task<int> CountDeadLettersAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return deadLetterStore.CountAsync(treeId, cancellationToken);
    }
}
