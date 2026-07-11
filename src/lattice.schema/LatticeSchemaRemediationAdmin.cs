namespace Orleans.Lattice.Schema;

/// <summary>
/// The default <see cref="ILatticeSchemaRemediationAdmin"/>. It validates the
/// governed tree id and forwards to the per-tree durable
/// <see cref="ILatticeSchemaRemediationGrain"/> coordinator, which owns the dry-run
/// gate, the destination build, cutover, and durable state.
/// </summary>
internal sealed class LatticeSchemaRemediationAdmin(IGrainFactory grainFactory) : ILatticeSchemaRemediationAdmin
{
    /// <inheritdoc />
    public Task<LatticeSchemaRemediationReport> RemediateAsync(
        string treeId,
        LatticeValueTransform transform,
        LatticeSchemaPolicy targetPolicy,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(targetPolicy);
        SchemaConstants.ThrowIfReservedTree(treeId, nameof(treeId));

        return grainFactory.GetGrain<ILatticeSchemaRemediationGrain>(treeId)
            .StartAsync(transform, targetPolicy, cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeSchemaRemediationReport> GetRemediationStatusAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return grainFactory.GetGrain<ILatticeSchemaRemediationGrain>(treeId).GetStatusAsync();
    }
}
