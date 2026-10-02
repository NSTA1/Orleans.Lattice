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
    /// <remarks>
    /// Accepts the remediation on the tree's coordinator, then drives it one bounded
    /// slice at a time, so no single grain call runs for the whole remediation and
    /// none is cut off by the response timeout. The call still returns only once the
    /// remediation is terminal.
    /// </remarks>
    public async Task<LatticeSchemaRemediationReport> RemediateAsync(
        string treeId,
        LatticeValueTransform transform,
        LatticeSchemaPolicy targetPolicy,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(targetPolicy);
        SchemaConstants.ThrowIfReservedTree(treeId, nameof(treeId));

        var grain = grainFactory.GetGrain<ILatticeSchemaRemediationGrain>(treeId);
        var accepted = await grain.AcceptAsync(transform, targetPolicy, Guid.NewGuid().ToString("N")).ConfigureAwait(false);
        return await SchemaRemediationDriver.DriveAsync(grain, accepted, progress: null, CancellationToken.None)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task<LatticeSchemaRemediationReport> GetRemediationStatusAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return grainFactory.GetGrain<ILatticeSchemaRemediationGrain>(treeId).GetStatusAsync();
    }
}
