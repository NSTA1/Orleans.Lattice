namespace Orleans.Lattice.Replication;

/// <summary>
/// The default <see cref="ILatticeTreeContentProbe"/>. Resolves the target
/// <see cref="ILattice"/> grain through the grain factory and reads its live
/// entry count under <see cref="LatticeAccessGateContext.EnterSystemOrigin"/>,
/// mirroring how <see cref="LatticeReplicationConfigStore"/> reads the config
/// tree: the probe is replication <b>infrastructure</b> supporting an operator
/// action that the API facade authorizes separately, so it runs under the
/// system origin rather than re-deriving a caller identity.
/// </summary>
internal sealed class GrainFactoryTreeContentProbe(IGrainFactory grainFactory)
    : ILatticeTreeContentProbe
{
    private readonly IGrainFactory _grainFactory =
        grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));

    /// <inheritdoc />
    public async Task<int> CountAsync(string treeId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        var tree = _grainFactory.GetGrain<ILattice>(treeId);

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            return await tree.CountAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
