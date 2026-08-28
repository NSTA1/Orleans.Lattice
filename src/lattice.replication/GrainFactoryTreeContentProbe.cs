namespace Orleans.Lattice.Replication;

/// <summary>
/// The default <see cref="ILatticeTreeContentProbe"/>. Resolves the target
/// <see cref="ILattice"/> grain through the grain factory and reads it under
/// <see cref="LatticeAccessGateContext.EnterSystemOrigin"/>, mirroring how
/// <see cref="LatticeReplicationConfigStore"/> reads the config tree: the probe
/// is replication <b>infrastructure</b> supporting an operator action that the
/// API facade authorizes separately, so it runs under the system origin rather
/// than re-deriving a caller identity.
/// <para>
/// Existence is answered by taking the first key from the tree's key stream and
/// abandoning the enumeration, not by counting. <c>ILattice.CountAsync</c> is a
/// strongly-consistent whole-tree fan-out that walks every leaf chain and
/// restarts whenever the shard map moves under it, so on a large or actively
/// splitting tree it can cost orders of magnitude more than the boolean the
/// caller reduces it to - and enabling replication is an operator action that
/// runs against live, concurrently-written trees. Taking one key short-circuits
/// at the first row.
/// </para>
/// </summary>
internal sealed class GrainFactoryTreeContentProbe(IGrainFactory grainFactory)
    : ILatticeTreeContentProbe
{
    private readonly IGrainFactory _grainFactory =
        grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));

    /// <inheritdoc />
    public async Task<bool> HasContentAsync(string treeId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        var tree = _grainFactory.GetGrain<ILattice>(treeId);

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await foreach (var _ in tree
                .KeysAsync(cancellationToken: cancellationToken)
                .ConfigureAwait(false))
            {
                // The first key settles it; disposing the enumerator here stops
                // the server-side scan rather than draining the whole tree.
                return true;
            }
        }

        return false;
    }
}
