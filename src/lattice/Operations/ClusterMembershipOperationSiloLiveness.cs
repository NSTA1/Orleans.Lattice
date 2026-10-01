namespace Orleans.Lattice.Operations;

/// <summary>
/// The default <see cref="ILatticeOperationSiloLiveness"/>: reads the current
/// cluster membership snapshot.
/// </summary>
/// <param name="membership">The cluster membership service.</param>
internal sealed class ClusterMembershipOperationSiloLiveness(IClusterMembershipService membership)
    : ILatticeOperationSiloLiveness
{
    /// <inheritdoc />
    public bool IsDead(SiloAddress silo) =>
        membership.CurrentSnapshot.GetSiloStatus(silo) == SiloStatus.Dead;
}
