namespace Orleans.Lattice.Operations;

/// <summary>
/// Answers whether the silo running an operation is known to be dead, so a read
/// can fail a lost operation at once instead of waiting out its heartbeat lease.
/// A seam so the grain is unit-testable without a cluster membership snapshot.
/// </summary>
internal interface ILatticeOperationSiloLiveness
{
    /// <summary>Returns <see langword="true"/> when <paramref name="silo"/> is declared dead by cluster membership.</summary>
    /// <param name="silo">The silo address.</param>
    /// <returns>Whether the silo is dead.</returns>
    bool IsDead(SiloAddress silo);
}
