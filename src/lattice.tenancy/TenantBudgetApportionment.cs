namespace Orleans.Lattice.Tenancy;

/// <summary>
/// Pure apportionment arithmetic: divides a tenant's cluster-wide request rate
/// into the share one silo may enforce, under either static-even or
/// demand-proportional leasing. Every method is a total function of its arguments
/// with no shared state, so it is trivially testable and allocation-free; the
/// budget coordinator calls it at lease cadence only.
/// </summary>
internal static class TenantBudgetApportionment
{
    /// <summary>
    /// The static-even per-silo share: <c>clusterRate / max(1, liveSiloCount)</c>,
    /// floored. The sum across all silos never exceeds the cluster rate (integer
    /// division floors), so the apportionment is always cluster-bounded.
    /// </summary>
    /// <param name="clusterRate">The tenant's cluster-wide operations-per-second ceiling.</param>
    /// <param name="liveSiloCount">The number of live silos (treated as at least 1).</param>
    /// <returns>The per-silo operations-per-second share.</returns>
    internal static long StaticEvenShare(long clusterRate, int liveSiloCount)
    {
        if (clusterRate < 0)
        {
            clusterRate = 0;
        }

        var n = liveSiloCount < 1 ? 1 : liveSiloCount;
        return clusterRate / n;
    }

    /// <summary>
    /// The demand-proportional per-silo share. A fixed reserve fraction of the
    /// cluster rate is split evenly across silos to guarantee an idle silo a
    /// non-zero floor (avoiding the chicken-and-egg starvation where a silo with
    /// zero recent demand could never lease enough to build demand); the remaining
    /// pool is divided in proportion to this silo's share of the cluster-wide
    /// demand. The blended share is capped at the cluster rate, and when total
    /// demand is zero the result collapses to <see cref="StaticEvenShare"/> (the
    /// bootstrap case).
    /// </summary>
    /// <param name="clusterRate">The tenant's cluster-wide operations-per-second ceiling.</param>
    /// <param name="liveSiloCount">The number of live silos (treated as at least 1).</param>
    /// <param name="thisSiloDemand">This silo's recent admitted-operation count for the tenant.</param>
    /// <param name="totalClusterDemand">The cluster-wide recent admitted-operation count for the tenant.</param>
    /// <param name="reserveFraction">The fraction of the cluster rate reserved for the even floor, in <c>[0, 1]</c>.</param>
    /// <returns>The per-silo operations-per-second share, never exceeding the cluster rate.</returns>
    internal static long DemandProportionalShare(
        long clusterRate,
        int liveSiloCount,
        long thisSiloDemand,
        long totalClusterDemand,
        double reserveFraction)
    {
        if (clusterRate < 0)
        {
            clusterRate = 0;
        }

        var n = liveSiloCount < 1 ? 1 : liveSiloCount;

        if (totalClusterDemand <= 0 || thisSiloDemand < 0)
        {
            // No demand signal yet: bootstrap on static-even.
            return clusterRate / n;
        }

        if (reserveFraction < 0)
        {
            reserveFraction = 0;
        }
        else if (reserveFraction > 1)
        {
            reserveFraction = 1;
        }

        var reserved = (long)(clusterRate * reserveFraction);
        if (reserved < 0)
        {
            reserved = 0;
        }
        else if (reserved > clusterRate)
        {
            reserved = clusterRate;
        }

        var evenPart = reserved / n;
        var demandPool = clusterRate - reserved;

        long demandPart;
        if (thisSiloDemand >= totalClusterDemand)
        {
            // This silo is the sole source of demand: it may claim the whole pool.
            demandPart = demandPool;
        }
        else
        {
            // Exact proportional slice via 128-bit intermediate to avoid overflow.
            demandPart = (long)((UInt128)demandPool * (ulong)thisSiloDemand / (ulong)totalClusterDemand);
        }

        var share = evenPart + demandPart;
        return share > clusterRate ? clusterRate : share;
    }
}
