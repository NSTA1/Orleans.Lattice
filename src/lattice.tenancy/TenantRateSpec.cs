namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The cluster-wide request-rate configuration for one tenant, as compiled from
/// its policy: the sustained operations-per-second ceiling and the burst
/// allowance. This is the whole-cluster rate; the budget coordinator apportions a
/// per-silo share of it before sizing the silo-local token bucket. In-process
/// value only - it never crosses a grain or wire boundary and carries no Orleans
/// serialization attributes.
/// </summary>
/// <param name="Tenant">The tenant this rate applies to.</param>
/// <param name="OpsPerSecond">The cluster-wide sustained operations-per-second ceiling (positive).</param>
/// <param name="BurstPercent">The burst allowance as a percentage of the rate (<c>0</c> for none).</param>
internal readonly record struct TenantRateSpec(TenantId Tenant, long OpsPerSecond, int BurstPercent);
