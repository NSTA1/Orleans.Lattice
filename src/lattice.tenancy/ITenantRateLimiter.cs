namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The per-silo, in-process request-rate limiter: the data-plane entry path
/// consults it to decide whether one tenant-scoped operation may proceed under
/// the tenant's configured operations-per-second ceiling. It is a registered
/// per-silo singleton service, <b>not</b> an Orleans grain, so
/// <see cref="TryAcquire"/> takes zero grain hops and no lock - each silo
/// enforces only the share of the tenant's cluster-wide rate currently
/// apportioned to it by the low-frequency budget coordinator, and the per-op
/// decision is a lock-free, allocation-free token decrement over silo-local
/// state.
/// </summary>
/// <remarks>
/// <para>
/// A tenant with no configured rate (no <see cref="TenantQuotas.MaxOpsPerSecond"/>
/// in its policy) is <b>inert</b>: <see cref="TryAcquire"/> always admits it, so a
/// deployment that configures no per-tenant rate is byte-for-byte unthrottled and
/// pays only a single dictionary probe on the hot path.
/// </para>
/// <para>
/// This limiter is a decision surface only. Registering it wires no enforcement:
/// nothing on the data path consults it until a later feature threads it into the
/// write path. It answers "may this operation proceed?"; it does not itself fence
/// traffic.
/// </para>
/// </remarks>
public interface ITenantRateLimiter
{
    /// <summary>
    /// Attempts to admit a single tenant-scoped operation against
    /// <paramref name="tenant"/>'s silo-local rate budget. A lock-free,
    /// allocation-free, grain-hop-free token decrement.
    /// </summary>
    /// <param name="tenant">
    /// The tenant the operation is charged to. The uninitialised
    /// <c>default(TenantId)</c> ("no tenant") and any tenant with no configured
    /// rate are inert and always admitted.
    /// </param>
    /// <returns>
    /// <c>true</c> when the operation is admitted (a token was available);
    /// <c>false</c> when the tenant's silo-local budget is momentarily exhausted
    /// and the operation should be throttled.
    /// </returns>
    bool TryAcquire(TenantId tenant);
}
