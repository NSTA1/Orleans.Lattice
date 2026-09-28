namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// Configures the app bridge facade (<see cref="ILatticeAppBridge"/>) that <c>AddLatticeAppBridgeApi</c>
/// registers.
/// </summary>
/// <remarks>
/// The bridge rate-limits each <c>(caller, tenant, app)</c> partition with a fixed window: at most
/// <see cref="RateLimitPermitLimit"/> requests per <see cref="RateLimitWindow"/>. A request over the limit is
/// refused with <see cref="AppBridgeFailure.Unavailable"/> before any authorization or data access, so it may
/// succeed when retried in a later window. The defaults admit 100 requests per second per partition.
/// </remarks>
public sealed class LatticeAppBridgeOptions
{
    /// <summary>The default for <see cref="RateLimitPermitLimit"/>: 100 requests.</summary>
    public const int DefaultRateLimitPermitLimit = 100;

    /// <summary>The default for <see cref="RateLimitWindow"/>: one second.</summary>
    public static readonly TimeSpan DefaultRateLimitWindow = TimeSpan.FromSeconds(1);

    /// <summary>
    /// The number of requests each <c>(caller, tenant, app)</c> partition may make in one
    /// <see cref="RateLimitWindow"/>. Must be at least 1. Defaults to <see cref="DefaultRateLimitPermitLimit"/>.
    /// </summary>
    public int RateLimitPermitLimit { get; set; } = DefaultRateLimitPermitLimit;

    /// <summary>
    /// The length of one rate-limit window. Must be positive. Defaults to <see cref="DefaultRateLimitWindow"/>.
    /// </summary>
    public TimeSpan RateLimitWindow { get; set; } = DefaultRateLimitWindow;
}
