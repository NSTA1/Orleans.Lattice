namespace MultiSiteManufacturing.Host.Federation;

/// <summary>
/// Chaos knobs for a single <see cref="IFactBackend"/>. All defaults are
/// "nominal" — no jitter, no failures, no duplicates — so a decorated
/// backend behaves identically to the undecorated one until the operator
/// turns something on via the chaos fly-out.
/// </summary>
/// <remarks>
/// Plan §4.3 Tier 2: applying faults to <b>one</b> backend only is the
/// canonical way to surface baseline-vs-lattice divergence without a
/// scripted saga. Faults are applied on write paths only; read paths
/// pass through unchanged.
/// </remarks>
[GenerateSerializer, Immutable]
public readonly record struct BackendChaosConfig
{
    /// <summary>Nominal configuration: no chaos applied.</summary>
    public static BackendChaosConfig Nominal => default;

    /// <summary>Minimum per-call artificial latency, in milliseconds (inclusive).</summary>
    [Id(0)] public int JitterMsMin { get; init; }

    /// <summary>Maximum per-call artificial latency, in milliseconds (inclusive).</summary>
    [Id(1)] public int JitterMsMax { get; init; }

    /// <summary>Probability in [0, 1] that an emit throws a transient failure.</summary>
    [Id(2)] public double TransientFailureRate { get; init; }

    /// <summary>Probability in [0, 1] that an emit is applied twice (tests downstream dedup).</summary>
    [Id(3)] public double WriteAmplificationRate { get; init; }
}

/// <summary>
/// Observable snapshot of one backend's chaos configuration, published to
/// the gRPC <c>ListBackends</c> / <c>ConfigureBackend</c> feed and the
/// Blazor chaos fly-out (M10).
/// </summary>
[GenerateSerializer, Immutable]
public readonly record struct BackendChaosState
{
    /// <summary>Stable backend name (matches <see cref="IFactBackend.Name"/>).</summary>
    [Id(0)] public required string Name { get; init; }

    /// <summary>Current chaos configuration.</summary>
    [Id(1)] public BackendChaosConfig Config { get; init; }
}
