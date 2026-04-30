using VehicleFleetSimulator.Abstractions;

namespace VehicleFleetSimulator.Benchmark.Sink;

/// <summary>
/// Configuration for the <see cref="LatticeSink"/>. Bound from the silo configuration under
/// the <c>LatticeSink</c> section.
/// </summary>
public sealed class LatticeSinkOptions
{
    /// <summary>Default tree id when none is configured.</summary>
    public const string DefaultTreeId = "vehicle-fleet";

    /// <summary>Default channel capacity. ~1 second of headroom at 100k publishes/s.</summary>
    public const int DefaultChannelCapacity = 100_000;

    /// <summary>Default drain batch size.</summary>
    public const int DefaultBatchSize = 256;

    /// <summary>Default flush interval — caps end-to-end producer→Lattice latency.</summary>
    public static readonly TimeSpan DefaultFlushInterval = TimeSpan.FromMilliseconds(50);

    /// <summary>Default TTL for the event-log key shape (B-10).</summary>
    public static readonly TimeSpan DefaultEventLogTtl = TimeSpan.FromHours(1);

    /// <summary>The Lattice tree to write into.</summary>
    public string TreeId { get; set; } = DefaultTreeId;

    /// <summary>Maximum number of telemetry samples queued in front of the drain loop.</summary>
    public int ChannelCapacity { get; set; } = DefaultChannelCapacity;

    /// <summary>Drain-batch size. The drain loop sets up to this many keys per flush.</summary>
    public int BatchSize { get; set; } = DefaultBatchSize;

    /// <summary>Flush interval. The drain loop flushes whenever the batch fills or this many
    /// milliseconds have passed since the previous flush.</summary>
    public TimeSpan FlushInterval { get; set; } = DefaultFlushInterval;

    /// <summary>Selects which key the sink computes for each telemetry sample.</summary>
    public KeyShape KeyShape { get; set; } = KeyShape.CurrentStateByVehicleId;

    /// <summary>When set and <see cref="KeyShape"/> is <see cref="KeyShape.EventLogTimestamped"/>,
    /// telemetry is written via the <c>SetAsync(key, value, ttl)</c> overload.</summary>
    public TimeSpan? EventLogTtl { get; set; } = DefaultEventLogTtl;

    /// <summary>When <see cref="KeyShape"/> is <see cref="KeyShape.RegionPrefixedVehicleId"/>, this
    /// is the comma-separated list of region ids. The sink stable-hashes the vehicle id into the
    /// list. The first entry is given disproportionate weight so a single shard goes hot.
    /// </summary>
    public string Regions { get; set; } = "eu-west,eu-east,us-west,us-east";

    /// <summary>Weight of the hot region as a fraction of the total fleet, 0..1. Default 0.7 →
    /// 70% of vehicles map to the first region in <see cref="Regions"/>.</summary>
    public double HotRegionShare { get; set; } = 0.7;

    /// <summary>When true, drop oldest queued samples when the channel is full instead of blocking
    /// the producer. Drops are surfaced via <c>vehicle_fleet_simulator.sink.dropped</c>.</summary>
    public bool DropOnFull { get; set; } = true;

    /// <summary>Optional shutdown drain timeout. Pending samples that don't drain within this
    /// window are recorded as <c>dropped_on_shutdown</c> and the drain task is allowed to exit.
    /// </summary>
    public TimeSpan ShutdownDrainTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Custom serializer for <see cref="VehicleTelemetryEvent"/>. Defaults to a small
    /// hand-rolled UTF-8 JSON encoder so the package doesn't depend on <c>System.Text.Json</c>.
    /// </summary>
    public Func<VehicleTelemetryEvent, byte[]>? Serializer { get; set; }
}
