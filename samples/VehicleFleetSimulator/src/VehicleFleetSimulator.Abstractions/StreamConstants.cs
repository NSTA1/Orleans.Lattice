namespace VehicleFleetSimulator.Abstractions;

/// <summary>Stream provider name and namespaces shared by silo and clients.</summary>
public static class StreamConstants
{
    /// <summary>Orleans stream-provider name; must match silo + client registration.</summary>
    public const string ProviderName = "VehicleStreams";

    /// <summary>Per-vehicle telemetry stream namespace, keyed by <c>VehicleId</c>.</summary>
    public const string TelemetryNamespace = "vehicle-telemetry";

    /// <summary>Shared fan-in telemetry stream namespace; vehicle grains publish to one of
    /// <see cref="TelemetryAllShardCount"/> shards keyed by <c>VehicleId</c> so the dispatch
    /// path scales beyond a single grain's turn-based throughput cap.</summary>
    public const string TelemetryAllNamespace = "vehicle-telemetry-all";

    /// <summary>Single shared discrete-event stream namespace, keyed by <see cref="EventsStreamKey"/>.</summary>
    public const string EventsNamespace = "vehicle-events";

    /// <summary>Stable key for the shared events stream.</summary>
    public static readonly Guid EventsStreamKey = Guid.Empty;

    /// <summary>Number of shards the telemetry-all stream is split into. Each shard is consumed by
    /// its own <c>FleetFanOutGrain</c> instance.</summary>
    public const int TelemetryAllShardCount = 8;

    /// <summary>Stable grain key for the singleton events-feed <c>FleetFanOutGrain</c>. Lives on its
    /// own activation, distinct from the <see cref="TelemetryAllShardCount"/> telemetry shards, so
    /// the high-volume <c>PublishEvent</c> path never shares a message queue with the telemetry path
    /// (and so a backed-up events queue can't block control-plane Subscribe/Unsubscribe on a shard
    /// that's also carrying telemetry).</summary>
    public static readonly Guid EventsGrainKey = new(0xeeee_0000, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

    /// <summary>Returns the deterministic stream key (and grain key) for telemetry-all shard
    /// <paramref name="shard"/>. Encoding the shard index into the high-order bytes guarantees a
    /// stable mapping that's stable across silo restarts.</summary>
    public static Guid GetTelemetryAllShardKey(int shard)
    {
        if ((uint)shard >= TelemetryAllShardCount)
            throw new ArgumentOutOfRangeException(nameof(shard));
        return new Guid(0xfa00_0000 | (uint)shard, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    /// <summary>Maps a vehicle id to its telemetry-all shard index.</summary>
    public static int ShardForVehicle(Guid vehicleId)
    {
        // High 32 bits of the Guid give a well-distributed bucket selector.
        Span<byte> bytes = stackalloc byte[16];
        vehicleId.TryWriteBytes(bytes);
        var hash = BitConverter.ToUInt32(bytes[..4]);
        return (int)(hash % TelemetryAllShardCount);
    }
}

