namespace VehicleFleetSimulator.Benchmark.Sink;

/// <summary>
/// Selects how the Lattice sink derives the key for a per-vehicle telemetry sample. Maps directly
/// to the benchmark scenarios defined in <c>benchmark/benchmark-scenarios.md</c>.
/// </summary>
public enum KeyShape
{
    /// <summary>
    /// <c>vehicleId.ToString("N")</c>. Each vehicle owns one current-state row that is overwritten
    /// every tick. Default key shape and target of current-state-no-replication, current-state-single-peer, replication-backpressure, receiver-crash, bidirectional-replication, replication-key-filter, observer-no-peer.
    /// </summary>
    CurrentStateByVehicleId = 0,

    /// <summary>
    /// <c>region/vehicleId</c> with a deliberately oversubscribed region prefix so a single shard
    /// goes hot. Drives skewed-key-shard-splits (skewed-key adaptive shard splits).
    /// </summary>
    RegionPrefixedVehicleId = 1,

    /// <summary>
    /// <c>vehicleId/{Timestamp:O}</c> with a TTL applied via the Lattice <c>SetAsync(ttl)</c>
    /// overload. Each tick produces a new key; TTL drives steady-state tombstone compaction.
    /// Drives event-log-with-ttl (event-log tree with TTL).
    /// </summary>
    EventLogTimestamped = 2,
}
