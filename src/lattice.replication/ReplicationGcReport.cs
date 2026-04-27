using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Result of a single <see cref="ILatticeReplicationGc.RunOnceAsync"/>
/// pass. Diagnostic only; the GC run is the durable side-effect.
/// </summary>
/// <param name="TreeName">Tree the run targeted.</param>
/// <param name="MinCursor">Minimum reported consumer cursor at the time of the run, or <see langword="null"/> when no consumer has reported.</param>
/// <param name="TtlCeilingHlc">The wall-clock TTL ceiling expressed as an <see cref="HybridLogicalClock"/> (entries with <c>Timestamp &lt;= ceiling</c> are eligible for trim regardless of cursor), or <see langword="null"/> when <see cref="LatticeReplicationOptions.WalRetention"/> is unset.</param>
/// <param name="ShardsScanned">Number of WAL partitions visited during the run.</param>
/// <param name="EntriesTrimmed">Total number of entries removed from the WAL across every partition, summing the per-shard counts. Zero when the predicate yielded no trim point or the WAL was already empty up to that point.</param>
public readonly record struct ReplicationGcReport(
    string TreeName,
    HybridLogicalClock? MinCursor,
    HybridLogicalClock? TtlCeilingHlc,
    int ShardsScanned,
    long EntriesTrimmed);
