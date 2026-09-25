namespace VehicleFleetSimulator.AzureThroughput.Engine;

internal sealed record IngestSettings(string TreeId, int TcpPort, int BatchSize, TimeSpan FlushInterval, TimeSpan ReportInterval, int FlushConcurrency, int ShardCountOverride, BenchWorkloadMode WorkloadMode, int AtomicBatchSize, int PreseedKeyCount, int WalMaxPendingBatches, int ResponseTimeoutSec, int WalPartitions, int WalAccounts, string IngestMode)
{
    /// <summary>
    /// Requested per-slot fan-out for the point modes (BENCH_POINT_FANOUT);
    /// zero or negative means unset.
    /// </summary>
    public int PointFanOut { get; init; }

    /// <summary>
    /// Concurrent calls each flush slot fans out into for the point modes.
    /// Falls back to <see cref="FlushConcurrency"/> when
    /// <see cref="PointFanOut"/> is unset, which is the single-VM rig
    /// behaviour: in-flight = FlushConcurrency squared. The ACA cohort script
    /// sets the per-silo bound so in-flight grows linearly with silo count.
    /// </summary>
    public int EffectivePointFanOut => ResolvePointFanOut(PointFanOut, FlushConcurrency);

    /// <summary>
    /// Resolves the point-mode per-slot fan-out: <paramref name="pointFanOut"/>
    /// when positive, otherwise <paramref name="flushConcurrency"/>.
    /// </summary>
    public static int ResolvePointFanOut(int pointFanOut, int flushConcurrency) =>
        pointFanOut > 0 ? pointFanOut : flushConcurrency;
}
