namespace VehicleFleetSimulator.Benchmark.Sink;

/// <summary>
/// Configuration for the <see cref="LatticeWriteDriver"/>. Bound from the silo configuration
/// under the <c>WriteDriver</c> section. When <see cref="Enabled"/> is <c>false</c>, the
/// driver hosted-service exits its <c>ExecuteAsync</c> immediately.
/// </summary>
/// <remarks>
/// Used by bidirectional-replication scenarios to run an in-silo write producer on the
/// replica side, so both clusters generate WAL traffic and the reverse-direction ship/apply
/// metrics are non-empty. Without this driver, the simulator API only points at the origin
/// cluster and the replica produces no outbound writes - making the "bidirectional"
/// scenario unidirectional in practice.
/// </remarks>
public sealed class LatticeWriteDriverOptions
{
    /// <summary>Master switch. When <c>false</c> the driver does nothing.</summary>
    public bool Enabled { get; set; }

    /// <summary>The Lattice tree to write into. Must match the replicated tree id so the
    /// sender-side replication observer captures every <c>SetAsync</c> call.</summary>
    public string TreeId { get; set; } = LatticeSinkOptions.DefaultTreeId;

    /// <summary>Target write rate in writes per second. The driver paces issuance using N
    /// persistent worker tasks (where N = <see cref="Concurrency"/>), each on its own
    /// <c>Stopwatch</c>-based deadline of <c>RatePerSecond / Concurrency</c> writes/s,
    /// phase-staggered for even temporal distribution. Set to 0 to disable.</summary>
    public int RatePerSecond { get; set; }

    /// <summary>Concurrency degree - number of in-flight <c>SetAsync</c> calls. Higher
    /// values expose tail latency that single-flight execution would hide.</summary>
    public int Concurrency { get; set; } = 16;

    /// <summary>How long to wait after silo startup before issuing the first write. Gives
    /// the cluster time to come up and the replication driver activation service time to
    /// activate the per-(tree, peer) shippers.</summary>
    public TimeSpan WarmupDelay { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>Number of distinct keys the driver writes into. Each tick writes
    /// <c>"{KeyPrefix}{rng.Next(KeyspaceSize)}"</c>. Keep small so the sender-side WAL has
    /// repeated key updates (LWW conflict shape) rather than one entry per key.</summary>
    public int KeyspaceSize { get; set; } = 2_000;

    /// <summary>Prefix applied to every generated key. Pick a prefix distinct from the
    /// origin-side simulator keys so cross-cluster traffic is identifiable on the dashboard
    /// (e.g. <c>"replica-"</c>).</summary>
    public string KeyPrefix { get; set; } = "replica-";

    /// <summary>Size in bytes of the value written each tick. Mirrors the order-of-magnitude
    /// vehicle telemetry payload (~256 B) by default.</summary>
    public int ValueSizeBytes { get; set; } = 256;
}