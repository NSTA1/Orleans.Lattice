namespace VehicleFleetSimulator.Benchmark.Sink;

/// <summary>
/// Configuration for the <see cref="LatticeAtomicSagaDriver"/>. Bound from the silo
/// configuration under the <c>AtomicSagaDriver</c> section. When <see cref="Enabled"/> is
/// <c>false</c>, the driver hosted-service exits its <c>ExecuteAsync</c> immediately.
/// </summary>
/// <remarks>
/// Drives <c>SetManyAtomicAsync</c> sagas at a configured rate so atomic-write scenarios
/// can measure saga throughput, fan-out latency, and (when paired with a replication
/// overlay) cross-cluster atomic visibility behaviour. Mirrors
/// <see cref="LatticeWriteDriverOptions"/> shape so operators can reason about the two
/// drivers symmetrically.
/// </remarks>
public sealed class LatticeAtomicSagaDriverOptions
{
    /// <summary>Master switch. When <c>false</c> the driver does nothing.</summary>
    public bool Enabled { get; set; }

    /// <summary>The Lattice tree to write into. Must match the replicated tree id so the
    /// sender-side replication observer captures every saga's per-key writes.</summary>
    public string TreeId { get; set; } = LatticeSinkOptions.DefaultTreeId;

    /// <summary>Target saga rate in sagas per second. Each saga issues a single
    /// <c>SetManyAtomicAsync</c> call carrying <see cref="BatchSize"/> entries. Pacing is
    /// shared across <see cref="Concurrency"/> persistent worker tasks. Set to 0 to
    /// disable the driver entirely.</summary>
    public int RatePerSecond { get; set; }

    /// <summary>Number of concurrent in-flight sagas. Each worker holds at most one
    /// outstanding <c>SetManyAtomicAsync</c> at a time - the saga concurrency degree
    /// equals the worker count.</summary>
    public int Concurrency { get; set; } = 16;

    /// <summary>How long to wait after silo startup before issuing the first saga. Gives
    /// the cluster time to come up and the replication shippers time to activate.</summary>
    public TimeSpan WarmupDelay { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>Number of entries per saga. Each entry is one <c>(key, value)</c> pair
    /// inside the atomic batch. Higher values stress the WAL-prepare path; lower values
    /// approach single-key write throughput.</summary>
    public int BatchSize { get; set; } = 16;

    /// <summary>Per-worker key-space size. Each saga draws <see cref="BatchSize"/> keys
    /// from the worker's slice (<c>{KeyPrefix}w{workerId}-{j}</c>), so concurrent sagas
    /// across workers never collide. Keep small so the WAL has repeated key updates
    /// (LWW conflict shape) rather than one entry per key.</summary>
    public int KeyspaceSize { get; set; } = 4_000;

    /// <summary>Prefix applied to every generated key. Pick a prefix distinct from the
    /// origin-side simulator keys so atomic-saga traffic is identifiable on the dashboard
    /// (e.g. <c>"atomic-"</c>).</summary>
    public string KeyPrefix { get; set; } = "atomic-";

    /// <summary>Size in bytes of the value written for every batch entry. Mirrors the
    /// order-of-magnitude vehicle telemetry payload (~256 B) by default.</summary>
    public int ValueSizeBytes { get; set; } = 256;
}
