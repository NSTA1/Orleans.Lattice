namespace VehicleFleetSimulator.Benchmark.Sink;

/// <summary>
/// Configuration for the <see cref="LatticeReadDriver"/>. Bound from the silo configuration
/// under the <c>ReadDriver</c> section. When <see cref="Enabled"/> is <c>false</c>, the driver
/// hosted-service exits its <c>ExecuteAsync</c> immediately.
/// </summary>
public sealed class LatticeReadDriverOptions
{
    /// <summary>Master switch. When <c>false</c> the driver does nothing.</summary>
    public bool Enabled { get; set; }

    /// <summary>The Lattice tree to read from. Should match the sink''s tree id so reads target
    /// keys the simulator is producing.</summary>
    public string TreeId { get; set; } = LatticeSinkOptions.DefaultTreeId;

    /// <summary>Target read rate in reads per second. The driver paces issuance using N
    /// persistent worker tasks (where N = <see cref="Concurrency"/>), each on its own
    /// <c>Stopwatch</c>-based deadline of <c>RatePerSecond / Concurrency</c> reads/s,
    /// phase-staggered for even temporal distribution. Set to 0 to disable issuing reads.</summary>
    public int RatePerSecond { get; set; }

    /// <summary>How the driver picks the next key from the discovered keyspace.</summary>
    public ReadDriverPattern Pattern { get; set; } = ReadDriverPattern.Random;

    /// <summary>Concurrency degree - number of in-flight <c>GetAsync</c> calls. Higher values
    /// expose tail latency that single-flight execution would hide.</summary>
    public int Concurrency { get; set; } = 16;

    /// <summary>How long to wait after silo startup before issuing the first read. Gives the
    /// simulator time to populate the keyspace so the cursor scan finds something.</summary>
    public TimeSpan WarmupDelay { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>How often the driver re-scans the keyspace via cursor pages so newly-published
    /// vehicle ids get picked up. Set very high to read against a frozen sample.</summary>
    public TimeSpan KeyspaceRefreshInterval { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>Maximum keys cached per refresh. Protects the driver from running away on a
    /// huge keyspace; the cursor-scan stops at this count and the next refresh rotates the
    /// sample window.</summary>
    public int KeyspaceSampleSize { get; set; } = 4096;
}

/// <summary>How <see cref="LatticeReadDriver"/> picks the next key to read.</summary>
public enum ReadDriverPattern
{
    /// <summary>Pick a uniformly-random index from the discovered keyspace each tick.</summary>
    Random = 0,

    /// <summary>Walk the discovered keyspace in sorted order, wrapping at the end.</summary>
    Sequential = 1,
}