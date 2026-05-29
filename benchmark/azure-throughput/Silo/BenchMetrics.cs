// Benchmark-local meter for the Azure throughput harness.
//
// These instruments live OUTSIDE Orleans.Lattice's public surface
// (the meter is `azure.throughput.bench`, not `orleans.lattice`) so
// they can never accidentally leak into a consumer's telemetry. They
// exist only to disambiguate where on the silo side the receive path
// is queuing during a benchmark rung:
//
//   tcp.read.line_bytes           - bytes per JSON line accepted off the TCP socket.
//                                   Sanity check on the wire shape; combined with
//                                   tcp.read.lines_per_drain it answers "is the
//                                   silo presenting too-small reads to the
//                                   JSON deserialiser?".
//
//   tcp.read.channel_write_wait_ms - wall-clock spent inside ChannelWriter.WriteAsync
//                                   per accepted line. Near-zero means the drain
//                                   side is keeping up and the TCP-read loop is
//                                   the bottleneck; high means the drain side
//                                   is blocked on the lattice and the channel
//                                   is full, propagating backpressure to the
//                                   TCP reader (and from there to the producer).
//
//   drain.flush_dispatch_size      - size of the batch list at the moment
//                                   DispatchFlushAsync is called. Tells us
//                                   whether the silo is presenting tiny batches
//                                   to the lattice (would explain the
//                                   `provider.phase2.batch_size=1.00` shape
//                                   observed in U9l).
//
//   drain.flush_dispatch_wait_ms   - wall-clock spent waiting on the flush gate
//                                   semaphore before the batch was dispatched.
//                                   Near-zero means the flush slots are not
//                                   contended; high means all FlushConcurrency
//                                   slots are saturated and the drain loop is
//                                   stalled on the lattice's commit speed.
//
// The PhaseADiagnosticReporter subscribes to this meter in addition to
// the orleans.lattice meter and renders both into the same `[phaseA]
// instrument=...` line shape so the ladder script needs no parser
// change.

using System.Diagnostics.Metrics;

namespace VehicleFleetSimulator.AzureThroughput.Silo;

internal static class BenchMetrics
{
    /// <summary>
    /// Benchmark-local meter. Distinct from <c>orleans.lattice</c> so
    /// these instruments are unambiguously bench-side measurements and
    /// never leak through the public lattice surface.
    /// </summary>
    public static readonly Meter Meter = new("azure.throughput.bench");

    /// <summary>
    /// Bytes per JSON line accepted off the TCP socket by
    /// <c>HandleConnectionAsync</c>.
    /// </summary>
    public static readonly Histogram<int> TcpReadLineBytes =
        Meter.CreateHistogram<int>(
            name: "azure.throughput.bench.tcp.read.line_bytes",
            unit: "By",
            description: "Bytes per JSON line received on the silo's TCP ingest socket.");

    /// <summary>
    /// Wall-clock ms spent inside <c>ChannelWriter.WriteAsync</c> when
    /// the TCP read loop hands an accepted line to the drain channel.
    /// High values indicate the channel is full and the drain side
    /// (lattice flushes) is the queue point.
    /// </summary>
    public static readonly Histogram<double> TcpReadChannelWriteWaitMs =
        Meter.CreateHistogram<double>(
            name: "azure.throughput.bench.tcp.read.channel_write_wait_ms",
            unit: "ms",
            description: "Wall-clock ms spent in ChannelWriter.WriteAsync per accepted TCP line.");

    /// <summary>
    /// Size of the batch list when <c>DispatchFlushAsync</c> is called.
    /// </summary>
    public static readonly Histogram<int> DrainFlushDispatchSize =
        Meter.CreateHistogram<int>(
            name: "azure.throughput.bench.drain.flush_dispatch_size",
            unit: "entries",
            description: "Entries per batch handed to SetManyAsync by the silo drain loop.");

    /// <summary>
    /// Wall-clock ms the drain loop spent waiting on the flush gate
    /// semaphore before dispatching a batch. High values indicate all
    /// FlushConcurrency slots are saturated.
    /// </summary>
    public static readonly Histogram<double> DrainFlushDispatchWaitMs =
        Meter.CreateHistogram<double>(
            name: "azure.throughput.bench.drain.flush_dispatch_wait_ms",
            unit: "ms",
            description: "Wall-clock ms spent waiting on the silo flush gate per dispatched batch.");

    /// <summary>
    /// Wall-clock ms spent inside a single <c>ILattice</c> operation
    /// from the silo's perspective. The <c>mode</c> tag (carried by the
    /// dispatcher in <see cref="BenchWorkloadDispatcher"/>) disambiguates
    /// which ILattice call shape is being measured: <c>set-many</c>,
    /// <c>set-many-atomic</c>, <c>set-point</c>, <c>get-point</c>, or
    /// <c>get-many</c>. Renamed from <c>LatticeSetManyDurationMs</c> in
    /// throughput-capture-plan.md step 4 (the previous name only covered
    /// one of the five ops the silo now dispatches; the metric name now
    /// matches the actual coverage).
    /// </summary>
    public static readonly Histogram<double> LatticeOpDurationMs =
        Meter.CreateHistogram<double>(
            name: "azure.throughput.bench.lattice.op.duration_ms",
            unit: "ms",
            description: "Wall-clock ms per ILattice op call observed by the silo flusher; mode tag disambiguates set-many / set-many-atomic / set-point / get-point / get-many.");

    /// <summary>
    /// Number of retry attempts the silo flusher performed against a
    /// single <c>ILattice</c> op call after a transient
    /// <c>OrleansMessageRejectionException</c>. Recorded once per
    /// terminal outcome (success, exhaustion, or shutdown-discard) with
    /// the value being the *retry* count (0 on a first-try success).
    /// Carries the same <c>mode</c> tag as <see cref="LatticeOpDurationMs"/>.
    /// Step 8c-c-iv-a uses this to read the density of cold-start
    /// rejections off the phase-A scraper rather than inferring it from
    /// the failed-batch counter alone.
    /// </summary>
    public static readonly Histogram<int> LatticeOpRetryAttempts =
        Meter.CreateHistogram<int>(
            name: "azure.throughput.bench.lattice.op.retry_attempts",
            unit: "attempts",
            description: "Retry attempts per ILattice op call after a transient OrleansMessageRejectionException (0 on first-try success).");
}
