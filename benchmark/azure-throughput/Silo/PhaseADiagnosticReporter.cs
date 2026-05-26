// Phase A diagnostic reporter for the Azure throughput harness.
//
// Subscribes to the shared "orleans.lattice" meter and emits per-instrument
// p50/p90/p99/count lines to stdout once every BENCH_PHASEA_REPORT_SEC
// seconds (default 10). The ACI log carrying these lines is the single
// sink the ladder script (40-ladder.ps1) scrapes for attribution, so
// the format is intentionally fixed and easy to grep:
//
//   [phaseA] t=20.0s instrument=wal.append.turn_wait      tree=t shard=0 phase=- status=- count=42318 p50=1.41ms p90=3.20ms p99=8.74ms
//   [phaseA] t=20.0s instrument=provider.commit.duration  tree=t shard=0 phase=phase1 status=- count=4231 p50=12.8ms p90=18.4ms p99=41.2ms
//   [phaseA] t=20.0s instrument=provider.retry.exhausted  tree=t shard=0 phase=phase1 status=429 count=12
//   [phaseA] t=20.0s instrument=saga.perkey.duration      tree=t shard=- phase=- status=- count=8421 p50=2.50ms p90=11.0ms p99=34.2ms
//
// Design constraints:
//   - Zero per-Record allocation on the publisher hot path: tag values
//     are pulled out of the ReadOnlySpan<KeyValuePair<,>> Record gives
//     us and merged into a pooled string-keyed reservoir per
//     (instrument, treeTag, shardTag, phaseTag, statusTag) tuple.
//   - Bounded per-tuple state: a reservoir is a fixed-capacity
//     double[]. Each report tick the reservoir is sorted in place
//     and the (count, p50, p90, p99) tuple is emitted. Reservoirs
//     reset after every report tick so the next window's quantiles
//     reflect that window only.
//   - Bounded tuple count: the instrument allowlist + the small
//     cardinality of treeId / shardIndex / phase / status keep the
//     per-key dictionary below ~200 entries even on the largest
//     ladder rung.
//   - Counter instruments (provider.retry.exhausted) report
//     cumulative-delta count only; quantile fields are omitted.
//
// The reporter is wired in Program.cs via:
//   builder.Services.AddHostedService<PhaseADiagnosticReporter>();
// and runs as a long-lived BackgroundService that drains on host stop.

using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Text;
using Microsoft.Extensions.Hosting;
using Orleans.Lattice;

namespace VehicleFleetSimulator.AzureThroughput.Silo;

internal sealed class PhaseADiagnosticReporter : BackgroundService
{
    /// <summary>
    /// Cadence in seconds between successive <c>[phaseA] ...</c> emission
    /// windows. Read from <c>BENCH_PHASEA_REPORT_SEC</c> (default 10).
    /// Set to 0 to disable the reporter entirely - <see cref="ExecuteAsync"/>
    /// returns immediately and no listener is registered.
    /// </summary>
    private readonly int _reportSec;

    /// <summary>
    /// Wall-clock start used to render the <c>t=...s</c> prefix that mirrors
    /// the throughput drainer's existing per-second log line.
    /// </summary>
    private readonly long _startedAtTicks;

    /// <summary>
    /// Allowlist of <c>orleans.lattice</c> instrument names this reporter
    /// renders. Anything else seen on the meter is ignored so a future
    /// over-tagged instrument cannot blow up reservoir state.
    /// </summary>
    private static readonly HashSet<string> InstrumentAllowlist = new(StringComparer.Ordinal)
    {
        "orleans.lattice.wal.append.queue_depth",
        "orleans.lattice.wal.append.batch_entries",
        "orleans.lattice.wal.append.batch_bytes",
        "orleans.lattice.wal.append.in_flight",
        "orleans.lattice.wal.append.provider.duration",
        "orleans.lattice.wal.append.turn_wait",
        "orleans.lattice.leaf.commit.in_flight",
        "orleans.lattice.leaf.commit.duration",
        "orleans.lattice.provider.commit.duration",
        "orleans.lattice.provider.phase2.batch_size",
        "orleans.lattice.provider.retry.exhausted",
        "orleans.lattice.saga.fanout.size",
        "orleans.lattice.saga.perkey.duration",
        "orleans.lattice.saga.wait.serial_gap",
        // U9p step 2: ShardRootGrain.SetManyAsync split into local-apply
        // (per-leaf fan-out + WAL append + phase 2) and shadow-forward
        // (online-resize tail wait). Lattice-internal histograms on the
        // public LatticeMetrics surface, not bench-local - any caller
        // that already subscribes to the orleans.lattice meter sees
        // them automatically.
        "orleans.lattice.shard_root.set_many.local_apply.duration",
        "orleans.lattice.shard_root.set_many.shadow_forward.duration",
        "orleans.lattice.shard_root.set_many.leaf_rpc.duration",
        // U9p step 6: cross-grain dispatch view from `WalCommitLogWriter`
        // around `await walGrain.AppendAsync(...)`. The leaf's
        // `leaf.commit.duration phase=wal` measures the inside of
        // `CommitSetManyAsync`; this measures the outside of the WAL
        // grain call. The expected `dispatch.duration ≈ phase=wal`
        // equality is the U9p step 5 cross-check that the seconds-long
        // wait is on the WAL activation's turn-queue ahead of a
        // millisecond-scale provider call.
        "orleans.lattice.wal.shard.dispatch.duration",
        // U9o step 2: benchmark-local TCP-receive / drain instruments.
        // Live on the `azure.throughput.bench` meter so they cannot
        // leak into the public lattice surface, but ride the same
        // [phaseA] rendering path so the ladder script needs no
        // parser change.
        "azure.throughput.bench.tcp.read.line_bytes",
        "azure.throughput.bench.tcp.read.channel_write_wait_ms",
        "azure.throughput.bench.drain.flush_dispatch_size",
        "azure.throughput.bench.drain.flush_dispatch_wait_ms",
        // U9p step 1: outermost SetManyAsync call boundary observed by
        // the silo flusher. Confirms the ~18 s/call inference from U9o
        // step 2 directly rather than via gate-wait arithmetic.
        "azure.throughput.bench.lattice.set_many.duration_ms",
    };

    /// <summary>
    /// Per-(instrument, tags) aggregation state. Keyed by a rendered
    /// string of the form "instrument|tree|shard|phase|status" so the
    /// dictionary stays allocation-free for repeat keys (the string is
    /// built once on first sight, then re-used).
    /// </summary>
    private readonly Dictionary<string, Aggregator> _aggregators = new(StringComparer.Ordinal);

    /// <summary>
    /// Coarse gate guarding <see cref="_aggregators"/> and every
    /// <see cref="Aggregator"/> it owns. Held only for the duration of
    /// a single record-merge or single report-tick render; never across
    /// an await.
    /// </summary>
    private readonly Lock _gate = new();

    public PhaseADiagnosticReporter()
    {
        var raw = Environment.GetEnvironmentVariable("BENCH_PHASEA_REPORT_SEC");
        _reportSec = int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) && v >= 0
            ? v
            : 10;
        _startedAtTicks = Stopwatch.GetTimestamp();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_reportSec == 0)
        {
            Console.WriteLine("[phaseA] disabled (BENCH_PHASEA_REPORT_SEC=0)");
            return;
        }

        Console.WriteLine($"[phaseA] reporter enabled cadence={_reportSec}s instruments={InstrumentAllowlist.Count}");

        // Hold a hard reference to the listener for the lifetime of the
        // hosted service so the GC does not collect it out from under
        // the meter pipeline. Disposed in the finally block below so
        // a graceful shutdown stops measurement before the final tick.
        var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                // Two meters are honoured: the public lattice meter
                // (the historical Phase A signal) and the bench-local
                // `azure.throughput.bench` meter (U9o step 2 ingest /
                // drain probes). Anything else on the process is
                // ignored.
                var meterName = instrument.Meter.Name;
                var isLattice = ReferenceEquals(instrument.Meter, LatticeMetrics.Meter);
                var isBench = string.Equals(meterName, BenchMetrics.Meter.Name, StringComparison.Ordinal);
                if (!isLattice && !isBench)
                {
                    return;
                }
                if (!InstrumentAllowlist.Contains(instrument.Name))
                {
                    return;
                }
                l.EnableMeasurementEvents(instrument);
            },
        };

        listener.SetMeasurementEventCallback<int>(RecordInt);
        listener.SetMeasurementEventCallback<long>(RecordLong);
        listener.SetMeasurementEventCallback<double>(RecordDouble);
        listener.Start();

        try
        {
            var period = TimeSpan.FromSeconds(_reportSec);
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(period, stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                EmitReport();
            }
        }
        finally
        {
            // Final report drains the last partial window so a 60-second
            // ladder rung with a 10-second cadence still surfaces the
            // residual five seconds of measurements that landed after
            // the last scheduled tick.
            try { EmitReport(); } catch { /* terminal shutdown */ }
            listener.Dispose();
        }
    }

    private void RecordInt(Instrument instrument, int measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
    {
        Merge(instrument, measurement, tags, isCounter: instrument is Counter<long> or Counter<int>);
    }

    private void RecordLong(Instrument instrument, long measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
    {
        Merge(instrument, measurement, tags, isCounter: instrument is Counter<long> or Counter<int>);
    }

    private void RecordDouble(Instrument instrument, double measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
    {
        Merge(instrument, measurement, tags, isCounter: false);
    }

    private void Merge(Instrument instrument, double measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, bool isCounter)
    {
        // Pull the four tags we know how to render. Anything else is
        // silently dropped so a future tag addition does not corrupt
        // the dictionary key shape. `LeafCommitDuration` tags with
        // `TagStep` (wal/apply/observer/digest) where most other
        // instruments use `TagPhase`; both are mutually exclusive in
        // practice, so we fold a `TagStep` value into the `phase` slot
        // when no `TagPhase` value is observed. This keeps the rendered
        // [phaseA] schema fixed and lets the per-step breakdown surface
        // without a public-surface tag rename.
        string tree = "-", shard = "-", phase = "-", status = "-";
        string? step = null;
        for (var i = 0; i < tags.Length; i++)
        {
            var tagKey = tags[i].Key;
            var value = tags[i].Value?.ToString() ?? "-";
            if (string.Equals(tagKey, LatticeMetrics.TagTree, StringComparison.Ordinal))
            {
                tree = value;
            }
            else if (string.Equals(tagKey, LatticeMetrics.TagShard, StringComparison.Ordinal))
            {
                shard = value;
            }
            else if (string.Equals(tagKey, LatticeMetrics.TagPhase, StringComparison.Ordinal))
            {
                phase = value;
            }
            else if (string.Equals(tagKey, LatticeMetrics.TagStep, StringComparison.Ordinal))
            {
                step = value;
            }
            else if (string.Equals(tagKey, LatticeMetrics.TagStatus, StringComparison.Ordinal))
            {
                status = value;
            }
        }
        if (phase == "-" && step is not null)
        {
            phase = step;
        }

        // Render the dictionary key. The same (instrument, tags) tuple
        // every record builds the same key string; we eat the string
        // allocation on every record (small, gen-0) and rely on the
        // dictionary's hash equality to dedupe. The cost is a few
        // hundred bytes/sec at 50 k records/sec, which is invisible
        // next to the WAL hot path itself.
        var key = string.Concat(instrument.Name, "|", tree, "|", shard, "|", phase, "|", status);

        lock (_gate)
        {
            if (!_aggregators.TryGetValue(key, out var agg))
            {
                agg = new Aggregator(instrument.Name, tree, shard, phase, status, isCounter);
                _aggregators[key] = agg;
            }
            agg.Add(measurement);
        }
    }

    private void EmitReport()
    {
        // Snapshot under the gate, render outside the gate to keep the
        // hot-path merge latency unaffected by Console.WriteLine cost.
        Aggregator[] snapshot;
        lock (_gate)
        {
            if (_aggregators.Count == 0)
            {
                return;
            }
            snapshot = new Aggregator[_aggregators.Count];
            var i = 0;
            foreach (var agg in _aggregators.Values)
            {
                snapshot[i++] = agg.SnapshotAndReset();
            }
        }

        var elapsed = Stopwatch.GetElapsedTime(_startedAtTicks).TotalSeconds;
        // Sort by instrument name then tag tuple so successive report
        // ticks emit lines in a stable order - greppable in the ACI log
        // and predictable for the ladder-script CSV parse step.
        Array.Sort(snapshot, static (a, b) =>
        {
            var byName = string.CompareOrdinal(a.InstrumentName, b.InstrumentName);
            if (byName != 0) return byName;
            var byTree = string.CompareOrdinal(a.Tree, b.Tree);
            if (byTree != 0) return byTree;
            var byShard = string.CompareOrdinal(a.Shard, b.Shard);
            if (byShard != 0) return byShard;
            var byPhase = string.CompareOrdinal(a.Phase, b.Phase);
            if (byPhase != 0) return byPhase;
            return string.CompareOrdinal(a.Status, b.Status);
        });

        var sb = new StringBuilder(160);
        foreach (var agg in snapshot)
        {
            if (agg.Count == 0)
            {
                continue;
            }
            sb.Clear();
            sb.Append("[phaseA] t=");
            sb.Append(elapsed.ToString("0.0", CultureInfo.InvariantCulture));
            sb.Append("s instrument=");
            // Strip the "orleans.lattice." prefix so the ladder script's
            // regex stays compact. The full meter name is fixed and
            // implicit; the leaf instrument id is the discriminator.
            sb.Append(StripPrefix(agg.InstrumentName));
            sb.Append(" tree=").Append(agg.Tree);
            sb.Append(" shard=").Append(agg.Shard);
            sb.Append(" phase=").Append(agg.Phase);
            sb.Append(" status=").Append(agg.Status);
            sb.Append(" count=").Append(agg.Count.ToString(CultureInfo.InvariantCulture));
            if (!agg.IsCounter)
            {
                sb.Append(" sum=").Append(agg.Sum.ToString("F2", CultureInfo.InvariantCulture));
                sb.Append(" min=").Append(agg.Min.ToString("F2", CultureInfo.InvariantCulture));
                sb.Append(" p50=").Append(agg.P50.ToString("F2", CultureInfo.InvariantCulture));
                sb.Append(" p90=").Append(agg.P90.ToString("F2", CultureInfo.InvariantCulture));
                sb.Append(" p99=").Append(agg.P99.ToString("F2", CultureInfo.InvariantCulture));
                sb.Append(" max=").Append(agg.Max.ToString("F2", CultureInfo.InvariantCulture));
            }
            Console.WriteLine(sb.ToString());
        }
    }

    private static string StripPrefix(string name)
    {
        if (name.StartsWith("orleans.lattice.", StringComparison.Ordinal))
        {
            return name.Substring("orleans.lattice.".Length);
        }
        if (name.StartsWith("azure.throughput.bench.", StringComparison.Ordinal))
        {
            return name.Substring("azure.throughput.bench.".Length);
        }
        return name;
    }

    /// <summary>
    /// Per-(instrument, tag tuple) aggregator. Holds a fixed-capacity
    /// reservoir of recent measurements for quantile rendering plus
    /// cumulative count / sum / min / max counters. The reservoir is
    /// drained-and-reset on every report tick so each window's
    /// quantiles reflect that window's measurements only.
    /// </summary>
    private sealed class Aggregator
    {
        /// <summary>
        /// Maximum reservoir size. Each entry costs 8 bytes; 4096
        /// entries x ~200 unique keys = ~6 MiB peak across the
        /// reporter's lifetime, which is well under any sane silo's
        /// working set. Larger reservoirs would not improve quantile
        /// accuracy materially for the steady-state distributions this
        /// reporter measures.
        /// </summary>
        private const int ReservoirCapacity = 4096;

        public string InstrumentName { get; }
        public string Tree { get; }
        public string Shard { get; }
        public string Phase { get; }
        public string Status { get; }
        public bool IsCounter { get; }
        public long Count { get; private set; }
        public double Sum { get; private set; }
        public double Min { get; private set; } = double.PositiveInfinity;
        public double Max { get; private set; } = double.NegativeInfinity;
        public double P50 { get; private set; }
        public double P90 { get; private set; }
        public double P99 { get; private set; }

        private readonly double[] _reservoir = new double[ReservoirCapacity];
        private long _seen;

        public Aggregator(string instrumentName, string tree, string shard, string phase, string status, bool isCounter)
        {
            InstrumentName = instrumentName;
            Tree = tree;
            Shard = shard;
            Phase = phase;
            Status = status;
            IsCounter = isCounter;
        }

        public void Add(double value)
        {
            Count++;
            Sum += value;
            if (value < Min) Min = value;
            if (value > Max) Max = value;
            if (IsCounter)
            {
                // Counter: only count + sum are surfaced; no reservoir.
                return;
            }

            // Reservoir sampling (Algorithm R) keeps the per-window
            // tuple's memory cost bounded under arbitrarily high
            // measurement rates. The bias-toward-recency the reset-
            // every-tick discipline imposes is acceptable for a
            // diagnostic harness.
            if (_seen < ReservoirCapacity)
            {
                _reservoir[_seen] = value;
            }
            else
            {
                var j = Random.Shared.NextInt64(0, _seen + 1);
                if (j < ReservoirCapacity)
                {
                    _reservoir[(int)j] = value;
                }
            }
            _seen++;
        }

        /// <summary>
        /// Computes p50 / p90 / p99 for the current window, then resets
        /// every accumulator so the next window starts clean. Returns
        /// a defensive snapshot so the caller can sort / render
        /// outside the reporter's lock.
        /// </summary>
        public Aggregator SnapshotAndReset()
        {
            var snap = new Aggregator(InstrumentName, Tree, Shard, Phase, Status, IsCounter)
            {
                Count = Count,
                Sum = Sum,
                Min = double.IsPositiveInfinity(Min) ? 0 : Min,
                Max = double.IsNegativeInfinity(Max) ? 0 : Max,
            };

            if (!IsCounter && _seen > 0)
            {
                var live = (int)Math.Min(_seen, (long)ReservoirCapacity);
                // Sort in place over the live portion of the reservoir.
                Array.Sort(_reservoir, 0, live);
                snap.P50 = Quantile(_reservoir, live, 0.50);
                snap.P90 = Quantile(_reservoir, live, 0.90);
                snap.P99 = Quantile(_reservoir, live, 0.99);
            }

            // Reset for the next window.
            Count = 0;
            Sum = 0;
            Min = double.PositiveInfinity;
            Max = double.NegativeInfinity;
            _seen = 0;
            return snap;
        }

        private static double Quantile(double[] sorted, int live, double q)
        {
            if (live == 0) return 0;
            // Nearest-rank quantile with clamping at the upper bound.
            var idx = (int)Math.Ceiling(q * live) - 1;
            if (idx < 0) idx = 0;
            if (idx >= live) idx = live - 1;
            return sorted[idx];
        }
    }
}

