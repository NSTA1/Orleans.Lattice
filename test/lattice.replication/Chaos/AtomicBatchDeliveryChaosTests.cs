using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// End-to-end chaos verification for <b>cross-cluster atomic-batch
/// delivery</b>. Pins the four contracts the cross-cluster atomic
/// visibility wave collectively guarantees but no single component
/// asserts end-to-end:
/// <list type="number">
/// <item><description>
/// <b>Sustained load + partition.</b> Atomic-batch traffic delivered
/// across a partition-then-heal mid-workload converges to full atomic
/// visibility on every receiver (every key of every committed batch
/// is present, never split).
/// </description></item>
/// <item><description>
/// <b>Producer crash mid-saga.</b> A partial atomic batch staged on
/// the receiver but missing its remaining siblings is evicted by the
/// orphan-timeout sweep, parked on the per-tree DLQ tagged
/// <see cref="LatticeReplicationMetrics.ReasonOrphanTransaction"/>,
/// and the per-origin high-water-mark advances past the orphan so
/// causal-stream progress resumes.
/// </description></item>
/// <item><description>
/// <b>Snapshot-during-saga.</b> An <see cref="ISnapshotProvider.ExportAsync"/>
/// running concurrently with an in-flight saga that does not drain
/// within the configured quiesce window adds the saga's transaction
/// id to <see cref="SnapshotStream.SagaBlacklist"/>; the receiver-side
/// staging buffer rejects subsequent admissions for blacklisted ids
/// via <see cref="TxBufferAdmissionResult.BlacklistedBypass"/> so the
/// applier degrades to point-apply rather than stalling.
/// </description></item>
/// <item><description>
/// <b>Buffer overflow.</b> 100 concurrent partial transactions
/// admitted to a buffer with
/// <see cref="LatticeReplicationOptions.AtomicBatchBufferMaxTransactions"/>=4
/// surface as 96 capacity-evictions to the DLQ tagged
/// <see cref="LatticeReplicationMetrics.ReasonEvicted"/>; the
/// terminal-outcome counter
/// <see cref="LatticeReplicationMetrics.ApplyTxCompleted"/> records
/// exactly 96 increments under
/// <see cref="LatticeReplicationMetrics.OutcomeTxEvictedCapacity"/>;
/// the four still-buffered transactions are not (yet) terminal.
/// </description></item>
/// </list>
/// <para>
/// Marked <see cref="CategoryAttribute"/>=<c>Chaos</c> and
/// <see cref="NonParallelizableAttribute"/> per repo convention so
/// the inner-loop suite (run with <c>--filter "TestCategory!=Chaos"</c>)
/// excludes them; CI runs the full chaos batch separately.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public partial class AtomicBatchDeliveryChaosTests
{
    /// <summary>
    /// Tree id every test scopes its workload to. Distinct from the
    /// other chaos suites so concurrent runs cannot collide on
    /// reserved system trees, and short enough to read inline.
    /// </summary>
    private const string TreeName = "chaos-atomic-batch";

    /// <summary>
    /// Producer cluster id used by the multi-site sustained-load test
    /// and by the single-cluster scenarios as the "remote" origin so
    /// the receiver-side dedupe path treats incoming entries as
    /// foreign-origin (the canonical apply path).
    /// </summary>
    private const string LocalClusterId = "site-0";

    /// <summary>
    /// Generous wall-clock window used by the sustained-load test
    /// when waiting for every authored batch to converge across every
    /// receiver. Sized to absorb worst-case grain reactivation under
    /// cluster restart + 200 ms partition-heal jitter without
    /// timing out the test on a contended CI runner. The drain
    /// stability window inside <see cref="ChaosDeliveryPump.DrainAsync"/>
    /// is multiplicatively shorter; this is the outer-loop ceiling.
    /// </summary>
    private static readonly TimeSpan DrainTimeout = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Subscribes to the
    /// <see cref="LatticeReplicationMetrics.ApplyTxCompleted"/>
    /// counter and partitions every emitted measurement by the
    /// <see cref="LatticeReplicationMetrics.TagOutcome"/> tag so the
    /// terminal-outcome accounting invariant can sum across each
    /// bucket. Distinct from the project's existing
    /// <c>MeterCollector&lt;T&gt;</c>: this collector flattens tags
    /// into a typed projection and exposes per-outcome sums rather
    /// than the raw measurement list.
    /// </summary>
    private sealed class TxOutcomeCollector : IDisposable
    {
        private readonly MeterListener _listener;
        private readonly ConcurrentBag<(long Value, string Outcome, string Tree)> _samples = new();

        public TxOutcomeCollector()
        {
            _listener = new MeterListener
            {
                InstrumentPublished = (instrument, l) =>
                {
                    if (instrument.Meter.Name == LatticeReplicationMetrics.MeterName
                        && instrument.Name == LatticeReplicationMetrics.ApplyTxCompletedName)
                    {
                        l.EnableMeasurementEvents(instrument);
                    }
                },
            };
            _listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
            {
                string? outcome = null;
                string? tree = null;
                for (var i = 0; i < tags.Length; i++)
                {
                    var tag = tags[i];
                    if (tag.Key == LatticeReplicationMetrics.TagOutcome && tag.Value is string o)
                    {
                        outcome = o;
                    }
                    else if (tag.Key == LatticeReplicationMetrics.TagTree && tag.Value is string t)
                    {
                        tree = t;
                    }
                }

                if (outcome is null || tree is null)
                {
                    return;
                }

                _samples.Add((value, outcome, tree));
            });
            _listener.Start();
        }

        /// <summary>
        /// Returns the sum of every recorded counter sample matching
        /// the supplied outcome tag and scoped to the supplied tree.
        /// Counter increments default to <c>1</c> per call site but
        /// the helper sums values to stay forward-compatible with
        /// any future call site that records a non-unit increment.
        /// </summary>
        public long SumFor(string outcome, string treeName)
        {
            long total = 0;
            foreach (var sample in _samples)
            {
                if (string.Equals(sample.Outcome, outcome, StringComparison.Ordinal)
                    && string.Equals(sample.Tree, treeName, StringComparison.Ordinal))
                {
                    total += sample.Value;
                }
            }
            return total;
        }

        /// <summary>
        /// Returns the sum across every recorded outcome bucket for
        /// the supplied tree. Used by the terminal-outcome accounting
        /// invariant: every transaction admitted to the buffer
        /// reaches exactly one terminal outcome, so the sum across
        /// every bucket must equal the total admitted count.
        /// </summary>
        public long TotalFor(string treeName)
        {
            long total = 0;
            foreach (var sample in _samples)
            {
                if (string.Equals(sample.Tree, treeName, StringComparison.Ordinal))
                {
                    total += sample.Value;
                }
            }
            return total;
        }

        public void Dispose() => _listener.Dispose();
    }

    /// <summary>
    /// Manifest of an atomic batch the chaos workload authored: the
    /// keys (in canonical order) and the batch's identifying tag, so
    /// a post-drain reader can verify atomic visibility per batch
    /// (every key present together, never split).
    /// </summary>
    private readonly record struct AuthoredBatch(string Tag, IReadOnlyList<string> Keys);

    /// <summary>
    /// Polls until <paramref name="predicate"/> is true or the
    /// supplied <paramref name="timeout"/> elapses. Returns
    /// <c>true</c> on the predicate satisfying, <c>false</c> on
    /// timeout. Uses <see cref="Stopwatch"/> for monotonic deadline
    /// computation so an NTP correction during the wait does not
    /// truncate the budget.
    /// </summary>
    private static async Task<bool> WaitForAsync(
        Func<Task<bool>> predicate,
        TimeSpan timeout,
        TimeSpan? pollInterval = null)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        var poll = pollInterval ?? TimeSpan.FromMilliseconds(50);
        var sw = Stopwatch.StartNew();
        while (sw.Elapsed < timeout)
        {
            if (await predicate().ConfigureAwait(false))
            {
                return true;
            }
            await Task.Delay(poll).ConfigureAwait(false);
        }
        return false;
    }

    /// <summary>
    /// Subscribes to the dead-letter <c>enqueued</c> counter and
    /// partitions samples by the
    /// <see cref="LatticeReplicationMetrics.TagReason"/> /
    /// <see cref="LatticeReplicationMetrics.TagTree"/> pair. Used to
    /// pin the canonical reason-tag constants
    /// (<see cref="LatticeReplicationMetrics.ReasonOrphanTransaction"/>,
    /// <see cref="LatticeReplicationMetrics.ReasonEvicted"/>) on the
    /// enqueue path rather than the structurally-weak free-text
    /// <see cref="DeadLetterEntry.FailureReason"/>. Mirrors
    /// <see cref="TxOutcomeCollector"/> but on the dead-letter
    /// counter rather than the tx-completed counter.
    /// </summary>
    private sealed class DlqReasonCollector : IDisposable
    {
        private readonly MeterListener _listener;
        private readonly ConcurrentBag<(long Value, string Reason, string Tree)> _samples = new();

        public DlqReasonCollector()
        {
            _listener = new MeterListener
            {
                InstrumentPublished = (instrument, l) =>
                {
                    if (instrument.Meter.Name == LatticeReplicationMetrics.MeterName
                        && instrument.Name == "orleans.lattice.replication.dead_letter.enqueued")
                    {
                        l.EnableMeasurementEvents(instrument);
                    }
                },
            };
            _listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
            {
                string? reason = null;
                string? tree = null;
                for (var i = 0; i < tags.Length; i++)
                {
                    var tag = tags[i];
                    if (tag.Key == LatticeReplicationMetrics.TagReason && tag.Value is string r)
                    {
                        reason = r;
                    }
                    else if (tag.Key == LatticeReplicationMetrics.TagTree && tag.Value is string t)
                    {
                        tree = t;
                    }
                }
                if (reason is null || tree is null)
                {
                    return;
                }
                _samples.Add((value, reason, tree));
            });
            _listener.Start();
        }

        /// <summary>Returns the sum of every recorded enqueue sample matching the supplied reason and tree.</summary>
        public long SumFor(string reason, string treeName)
        {
            long total = 0;
            foreach (var sample in _samples)
            {
                if (string.Equals(sample.Reason, reason, StringComparison.Ordinal)
                    && string.Equals(sample.Tree, treeName, StringComparison.Ordinal))
                {
                    total += sample.Value;
                }
            }
            return total;
        }

        public void Dispose() => _listener.Dispose();
    }

    /// <summary>
    /// Subscribes to the
    /// <see cref="LatticeReplicationMetrics.ApplyTxApplyDurationMs"/>
    /// histogram and counts samples by outcome bucket. Used by the
    /// histogram carve-out test to pin R-101's invariant that the
    /// duration histogram is recorded for <c>success</c> and
    /// <c>dlq_apply_failure</c> but intentionally NOT for
    /// <c>dlq_orphan</c> or <c>evicted_capacity</c>.
    /// </summary>
    private sealed class HistogramOutcomeCollector : IDisposable
    {
        private readonly MeterListener _listener;
        private readonly ConcurrentBag<(double Value, string Outcome, string Tree)> _samples = new();

        public HistogramOutcomeCollector()
        {
            _listener = new MeterListener
            {
                InstrumentPublished = (instrument, l) =>
                {
                    if (instrument.Meter.Name == LatticeReplicationMetrics.MeterName
                        && instrument.Name == LatticeReplicationMetrics.ApplyTxApplyDurationMsName)
                    {
                        l.EnableMeasurementEvents(instrument);
                    }
                },
            };
            _listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) =>
            {
                string? outcome = null;
                string? tree = null;
                for (var i = 0; i < tags.Length; i++)
                {
                    var tag = tags[i];
                    if (tag.Key == LatticeReplicationMetrics.TagOutcome && tag.Value is string o)
                    {
                        outcome = o;
                    }
                    else if (tag.Key == LatticeReplicationMetrics.TagTree && tag.Value is string t)
                    {
                        tree = t;
                    }
                }
                if (outcome is null || tree is null)
                {
                    return;
                }
                _samples.Add((value, outcome, tree));
            });
            _listener.Start();
        }

        /// <summary>Returns the number of histogram samples recorded for the supplied outcome and tree.</summary>
        public int CountFor(string outcome, string treeName)
        {
            var count = 0;
            foreach (var sample in _samples)
            {
                if (string.Equals(sample.Outcome, outcome, StringComparison.Ordinal)
                    && string.Equals(sample.Tree, treeName, StringComparison.Ordinal))
                {
                    count++;
                }
            }
            return count;
        }

        public void Dispose() => _listener.Dispose();
    }

    /// <summary>
    /// Subscribes to the
    /// <see cref="LatticeReplicationMetrics.ApplyTxBufferBytes"/>
    /// up/down counter and tracks the running per-tree sum so a test
    /// can pin the gauge's drain-to-zero invariant after every
    /// staged transaction reaches a terminal disposition.
    /// </summary>
    private sealed class BufferBytesCollector : IDisposable
    {
        private readonly MeterListener _listener;
        private readonly object _gate = new();
        private readonly Dictionary<string, long> _bytesByTree = new(StringComparer.Ordinal);

        public BufferBytesCollector()
        {
            _listener = new MeterListener
            {
                InstrumentPublished = (instrument, l) =>
                {
                    if (instrument.Meter.Name == LatticeReplicationMetrics.MeterName
                        && instrument.Name == LatticeReplicationMetrics.ApplyTxBufferBytesName)
                    {
                        l.EnableMeasurementEvents(instrument);
                    }
                },
            };
            _listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
            {
                string? tree = null;
                for (var i = 0; i < tags.Length; i++)
                {
                    var tag = tags[i];
                    if (tag.Key == LatticeReplicationMetrics.TagTree && tag.Value is string t)
                    {
                        tree = t;
                        break;
                    }
                }
                if (tree is null)
                {
                    return;
                }
                lock (_gate)
                {
                    _bytesByTree.TryGetValue(tree, out var current);
                    _bytesByTree[tree] = current + value;
                }
            });
            _listener.Start();
        }

        /// <summary>Returns the running gauge value for <paramref name="treeName"/>.</summary>
        public long CurrentFor(string treeName)
        {
            lock (_gate)
            {
                return _bytesByTree.TryGetValue(treeName, out var v) ? v : 0L;
            }
        }

        public void Dispose() => _listener.Dispose();
    }
}

