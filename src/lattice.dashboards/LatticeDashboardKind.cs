namespace Orleans.Lattice.Dashboards;

/// <summary>
/// Identifies a built-in Grafana dashboard bundled with
/// <see cref="LatticeDashboards"/>. Each value resolves to a single
/// embedded JSON resource and a focused operator workflow.
/// </summary>
public enum LatticeDashboardKind
{
    /// <summary>
    /// Per-tree throughput, leaf-write percentiles, cache hit-rate,
    /// tombstone churn, splits committed, atomic-write outcomes,
    /// coordinator completions, tree-lifecycle, event publish / drop,
    /// and runtime configuration changes. Sources the
    /// <c>orleans.lattice</c> meter only and does not require the
    /// replication package.
    /// </summary>
    Overview = 0,

    /// <summary>
    /// Targets the WAL-first commit pipeline:
    /// <c>leaf.commit.duration</c> broken out by step
    /// (<c>wal</c> / <c>apply</c> / <c>observer</c>),
    /// <c>leaf.write.duration</c> for storage-provider write latency,
    /// <c>leaf.compaction.duration</c> for tombstone-compaction latency,
    /// and activation-time <c>leaf.replay.duration</c> /
    /// <c>leaf.replay.entries</c> tagged by recovery outcome. Sources
    /// the <c>orleans.lattice</c> meter only.
    /// </summary>
    CommitPath = 1,

    /// <summary>
    /// Cross-cluster replication operator view: ship / apply / lag
    /// durations, WAL append vs trim throughput, dead-letter queue
    /// churn, apply FIFO violations, causal-wait histograms,
    /// fall-off-log events, and per-peer cursor lag. Sources the
    /// <c>orleans.lattice.replication</c> meter; useful only when the
    /// replication package is registered on the silo.
    /// </summary>
    Replication = 2,

    /// <summary>
    /// Deep-dive into the <c>SetManyAtomicAsync</c> saga: outcome
    /// rate, end-to-end saga duration percentiles
    /// (<c>atomic_write.duration</c>), batch-size percentiles
    /// (<c>atomic_write.batch_size</c>), per-tree committed
    /// throughput, and a dedicated saga-failure-rate panel
    /// (compensated + failed as a fraction of all terminal
    /// transitions). Sources the <c>orleans.lattice</c> meter only.
    /// </summary>
    AtomicWrites = 3,
}
