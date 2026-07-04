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
    /// runtime configuration changes, and top-of-stack
    /// <c>GetAsync</c> / <c>GetManyAsync</c> / <c>ExistsAsync</c> /
    /// <c>GetWithVersionAsync</c> per-call latency envelopes with
    /// per-stage decomposition (<c>get.duration</c>,
    /// <c>get.stage.duration</c>, <c>get_many.duration</c>,
    /// <c>get_many.stage.duration</c>, <c>exists.duration</c>,
    /// <c>get_with_version.duration</c>). Sources the
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

    /// <summary>
    /// Cluster-wide health of asynchronous materialised views: apply lag and
    /// drain-backlog-depth percentiles, filter / re-project and aggregation
    /// apply throughput, and the operator warnings - lag-budget evictions,
    /// re-key collisions, atomic-staging backstop fall-backs, and cross-tree
    /// joint-atomicity violations. Every panel is keyed by view name (and,
    /// where deployments span clusters, by cluster); because a view's
    /// maintainer is a single grain activation that migrates between silos,
    /// the dashboard deliberately offers no per-silo filter and aggregates
    /// across the whole cluster. Sources the <c>orleans.lattice</c> meter
    /// only; the instruments are emitted whenever a WAL-backed view is
    /// registered and do not require the replication package.
    /// </summary>
    MaterialisedViews = 4,

    /// <summary>
    /// Identity and authorization operator view. Charts the enforcement gate's
    /// decision throughput (by <c>effect</c> and by <c>operation</c>),
    /// decision-latency percentiles (<c>decision.duration</c>), compiled-snapshot
    /// rebuild rate and the snapshot <c>epoch</c> / <c>age</c> gauges from the
    /// <c>orleans.lattice.auth</c> meter, alongside the subject-resolution cache
    /// hit-ratio and hit / miss throughput from the
    /// <c>orleans.lattice.membership</c> meter. Sources the
    /// <c>orleans.lattice.auth</c> and <c>orleans.lattice.membership</c> meters;
    /// useful only when the authentication / authorization packages are
    /// registered on the silo.
    /// </summary>
    Authorization = 5,
}
