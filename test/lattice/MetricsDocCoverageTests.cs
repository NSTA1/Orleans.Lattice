using System.Diagnostics.Metrics;
using NUnit.Framework;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Asserts every instrument on the core <c>orleans.lattice</c> meter is documented,
/// by its exact dotted name, in both the metrics catalog and the
/// instrument-to-panel reference map, reusing the shared
/// <see cref="MetricsDocCoverageTestsBase"/> drift guard so a new core instrument
/// cannot ship without a documentation entry.
/// </summary>
[TestFixture]
public sealed class MetricsDocCoverageTests : MetricsDocCoverageTestsBase
{
    protected override Meter Meter => LatticeMetrics.Meter;

    protected override IEnumerable<string> DocRelativePaths => new[]
    {
        "docs/lattice/metrics.md",
        "docs/lattice.dashboards/metrics-to-panel-map.md",
    };

    // Pre-existing instrument-to-panel-map backlog: these core instruments predate
    // this drift guard and are catalogued in docs/lattice/metrics.md but not yet
    // carried through into docs/lattice.dashboards/metrics-to-panel-map.md. They are
    // allow-listed so the guard bites for every FUTURE core instrument while the
    // backlog is worked off; removing a name here (after adding its panel-map row)
    // must keep this test green. Tracked as a documentation follow-up.
    protected override IReadOnlySet<string> IntentionallyUndocumented { get; } =
        new HashSet<string>(StringComparer.Ordinal)
        {
            "orleans.lattice.atomic_write.cross_tree.completed",
            "orleans.lattice.atomic_write.cross_tree.duration",
            "orleans.lattice.atomic_write.cross_tree.participants",
            "orleans.lattice.exists.duration",
            "orleans.lattice.get.duration",
            "orleans.lattice.get.stage.duration",
            "orleans.lattice.get_many.duration",
            "orleans.lattice.get_many.stage.duration",
            "orleans.lattice.get_with_version.duration",
            "orleans.lattice.leaf.commit.in_flight",
            "orleans.lattice.leaf.digest.publishes",
            "orleans.lattice.provider.commit.duration",
            "orleans.lattice.provider.idempotent_replays",
            "orleans.lattice.provider.phase2.batch_size",
            "orleans.lattice.provider.retry.attempts",
            "orleans.lattice.provider.retry.exhausted",
            "orleans.lattice.saga.broadcast.duration",
            "orleans.lattice.saga.broadcast.leaf.duration",
            "orleans.lattice.saga.broadcast.shard.duration",
            "orleans.lattice.saga.broadcast.shard.stage.duration",
            "orleans.lattice.saga.checkpoint.duration",
            "orleans.lattice.saga.fanout.size",
            "orleans.lattice.saga.perkey.duration",
            "orleans.lattice.saga.prepare.duration",
            "orleans.lattice.saga.reminder.duration",
            "orleans.lattice.saga.terminal_decision.duration",
            "orleans.lattice.saga.wait.serial_gap",
            "orleans.lattice.set.duration",
            "orleans.lattice.set.stage.duration",
            "orleans.lattice.set_many.duration",
            "orleans.lattice.set_many.stage.duration",
            "orleans.lattice.shard_root.set_many.leaf_rpc.duration",
            "orleans.lattice.shard_root.set_many.local_apply.duration",
            "orleans.lattice.shard_root.set_many.shadow_forward.duration",
            "orleans.lattice.storage.wal.compression_skipped",
            "orleans.lattice.storage.wal.stored_bytes",
            "orleans.lattice.storage.wal.uncompressed_bytes",
            "orleans.lattice.wal.append.batch_bytes",
            "orleans.lattice.wal.append.batch_entries",
            "orleans.lattice.wal.append.in_flight",
            "orleans.lattice.wal.append.provider.duration",
            "orleans.lattice.wal.append.queue_depth",
            "orleans.lattice.wal.append.turn_wait",
            "orleans.lattice.wal.saturation.transitions",
            "orleans.lattice.wal.shard.dispatch.duration",
            "orleans.lattice.wal.shard.dispatch.entries",
            "orleans.lattice.warmup.duration",
            "orleans.lattice.warmup.invocations",
        };
}
