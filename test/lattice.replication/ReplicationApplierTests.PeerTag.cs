using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Pins the `peer` tag dimension on `apply.duration` and `apply.lag`
/// so the documented metric schema and the actual emission cannot
/// drift again. Each test asserts that the recorded sample carries
/// a `peer` tag whose value matches the entry's
/// <see cref="WalRecord.OriginClusterId"/> — the authoring cluster
/// of the replicated mutation, not the immediate transport hop. The
/// per-entry path's defence-in-depth gates and the batch path's
/// per-origin run grouping are both covered so a future refactor
/// of either pipeline preserves the per-source-peer attribution
/// operators rely on for inbound apply dashboards and per-peer
/// throughput break-down.
/// </summary>
public partial class ReplicationApplierTests
{
    private static bool HasPeer(IReadOnlyList<KeyValuePair<string, object?>> tags, string peer) =>
        tags.Any(t => t.Key == LatticeReplicationMetrics.TagPeer && (string?)t.Value == peer);

    [Test]
    public async Task ApplyAsync_apply_duration_tags_peer_with_origin_for_set()
    {
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyDurationName);
        var (applier, _, _, _) = CreateApplier();

        await applier.ApplyAsync(SetEntry("k", Hlc(10)));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.Multiple(() =>
        {
            Assert.That(HasTree(only.Tags, Tree), Is.True);
            Assert.That(HasPeer(only.Tags, RemoteCluster), Is.True);
            Assert.That(HasOutcome(only.Tags, LatticeReplicationMetrics.OutcomeSuccess), Is.True);
        });
    }

    [Test]
    public async Task ApplyAsync_apply_duration_tags_peer_for_range_delete()
    {
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyDurationName);
        var (applier, _, _, _) = CreateApplier();

        await applier.ApplyAsync(RangeDeleteEntry("a", "z"));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        Assert.That(HasPeer(collector.Measurements.Single().Tags, RemoteCluster), Is.True);
    }

    [Test]
    public async Task ApplyAsync_apply_duration_tags_peer_for_hwm_dedup()
    {
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyDurationName);
        var (applier, _, _, hwm) = CreateApplier();
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Hlc(100));

        await applier.ApplyAsync(SetEntry("k", Hlc(50)));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.Multiple(() =>
        {
            Assert.That(HasPeer(only.Tags, RemoteCluster), Is.True);
            Assert.That(HasOutcome(only.Tags, LatticeReplicationMetrics.OutcomeDedup), Is.True);
        });
    }

    [Test]
    public async Task ApplyAsync_apply_duration_tags_peer_for_local_origin_dedup()
    {
        // Local-origin defence-in-depth: peer tag carries the local
        // cluster id (the entry's origin), even though the apply was
        // a no-op. This preserves per-peer attribution for operators
        // monitoring which cluster's emits were filtered.
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyDurationName);
        var (applier, _, _, _) = CreateApplier();

        await applier.ApplyAsync(SetEntry("k", Hlc(10), origin: LocalCluster));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.Multiple(() =>
        {
            Assert.That(HasPeer(only.Tags, LocalCluster), Is.True);
            Assert.That(HasOutcome(only.Tags, LatticeReplicationMetrics.OutcomeDedup), Is.True);
        });
    }

    [Test]
    public void ApplyAsync_apply_duration_tags_peer_with_empty_string_when_origin_guard_throws()
    {
        // The origin guard fires inside the try, so the finally still
        // records a sample. The peer tag is the entry's OriginClusterId
        // (the empty string in this test); the recorder emits it
        // verbatim rather than skipping so cardinality stays stable
        // across failure outcomes.
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyDurationName);
        var (applier, _, _, _) = CreateApplier();
        var entry = SetEntry("k", Hlc(10)) with { OriginClusterId = string.Empty };

        Assert.That(
            async () => await applier.ApplyAsync(entry),
            Throws.InstanceOf<ArgumentException>());

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.Multiple(() =>
        {
            Assert.That(HasPeer(only.Tags, string.Empty), Is.True);
            Assert.That(HasOutcome(only.Tags, LatticeReplicationMetrics.OutcomeFailure), Is.True);
        });
    }

    [Test]
    public async Task ApplyAsync_apply_lag_tags_peer_with_origin_for_set()
    {
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyLagName);
        var (applier, _, _, _) = CreateApplier();

        var pastTicks = DateTime.UtcNow.Ticks - TimeSpan.FromMilliseconds(100).Ticks;
        await applier.ApplyAsync(SetEntry("k", Hlc(pastTicks)));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.Multiple(() =>
        {
            Assert.That(HasTree(only.Tags, Tree), Is.True);
            Assert.That(HasPeer(only.Tags, RemoteCluster), Is.True);
        });
    }

    [Test]
    public async Task ApplyAsync_apply_lag_tags_peer_for_delete()
    {
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyLagName);
        var (applier, _, _, _) = CreateApplier();

        var pastTicks = DateTime.UtcNow.Ticks - TimeSpan.FromMilliseconds(50).Ticks;
        await applier.ApplyAsync(DeleteEntry("k", Hlc(pastTicks)));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        Assert.That(HasPeer(collector.Measurements.Single().Tags, RemoteCluster), Is.True);
    }

    [Test]
    public async Task ApplyBatchAsync_apply_duration_tags_peer_per_run()
    {
        // Batch path: ApplyOriginRunAsync captures origin once at the
        // start of each (treeId, origin) run and threads it through
        // every per-entry RecordApplyDuration call site. A multi-entry
        // run from one origin must produce N samples all tagged with
        // that single origin value.
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyDurationName);
        var (applier, _, _, _) = CreateApplier();

        var batch = new[]
        {
            SetEntry("a", Hlc(10)),
            SetEntry("b", Hlc(20)),
            SetEntry("c", Hlc(30)),
        };

        await applier.ApplyBatchAsync(batch);

        Assert.That(collector.Measurements, Has.Count.EqualTo(3));
        Assert.That(
            collector.Measurements.All(m => HasPeer(m.Tags, RemoteCluster)),
            Is.True,
            "every per-entry sample in a same-origin run must tag peer with that origin");
    }

    [Test]
    public async Task ApplyBatchAsync_apply_duration_tags_peer_per_origin_for_mixed_batch()
    {
        // A batch that spans two origins must produce samples whose
        // peer tag matches the origin of each entry (not a single
        // dominant value). "Tag with the dominant origin" was the
        // alternative design, but the batch-grouping approach
        // already gives us per-origin tagging for free because each
        // run is one origin.
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyDurationName);
        var (applier, _, _, _) = CreateApplier();

        const string OtherCluster = "site-c";
        var batch = new[]
        {
            SetEntry("a", Hlc(10), origin: RemoteCluster),
            SetEntry("b", Hlc(20), origin: RemoteCluster),
            SetEntry("c", Hlc(30), origin: OtherCluster),
            SetEntry("d", Hlc(40), origin: OtherCluster),
        };

        await applier.ApplyBatchAsync(batch);

        Assert.That(collector.Measurements, Has.Count.EqualTo(4));
        var byPeer = collector.Measurements
            .GroupBy(m => m.Tags.First(t => t.Key == LatticeReplicationMetrics.TagPeer).Value)
            .ToDictionary(g => (string)g.Key!, g => g.Count());
        Assert.Multiple(() =>
        {
            Assert.That(byPeer.ContainsKey(RemoteCluster), Is.True);
            Assert.That(byPeer.ContainsKey(OtherCluster), Is.True);
            Assert.That(byPeer[RemoteCluster], Is.EqualTo(2));
            Assert.That(byPeer[OtherCluster], Is.EqualTo(2));
        });
    }
}
