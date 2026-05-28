using System.Text;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

// Phase D-instr (c2-xv routing memo): the saga's three internal phases
// (prepare via lattice.SetManyAsync, terminal-decision write via the
// TxRegistry, per-shard terminal broadcast) are each timed by a new
// histogram on the LatticeMetrics surface. These integration tests
// pin that each histogram emits at least one observation when a saga
// commits, tagged with the saga's tree id, so a future change cannot
// silently remove the instrumentation and break the dashboards' saga
// p50 decomposition.
public partial class LatticeMetricsIntegrationTests
{
    [Test]
    public async Task SetManyAtomicAsync_emits_saga_prepare_duration_histogram()
    {
        using var recorder = new MetricRecorder();
        var treeId = $"metrics-saga-prepare-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        var entries = new List<KeyValuePair<string, byte[]>>
        {
            new("a", Encoding.UTF8.GetBytes("1")),
            new("b", Encoding.UTF8.GetBytes("2")),
            new("c", Encoding.UTF8.GetBytes("3")),
        };
        await tree.SetManyAtomicAsync(entries);

        var count = recorder.CountFor("orleans.lattice.saga.prepare.duration", treeId);
        Assert.That(count, Is.GreaterThanOrEqualTo(1),
            "Expected at least one saga.prepare.duration observation tagged with the saga's tree id.");
    }

    [Test]
    public async Task SetManyAtomicAsync_emits_saga_terminal_decision_duration_histogram()
    {
        using var recorder = new MetricRecorder();
        var treeId = $"metrics-saga-decision-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        var entries = new List<KeyValuePair<string, byte[]>>
        {
            new("a", Encoding.UTF8.GetBytes("1")),
            new("b", Encoding.UTF8.GetBytes("2")),
        };
        await tree.SetManyAtomicAsync(entries);

        var count = recorder.CountFor("orleans.lattice.saga.terminal_decision.duration", treeId);
        Assert.That(count, Is.GreaterThanOrEqualTo(1),
            "Expected at least one saga.terminal_decision.duration observation tagged with the saga's tree id.");
    }

    [Test]
    public async Task SetManyAtomicAsync_emits_saga_broadcast_duration_histogram()
    {
        using var recorder = new MetricRecorder();
        var treeId = $"metrics-saga-broadcast-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        var entries = new List<KeyValuePair<string, byte[]>>
        {
            new("a", Encoding.UTF8.GetBytes("1")),
            new("b", Encoding.UTF8.GetBytes("2")),
        };
        await tree.SetManyAtomicAsync(entries);

        var count = recorder.CountFor("orleans.lattice.saga.broadcast.duration", treeId);
        Assert.That(count, Is.GreaterThanOrEqualTo(1),
            "Expected at least one saga.broadcast.duration observation tagged with the saga's tree id.");
    }

    [Test]
    public async Task SetManyAtomicAsync_phase_histograms_carry_wal_partitions_tag()
    {
        // The three saga-phase histograms share the same (tree, walPartitions)
        // tag pair the existing SagaFanoutSize / SagaPerKeyDuration instruments
        // use, so dashboards can correlate phase costs against the tree's
        // configured WAL partition count without joining across instruments.
        // This pins that the tag pair is present, not just the tree tag.
        using var recorder = new MetricRecorder();
        var treeId = $"metrics-saga-tags-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        var entries = new List<KeyValuePair<string, byte[]>>
        {
            new("a", Encoding.UTF8.GetBytes("1")),
        };
        await tree.SetManyAtomicAsync(entries);

        var anyPrepareWithBothTags = recorder.Records.Any(r =>
            r.Name == "orleans.lattice.saga.prepare.duration"
            && r.Tags.Any(t => t.Key == LatticeMetrics.TagTree && (t.Value as string) == treeId)
            && r.Tags.Any(t => t.Key == LatticeMetrics.TagWalPartitions && t.Value is int));
        Assert.That(anyPrepareWithBothTags, Is.True,
            "saga.prepare.duration must carry both the tree tag and the wal_partitions tag.");

        var anyBroadcastWithBothTags = recorder.Records.Any(r =>
            r.Name == "orleans.lattice.saga.broadcast.duration"
            && r.Tags.Any(t => t.Key == LatticeMetrics.TagTree && (t.Value as string) == treeId)
            && r.Tags.Any(t => t.Key == LatticeMetrics.TagWalPartitions && t.Value is int));
        Assert.That(anyBroadcastWithBothTags, Is.True,
            "saga.broadcast.duration must carry both the tree tag and the wal_partitions tag.");
    }
}
