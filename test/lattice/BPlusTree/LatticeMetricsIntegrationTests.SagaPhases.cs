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

    [Test]
    public async Task SetManyAtomicAsync_emits_saga_checkpoint_duration_histogram_with_phase_tag()
    {
        // Each state.WriteStateAsync call inside AtomicWriteGrain is
        // wrapped by WriteSagaStateAsync, which records on
        // SagaCheckpointDuration with a per-call phase tag identifying
        // the call site. A successful saga always hits at least the
        // "prepare" and "complete" phases; pin both so a future change
        // that drops the wrapper at one site is detected.
        using var recorder = new MetricRecorder();
        var treeId = $"metrics-saga-checkpoint-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        var entries = new List<KeyValuePair<string, byte[]>>
        {
            new("a", Encoding.UTF8.GetBytes("1")),
            new("b", Encoding.UTF8.GetBytes("2")),
        };
        await tree.SetManyAtomicAsync(entries);

        var preparePhaseObserved = recorder.Records.Any(r =>
            r.Name == "orleans.lattice.saga.checkpoint.duration"
            && r.Tags.Any(t => t.Key == LatticeMetrics.TagTree && (t.Value as string) == treeId)
            && r.Tags.Any(t => t.Key == LatticeMetrics.TagPhase && (t.Value as string) == "prepare"));
        Assert.That(preparePhaseObserved, Is.True,
            "Expected saga.checkpoint.duration observation with phase=prepare for the saga's initial state persist.");

        var completePhaseObserved = recorder.Records.Any(r =>
            r.Name == "orleans.lattice.saga.checkpoint.duration"
            && r.Tags.Any(t => t.Key == LatticeMetrics.TagTree && (t.Value as string) == treeId)
            && r.Tags.Any(t => t.Key == LatticeMetrics.TagPhase && (t.Value as string) == "complete"));
        Assert.That(completePhaseObserved, Is.True,
            "Expected saga.checkpoint.duration observation with phase=complete for the saga's terminal state persist.");
    }

    [Test]
    public async Task SetManyAtomicAsync_checkpoint_histogram_carries_wal_partitions_tag()
    {
        // Mirror of the prepare/broadcast tag-presence check for the
        // new checkpoint histogram so dashboards can join across
        // checkpoint observations and the (tree, walPartitions)
        // dimension without instrument-level joins.
        using var recorder = new MetricRecorder();
        var treeId = $"metrics-saga-checkpoint-tags-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        var entries = new List<KeyValuePair<string, byte[]>>
        {
            new("a", Encoding.UTF8.GetBytes("1")),
        };
        await tree.SetManyAtomicAsync(entries);

        var anyWithAllThreeTags = recorder.Records.Any(r =>
            r.Name == "orleans.lattice.saga.checkpoint.duration"
            && r.Tags.Any(t => t.Key == LatticeMetrics.TagTree && (t.Value as string) == treeId)
            && r.Tags.Any(t => t.Key == LatticeMetrics.TagWalPartitions && t.Value is int)
            && r.Tags.Any(t => t.Key == LatticeMetrics.TagPhase && t.Value is string));
        Assert.That(anyWithAllThreeTags, Is.True,
            "saga.checkpoint.duration must carry the tree tag, the wal_partitions tag, and a string phase tag.");
    }

    [Test]
    public async Task SetManyAtomicAsync_emits_saga_reminder_duration_histogram_for_register_and_unregister()
    {
        // RegisterKeepaliveAsync runs at saga entry; UnregisterKeepaliveAsync
        // runs inside CompleteSagaAsync. Both are Azure-Tables-shaped
        // RPCs against the Orleans reminder table - per the c2-xvii
        // routing memo, the plausible binding constraint at the
        // c2-iii operating point. Pin that BOTH lifecycle calls emit
        // observations with their distinct phase tags so a future
        // change cannot silently drop either.
        using var recorder = new MetricRecorder();
        var treeId = $"metrics-saga-reminder-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        var entries = new List<KeyValuePair<string, byte[]>>
        {
            new("a", Encoding.UTF8.GetBytes("1")),
            new("b", Encoding.UTF8.GetBytes("2")),
        };
        await tree.SetManyAtomicAsync(entries);

        var registerObserved = recorder.Records.Any(r =>
            r.Name == "orleans.lattice.saga.reminder.duration"
            && r.Tags.Any(t => t.Key == LatticeMetrics.TagTree && (t.Value as string) == treeId)
            && r.Tags.Any(t => t.Key == LatticeMetrics.TagPhase && (t.Value as string) == "register"));
        Assert.That(registerObserved, Is.True,
            "Expected saga.reminder.duration observation with phase=register for the saga's RegisterKeepaliveAsync call.");

        var unregisterGetObserved = recorder.Records.Any(r =>
            r.Name == "orleans.lattice.saga.reminder.duration"
            && r.Tags.Any(t => t.Key == LatticeMetrics.TagTree && (t.Value as string) == treeId)
            && r.Tags.Any(t => t.Key == LatticeMetrics.TagPhase && (t.Value as string) == "unregister-get"));
        Assert.That(unregisterGetObserved, Is.True,
            "Expected saga.reminder.duration observation with phase=unregister-get for the saga's UnregisterKeepaliveAsync GetReminder call.");
    }
}
