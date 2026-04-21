using System.Diagnostics.Metrics;
using System.Text;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Integration tests for the follow-up instruments added to
/// <see cref="LatticeMetrics"/>: atomic-write outcome, coordinator
/// completion, tree lifecycle, and event publisher health.
/// </summary>
[TestFixture]
public class LatticeMetricsCoordinatorIntegrationTests
{
    private FourShardClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FourShardClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    /// <summary>Captures every measurement on the Lattice meter by reference.</summary>
    private sealed class Recorder : IDisposable
    {
        private readonly MeterListener _listener;
        private readonly List<(string Name, long Value, KeyValuePair<string, object?>[] Tags)> _records = new();
        private readonly object _gate = new();

        public Recorder()
        {
            _listener = new MeterListener
            {
                InstrumentPublished = (inst, l) =>
                {
                    if (ReferenceEquals(inst.Meter, LatticeMetrics.Meter))
                        l.EnableMeasurementEvents(inst);
                }
            };
            _listener.SetMeasurementEventCallback<long>((inst, v, tags, _) =>
            {
                lock (_gate) _records.Add((inst.Name, v, tags.ToArray()));
            });
            _listener.Start();
        }

        public long SumWhereTag(string name, string tagKey, string tagValue, string treeId)
        {
            lock (_gate)
            {
                long total = 0;
                foreach (var r in _records)
                {
                    if (r.Name != name) continue;
                    if (!r.Tags.Any(t => t.Key == LatticeMetrics.TagTree && (t.Value as string) == treeId)) continue;
                    if (!r.Tags.Any(t => t.Key == tagKey && (t.Value as string) == tagValue)) continue;
                    total += r.Value;
                }
                return total;
            }
        }

        public long SumForTree(string name, string treeId)
        {
            lock (_gate)
            {
                long total = 0;
                foreach (var r in _records)
                {
                    if (r.Name != name) continue;
                    if (!r.Tags.Any(t => t.Key == LatticeMetrics.TagTree && (t.Value as string) == treeId)) continue;
                    total += r.Value;
                }
                return total;
            }
        }

        public void Dispose() => _listener.Dispose();
    }

    [Test]
    public async Task SetManyAtomicAsync_emits_atomic_write_committed_outcome()
    {
        using var recorder = new Recorder();
        var treeId = $"metrics-atomic-ok-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        await tree.SetManyAtomicAsync(new List<KeyValuePair<string, byte[]>>
        {
            new("a", Encoding.UTF8.GetBytes("1")),
            new("b", Encoding.UTF8.GetBytes("2")),
        });

        // The saga grain deactivates asynchronously after CompleteSagaAsync;
        // the measurement has already fired synchronously before the grain
        // call returns, so no additional wait is required.
        var committed = recorder.SumWhereTag(
            "orleans.lattice.atomic_write.completed",
            LatticeMetrics.TagOutcome, "committed", treeId);
        Assert.That(committed, Is.EqualTo(1));
    }

    [Test]
    public async Task DeleteTreeAsync_emits_tree_lifecycle_deleted_counter()
    {
        using var recorder = new Recorder();
        var treeId = $"metrics-delete-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        await tree.SetAsync("k", Encoding.UTF8.GetBytes("v"));

        await tree.DeleteTreeAsync();

        var deleted = recorder.SumWhereTag(
            "orleans.lattice.tree.lifecycle",
            LatticeMetrics.TagKind, "deleted", treeId);
        Assert.That(deleted, Is.EqualTo(1));
    }

    [Test]
    public async Task DeleteTreeAsync_then_RecoverAsync_emits_both_lifecycle_counters()
    {
        using var recorder = new Recorder();
        var treeId = $"metrics-delete-recover-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        await tree.SetAsync("k", Encoding.UTF8.GetBytes("v"));

        await tree.DeleteTreeAsync();
        await tree.RecoverTreeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(recorder.SumWhereTag("orleans.lattice.tree.lifecycle",
                LatticeMetrics.TagKind, "deleted", treeId), Is.EqualTo(1));
            Assert.That(recorder.SumWhereTag("orleans.lattice.tree.lifecycle",
                LatticeMetrics.TagKind, "recovered", treeId), Is.EqualTo(1));
        });
    }

    [Test]
    public async Task Write_path_does_not_emit_events_dropped_when_publishing_disabled()
    {
        // The fixture does not register a stream provider and leaves
        // PublishEvents at its default (false), so SetAsync should never
        // invoke the publisher and never emit events.dropped.
        using var recorder = new Recorder();
        var treeId = $"metrics-events-off-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        await tree.SetAsync("k", Encoding.UTF8.GetBytes("v"));

        Assert.Multiple(() =>
        {
            Assert.That(recorder.SumForTree("orleans.lattice.events.published", treeId), Is.EqualTo(0));
            Assert.That(recorder.SumForTree("orleans.lattice.events.dropped", treeId), Is.EqualTo(0));
        });
    }

    [Test]
    public async Task SetPublishEventsEnabledAsync_emits_config_changed_counter()
    {
        using var recorder = new Recorder();
        var treeId = $"metrics-config-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        await tree.SetPublishEventsEnabledAsync(true);
        await tree.SetPublishEventsEnabledAsync(false);
        await tree.SetPublishEventsEnabledAsync(null);

        var changes = recorder.SumWhereTag(
            "orleans.lattice.config.changed",
            LatticeMetrics.TagConfig, "publish_events", treeId);
        Assert.That(changes, Is.EqualTo(3));
    }
}