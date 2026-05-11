using System.Diagnostics.Metrics;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Telemetry-shape tests for the cross-migration LWW backstop branch
/// on <see cref="IBPlusLeafGrain.ApplyTxTerminalAsync"/>. The backstop
/// emits on <see cref="LatticeMetrics.LeafWriteDuration"/> with a
/// <see cref="LatticeMetrics.TagKind"/> = <c>"backstop"</c> tag so
/// operators can size cross-migration backstop traffic against
/// ordinary writes on the same instrument.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task ApplyTxTerminalAsync_backstop_emits_LeafWriteDuration_with_kind_backstop_tag()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var commitLog = new FakeCommitLogWriter();
        var grain = CreateGrain(state, commitLog: commitLog);
        var txid = Guid.NewGuid();
        var committed = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            ["k1"] = [1],
            ["k2"] = [2],
        };

        using var recorder = new BackstopMetricRecorder();
        await grain.ApplyTxTerminalAsync(txid, committed: true, committed);

        var backstopEmissions = recorder.BackstopEmissions();
        Assert.That(backstopEmissions.Count, Is.EqualTo(2),
            "backstop must emit on LeafWriteDuration exactly once per missing-key WAL append "
            + $"(expected 2 for [k1, k2]; observed {backstopEmissions.Count})");

        // Every backstop emission must carry the leaf's tree id on the
        // `tree` tag so per-tree dashboards aggregate correctly.
        foreach (var emission in backstopEmissions)
        {
            var treeTag = emission.Tags.SingleOrDefault(t => t.Key == LatticeMetrics.TagTree);
            Assert.That(treeTag.Value, Is.EqualTo("test-tree"),
                "every backstop emission must carry tree=test-tree (the seeded leaf tree id)");
        }
    }

    [Test]
    public async Task ApplyTxTerminalAsync_backstop_emits_no_kind_tag_when_path_is_idempotent_replay()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var commitLog = new FakeCommitLogWriter();
        var grain = CreateGrain(state, commitLog: commitLog);
        var txid = Guid.NewGuid();
        var committed = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            ["k"] = [1],
        };

        // First delivery: emits on the histogram with kind=backstop.
        await grain.ApplyTxTerminalAsync(txid, committed: true, committed);

        using var recorder = new BackstopMetricRecorder();

        // Idempotent replay: the per-(txid, key) dedup short-circuits
        // before the WAL append, so the histogram must observe zero
        // additional backstop emissions on this delivery.
        await grain.ApplyTxTerminalAsync(txid, committed: true, committed);

        Assert.That(recorder.BackstopEmissions(), Is.Empty,
            "idempotent replay must short-circuit before the WAL append; "
            + "no LeafWriteDuration emission with kind=backstop must fire");
    }

    /// <summary>
    /// Captures every measurement on <see cref="LatticeMetrics.Meter"/>
    /// for the lifetime of the recorder. Filtering to backstop emissions
    /// on the <see cref="LatticeMetrics.LeafWriteDuration"/> instrument
    /// is done at read time so parallel test interference (e.g. an
    /// ordinary <c>SetAsync</c> emission on the same histogram with no
    /// <see cref="LatticeMetrics.TagKind"/> tag) does not pollute the
    /// assertion.
    /// </summary>
    private sealed class BackstopMetricRecorder : IDisposable
    {
        private readonly MeterListener _listener;
        private readonly List<(string Name, double Value, KeyValuePair<string, object?>[] Tags)> _records = new();
        private readonly object _lock = new();

        public BackstopMetricRecorder()
        {
            _listener = new MeterListener
            {
                InstrumentPublished = (inst, l) =>
                {
                    if (ReferenceEquals(inst.Meter, LatticeMetrics.Meter))
                        l.EnableMeasurementEvents(inst);
                },
            };
            _listener.SetMeasurementEventCallback<double>(OnDouble);
            _listener.Start();
        }

        private void OnDouble(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
        {
            lock (_lock)
            {
                _records.Add((instrument.Name, value, tags.ToArray()));
            }
        }

        public IReadOnlyList<(string Name, double Value, KeyValuePair<string, object?>[] Tags)> BackstopEmissions()
        {
            lock (_lock)
            {
                var result = new List<(string, double, KeyValuePair<string, object?>[])>();
                foreach (var r in _records)
                {
                    if (r.Name != LatticeMetrics.LeafWriteDuration.Name) continue;
                    var kindTag = r.Tags.SingleOrDefault(t => t.Key == LatticeMetrics.TagKind);
                    if (kindTag.Value is "backstop")
                        result.Add(r);
                }
                return result;
            }
        }

        public void Dispose() => _listener.Dispose();
    }
}
