using System.Diagnostics.Metrics;
using Orleans.Lattice.BPlusTree.Grains;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the <see cref="LatticeMetrics.LeafCommitInFlight"/>
/// histogram (instrument id <c>orleans.lattice.leaf.commit.in_flight</c>).
/// The histogram snapshots the in-flight commit count on the leaf at
/// the moment a foreground <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.SetAsync(string, byte[])"/>
/// / <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.SetManyAsync"/> enters the commit
/// path. Under the shipping Orleans single-threaded grain scheduling
/// (neither commit entry point is marked <c>[AlwaysInterleave]</c>)
/// the snapshot is always <c>0</c>; the tests pin that invariant so
/// the U9m benchmark probe retains a stable
/// falsifiability baseline. A future change that introduces commit
/// reentrancy on the leaf is expected to surface here as a non-zero
/// measurement.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task Set_records_leaf_commit_in_flight_once_at_zero()
    {
        var grain = CreateGrain();
        using var recorder = new CommitInFlightRecorder();

        await grain.SetAsync("k0", Encoding.UTF8.GetBytes("v0"));

        Assert.Multiple(() =>
        {
            Assert.That(recorder.Count, Is.EqualTo(1),
                "exactly one leaf.commit.in_flight measurement must be recorded per foreground SetAsync");
            Assert.That(recorder.Max, Is.EqualTo(0),
                "the recorded in-flight depth must be 0 on a non-reentrant leaf grain");
        });
    }

    [Test]
    public async Task SetMany_records_leaf_commit_in_flight_once_at_zero()
    {
        var grain = CreateGrain();
        using var recorder = new CommitInFlightRecorder();

        await grain.SetManyAsync(new List<KeyValuePair<string, byte[]>>
        {
            new("a", Encoding.UTF8.GetBytes("1")),
            new("b", Encoding.UTF8.GetBytes("2")),
            new("c", Encoding.UTF8.GetBytes("3")),
        });

        Assert.Multiple(() =>
        {
            Assert.That(recorder.Count, Is.EqualTo(1),
                "the batched commit path records exactly one leaf.commit.in_flight measurement per call, "
                + "not one per key");
            Assert.That(recorder.Max, Is.EqualTo(0),
                "the recorded in-flight depth must be 0 on the batched path for the same scheduling reason");
        });
    }

    [Test]
    public async Task Sequential_sets_each_record_zero_in_flight()
    {
        var grain = CreateGrain();
        using var recorder = new CommitInFlightRecorder();

        await grain.SetAsync("k0", Encoding.UTF8.GetBytes("v0"));
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await grain.SetAsync("k2", Encoding.UTF8.GetBytes("v2"));

        Assert.Multiple(() =>
        {
            Assert.That(recorder.Count, Is.EqualTo(3),
                "each SetAsync records its own leaf.commit.in_flight measurement");
            Assert.That(recorder.Max, Is.EqualTo(0),
                "sequential awaited commits must each see depth 0 because the prior commit has already "
                + "decremented the counter via its scope's Dispose by the time the next commit enters");
        });
    }

    [Test]
    public async Task Recorded_measurement_carries_tree_tag()
    {
        var state = new Fakes.FakePersistentState<Orleans.Lattice.BPlusTree.State.LeafNodeState>();
        state.State.TreeId = "tree-under-test";
        var grain = CreateGrain(state);
        using var recorder = new CommitInFlightRecorder();

        await grain.SetAsync("k0", Encoding.UTF8.GetBytes("v0"));

        Assert.That(recorder.LastTreeTag, Is.EqualTo("tree-under-test"),
            "the leaf.commit.in_flight measurement must carry the leaf's TreeId on the tree tag so the "
            + "PhaseA reporter can attribute concurrency per tree");
    }

    /// <summary>
    /// Captures <see cref="LatticeMetrics.LeafCommitInFlight"/>
    /// measurements for the lifetime of the recorder. The histogram is
    /// an <see cref="Histogram{Int32}"/>, so the integer callback is
    /// the only one we need to subscribe.
    /// </summary>
    private sealed class CommitInFlightRecorder : IDisposable
    {
        // Use the literal instrument id rather than
        // LatticeMetrics.LeafCommitInFlight.Name so this filter is safe
        // to evaluate from InstrumentPublished callbacks that fire
        // during the LatticeMetrics static constructor itself - the
        // listener can be invoked re-entrantly while other instruments
        // on the same meter are still being initialised, and a static
        // field reference would NRE before its assignment runs.
        private const string InstrumentName = "orleans.lattice.leaf.commit.in_flight";

        private readonly MeterListener _listener;
        private readonly object _lock = new();
        private int _count;
        private int _max;
        private string? _lastTreeTag;

        public int Count { get { lock (_lock) return _count; } }
        public int Max { get { lock (_lock) return _max; } }
        public string? LastTreeTag { get { lock (_lock) return _lastTreeTag; } }

        public CommitInFlightRecorder()
        {
            _listener = new MeterListener
            {
                InstrumentPublished = (inst, l) =>
                {
                    if (ReferenceEquals(inst.Meter, LatticeMetrics.Meter)
                        && inst.Name == InstrumentName)
                    {
                        l.EnableMeasurementEvents(inst);
                    }
                },
            };
            _listener.SetMeasurementEventCallback<int>(OnInt);
            _listener.Start();
        }

        private void OnInt(Instrument instrument, int value, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
        {
            string? tree = null;
            for (var i = 0; i < tags.Length; i++)
            {
                if (tags[i].Key == LatticeMetrics.TagTree)
                {
                    tree = tags[i].Value?.ToString();
                    break;
                }
            }
            lock (_lock)
            {
                _count++;
                if (value > _max) _max = value;
                _lastTreeTag = tree;
            }
        }

        public void Dispose() => _listener.Dispose();
    }
}
