using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #2687. The shard-root batched-write timers
/// (<c>shard_root.set_many.local_apply.duration</c> and
/// <c>shard_root.set_many.leaf_rpc.duration</c>) are recorded by both the
/// unconditional <see cref="ILattice.SetManyAsync"/> path and the conditional
/// <see cref="ILattice.SetManyWherePredicateAsync"/> path. Untagged, a
/// dashboard comparing them against the <c>set_many.duration</c> envelope - which
/// only the unconditional path records - mixed the two populations and read a
/// shard-side cost that exceeded its own envelope. These pin that each path
/// stamps its own <c>operation</c> arm, and that the conditional path records an
/// envelope of its own to compare that arm against.
/// </summary>
/// <remarks>
/// Instruments are selected by their literal names rather than by field
/// reference so the fixture compiles, and fails on assertion, against a build
/// that does not declare the conditional envelope. Every measurement is filtered
/// to a tree id minted per test, so concurrent writes from other fixtures and
/// the order the tests run in cannot affect the result.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class SetManyOperationTagIntegrationTests
{
    private const string LocalApply = "orleans.lattice.shard_root.set_many.local_apply.duration";
    private const string LeafRpc = "orleans.lattice.shard_root.set_many.leaf_rpc.duration";
    private const string SetManyEnvelope = "orleans.lattice.set_many.duration";
    private const string SetManyWhereEnvelope = "orleans.lattice.set_many_where_predicate.duration";

    private ClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new ClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private sealed record Scored(int Score);

    private sealed record Observation(string Instrument, string? Tree, string? Operation);

    private static byte[] ScoredJson(int score) => Encoding.UTF8.GetBytes($"{{\"Score\":{score}}}");

    private static LatticePredicateNode ScoreAtLeast(int threshold) =>
        LatticePredicatePushdown.Compile<Scored>(
            s => s.Score >= threshold, JsonLatticeSerializer<Scored>.Default);

    private static List<KeyValuePair<string, byte[]>> Batch(int score) =>
    [
        new("a", ScoredJson(score)),
        new("m", ScoredJson(score)),
        new("z", ScoredJson(score)),
    ];

    private static MeterListener Listen(ConcurrentQueue<Observation> sink) =>
        MeterListening.StartForMeter(
            LatticeMetrics.Meter,
            [LocalApply, LeafRpc, SetManyEnvelope, SetManyWhereEnvelope],
            listener => listener.SetMeasurementEventCallback<double>((instrument, _, tags, _) =>
            {
                string? tree = null;
                string? operation = null;
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagTree) tree = tag.Value as string;
                    else if (tag.Key == LatticeMetrics.TagOperation) operation = tag.Value as string;
                }
                sink.Enqueue(new Observation(instrument.Name, tree, operation));
            }));

    private static string NewTreeId() => $"setmany-optag-{Guid.NewGuid():N}";

    private static List<Observation> For(ConcurrentQueue<Observation> sink, string treeId, string instrument) =>
        sink.Where(o => o.Tree == treeId && o.Instrument == instrument).ToList();

    [Test]
    public async Task SetManyAsync_stamps_operation_set_many_on_both_shard_root_timers()
    {
        var treeId = NewTreeId();
        var sink = new ConcurrentQueue<Observation>();
        using (Listen(sink))
        {
            await _cluster.GrainFactory.GetGrain<ILattice>(treeId).SetManyAsync(Batch(10));
        }

        foreach (var instrument in new[] { LocalApply, LeafRpc })
        {
            var observed = For(sink, treeId, instrument);
            Assert.That(observed, Is.Not.Empty, $"{instrument} must record for the unconditional batch");
            Assert.That(observed.Select(o => o.Operation), Is.All.EqualTo("set_many"),
                $"{instrument} must stamp operation=set_many on the unconditional path");
        }
    }

    [Test]
    public async Task SetManyWherePredicateAsync_stamps_operation_set_many_where_predicate_on_both_shard_root_timers()
    {
        var treeId = NewTreeId();
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await tree.SetManyAsync(Batch(1000));
        var sink = new ConcurrentQueue<Observation>();
        using (Listen(sink))
        {
            var written = await tree.SetManyWherePredicateAsync(Batch(2000), ScoreAtLeast(500));
            Assert.That(written, Has.Count.EqualTo(3), "precondition: the guard admits every entry");
        }

        foreach (var instrument in new[] { LocalApply, LeafRpc })
        {
            var observed = For(sink, treeId, instrument);
            Assert.That(observed, Is.Not.Empty, $"{instrument} must record for the conditional batch");
            Assert.That(observed.Select(o => o.Operation), Is.All.EqualTo("set_many_where_predicate"),
                $"{instrument} must stamp operation=set_many_where_predicate on the conditional path");
        }
    }

    [Test]
    public async Task SetManyWherePredicateAsync_records_its_own_envelope_and_not_the_set_many_envelope()
    {
        var treeId = NewTreeId();
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await tree.SetManyAsync(Batch(1000));
        var sink = new ConcurrentQueue<Observation>();
        using (Listen(sink))
        {
            await tree.SetManyWherePredicateAsync(Batch(2000), ScoreAtLeast(500));
        }

        Assert.That(For(sink, treeId, SetManyWhereEnvelope), Has.Count.EqualTo(1),
            "one conditional batched write must record exactly one caller-visible envelope observation");
        Assert.That(For(sink, treeId, SetManyEnvelope), Is.Empty,
            "the conditional path must not record the unconditional set_many envelope");
    }

    [Test]
    public async Task SetManyWherePredicateAsync_records_the_envelope_when_the_guard_rejects_every_entry()
    {
        var treeId = NewTreeId();
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await tree.SetManyAsync(Batch(10));
        var sink = new ConcurrentQueue<Observation>();
        using (Listen(sink))
        {
            var written = await tree.SetManyWherePredicateAsync(Batch(2000), ScoreAtLeast(500));
            Assert.That(written, Is.Empty, "precondition: the guard rejects every entry");
        }

        Assert.That(For(sink, treeId, SetManyWhereEnvelope), Has.Count.EqualTo(1));
    }

    [Test]
    public async Task SetManyAsync_does_not_record_the_conditional_envelope()
    {
        var treeId = NewTreeId();
        var sink = new ConcurrentQueue<Observation>();
        using (Listen(sink))
        {
            await _cluster.GrainFactory.GetGrain<ILattice>(treeId).SetManyAsync(Batch(10));
        }

        Assert.That(For(sink, treeId, SetManyEnvelope), Has.Count.EqualTo(1));
        Assert.That(For(sink, treeId, SetManyWhereEnvelope), Is.Empty);
    }
}
