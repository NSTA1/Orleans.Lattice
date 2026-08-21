using System.Globalization;
using System.Text;

namespace Orleans.Lattice.Tests.Crdt;

[TestFixture]
public class GCounterProvenanceDecoderTests
{
    private static GCounterProvenanceDecoder Decoder => GCounterProvenanceDecoder.Instance;

    private static GCounterDelta Delta(params (string Replica, long Value)[] increments)
    {
        var map = new Dictionary<string, long>();
        foreach (var (replica, value) in increments)
        {
            map[replica] = value;
        }

        return new GCounterDelta { Increments = map };
    }

    private static GCounter Counter(params (string Replica, long Amount)[] increments)
    {
        var counter = new GCounter();
        foreach (var (replica, amount) in increments)
        {
            counter.Increment(replica, amount);
        }

        return counter;
    }

    [Test]
    public void Mode_is_gcounter()
    {
        Assert.That(Decoder.Mode, Is.EqualTo(LatticeMergeMode.GCounter));
    }

    [Test]
    public void Instance_is_shared_singleton()
    {
        Assert.That(GCounterProvenanceDecoder.Instance, Is.SameAs(GCounterProvenanceDecoder.Instance));
    }

    [Test]
    public void DecodeDeltas_null_throws()
    {
        Assert.That(() => Decoder.DecodeDeltas(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void DecodeState_null_throws()
    {
        Assert.That(() => Decoder.DecodeState(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void DecodeCurrentValue_null_throws()
    {
        Assert.That(() => Decoder.DecodeCurrentValue(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void DecodeDeltas_empty_sequence_yields_no_events()
    {
        Assert.That(Decoder.DecodeDeltas(Array.Empty<CrdtProvenanceDelta>()), Is.Empty);
    }

    [Test]
    public void DecodeDeltas_all_non_positive_yields_no_events()
    {
        var deltas = new[] { new CrdtProvenanceDelta(Delta(("r1", 0), ("r2", -3))) };

        Assert.That(Decoder.DecodeDeltas(deltas), Is.Empty);
    }

    [Test]
    public void DecodeDeltas_emits_one_added_event_per_positive_replica()
    {
        var deltas = new[] { new CrdtProvenanceDelta(Delta(("r1", 5), ("r2", 2))) };

        var events = Decoder.DecodeDeltas(deltas);

        Assert.That(events, Has.Count.EqualTo(2));
        Assert.That(events.All(e => e.Kind == CrdtMemberChangeKind.Added), Is.True);
        var byReplica = events.ToDictionary(e => e.ReplicaId, e => e.Ordinal);
        Assert.Multiple(() =>
        {
            Assert.That(byReplica["r1"], Is.EqualTo(5L));
            Assert.That(byReplica["r2"], Is.EqualTo(2L));
            Assert.That(events[0].Element, Is.EqualTo(Encoding.UTF8.GetBytes(events[0].ReplicaId)));
        });
    }

    [Test]
    public void DecodeDeltas_skips_non_positive_replica_rows()
    {
        var deltas = new[] { new CrdtProvenanceDelta(Delta(("r1", 5), ("zero", 0), ("neg", -1))) };

        var events = Decoder.DecodeDeltas(deltas);

        Assert.That(events, Has.Count.EqualTo(1));
        Assert.That(events[0].ReplicaId, Is.EqualTo("r1"));
    }

    [Test]
    public void DecodeDeltas_propagates_wall_clock_stamp()
    {
        var stamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var deltas = new[] { new CrdtProvenanceDelta(Delta(("r1", 4)), stamp) };

        var events = Decoder.DecodeDeltas(deltas);

        Assert.That(events[0].WallClock, Is.EqualTo(stamp));
    }

    [Test]
    public void DecodeDeltas_preserves_delta_order_across_entries()
    {
        var deltas = new[]
        {
            new CrdtProvenanceDelta(Delta(("a", 1))),
            new CrdtProvenanceDelta(Delta(("b", 1))),
        };

        var events = Decoder.DecodeDeltas(deltas);

        Assert.That(events.Select(e => e.ReplicaId), Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public void DecodeState_empty_counter_yields_no_events()
    {
        Assert.That(Decoder.DecodeState(new GCounter()), Is.Empty);
    }

    [Test]
    public void DecodeState_yields_one_event_per_positive_replica()
    {
        var events = Decoder.DecodeState(Counter(("r1", 3), ("r2", 7)));

        Assert.That(events, Has.Count.EqualTo(2));
        var byReplica = events.ToDictionary(e => e.ReplicaId, e => e.Ordinal);
        Assert.Multiple(() =>
        {
            Assert.That(byReplica["r1"], Is.EqualTo(3L));
            Assert.That(byReplica["r2"], Is.EqualTo(7L));
            Assert.That(events.All(e => e.WallClock is null), Is.True);
            Assert.That(events.All(e => e.Kind == CrdtMemberChangeKind.Added), Is.True);
        });
    }

    [Test]
    public void DecodeCurrentValue_empty_counter_yields_no_members()
    {
        Assert.That(Decoder.DecodeCurrentValue(new GCounter()), Is.Empty);
    }

    [Test]
    public void DecodeCurrentValue_projects_single_total_member()
    {
        var members = Decoder.DecodeCurrentValue(Counter(("r1", 3), ("r2", 7)));

        Assert.That(members, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(members[0].Ordinal, Is.EqualTo(10L));
            Assert.That(members[0].ReplicaId, Is.EqualTo(string.Empty));
            Assert.That(members[0].Element, Is.EqualTo(Encoding.UTF8.GetBytes(10L.ToString(CultureInfo.InvariantCulture))));
        });
    }
}
