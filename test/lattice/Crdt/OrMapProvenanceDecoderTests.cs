using System.Text;

namespace Orleans.Lattice.Tests.Crdt;

[TestFixture]
public class OrMapProvenanceDecoderTests
{
    private static OrMapProvenanceDecoder Decoder => OrMapProvenanceDecoder.Instance;

    private static OrMapDeltaEntry<string, OrFlag> Add(string key, string replica, long counter) =>
        new() { Key = key, ReplicaId = replica, Counter = counter, Value = new OrFlag() };

    private static OrMapDeltaTombstone<string> Tomb(string key, string replica, long counter) =>
        new() { Key = key, ReplicaId = replica, Counter = counter };

    private static OrMapDelta<string, OrFlag> Delta(
        OrMapDeltaEntry<string, OrFlag>[]? adds = null,
        OrMapDeltaTombstone<string>[]? tombstones = null) => new()
    {
        Adds = adds ?? Array.Empty<OrMapDeltaEntry<string, OrFlag>>(),
        Tombstones = tombstones ?? Array.Empty<OrMapDeltaTombstone<string>>(),
    };

    private static string KeyOf(CrdtMemberChange change) => Encoding.UTF8.GetString(change.Element);

    [Test]
    public void Mode_is_ormap()
    {
        Assert.That(Decoder.Mode, Is.EqualTo(LatticeMergeMode.OrMap));
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
    public void DecodeDeltas_empty_sequence_yields_no_events()
    {
        Assert.That(Decoder.DecodeDeltas(Array.Empty<CrdtProvenanceDelta>()), Is.Empty);
    }

    [Test]
    public void DecodeState_empty_map_yields_no_events()
    {
        Assert.That(Decoder.DecodeState(new OrMap<string, OrFlag>()), Is.Empty);
    }

    [Test]
    public void DecodeDeltas_add_yields_added_with_key_bytes_and_dot()
    {
        var deltas = new[] { new CrdtProvenanceDelta(Delta(adds: new[] { Add("k1", "r1", 3) })) };

        var events = Decoder.DecodeDeltas(deltas);

        Assert.That(events, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(events[0].Kind, Is.EqualTo(CrdtMemberChangeKind.Added));
            Assert.That(KeyOf(events[0]), Is.EqualTo("k1"));
            Assert.That(events[0].ReplicaId, Is.EqualTo("r1"));
            Assert.That(events[0].Ordinal, Is.EqualTo(3L));
        });
    }

    [Test]
    public void DecodeDeltas_tombstone_yields_removed()
    {
        var deltas = new[] { new CrdtProvenanceDelta(Delta(tombstones: new[] { Tomb("k1", "r1", 3) })) };

        var events = Decoder.DecodeDeltas(deltas);

        Assert.That(events, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(events[0].Kind, Is.EqualTo(CrdtMemberChangeKind.Removed));
            Assert.That(KeyOf(events[0]), Is.EqualTo("k1"));
        });
    }

    [Test]
    public void DecodeDeltas_preserves_remove_then_readd_order()
    {
        var deltas = new[]
        {
            new CrdtProvenanceDelta(Delta(adds: new[] { Add("k", "r1", 1) })),
            new CrdtProvenanceDelta(Delta(tombstones: new[] { Tomb("k", "r1", 1) })),
            new CrdtProvenanceDelta(Delta(adds: new[] { Add("k", "r1", 2) })),
        };

        var events = Decoder.DecodeDeltas(deltas);

        Assert.That(events.Select(e => e.Kind), Is.EqualTo(new[]
        {
            CrdtMemberChangeKind.Added,
            CrdtMemberChangeKind.Removed,
            CrdtMemberChangeKind.Added,
        }));
    }

    [Test]
    public void DecodeState_concurrent_adds_for_same_key_both_survive()
    {
        var a = new OrMap<string, OrFlag>();
        a.Set("k", "r1", new OrFlag());
        var b = new OrMap<string, OrFlag>();
        b.Set("k", "r2", new OrFlag());
        var merged = OrMap<string, OrFlag>.Merge(a, b);

        var events = Decoder.DecodeState(merged);

        Assert.That(events, Has.Count.EqualTo(2));
        Assert.That(events.All(e => e.Kind == CrdtMemberChangeKind.Added && KeyOf(e) == "k"), Is.True);
        Assert.That(events.Select(e => e.ReplicaId), Is.EquivalentTo(new[] { "r1", "r2" }));
    }

    [Test]
    public void DecodeState_remove_then_readd_shows_both_events()
    {
        var map = new OrMap<string, OrFlag>();
        map.Set("k", "r1", new OrFlag());   // dot (r1, 1)
        map.Remove("k");                     // tombstones (r1, 1)
        map.Set("k", "r1", new OrFlag());   // fresh dot (r1, 2)

        var events = Decoder.DecodeState(map);

        // The tombstoned add stays in the add set, so a re-add surfaces both
        // the original add, its removal, and the new live add - all under "k".
        Assert.That(events.All(e => KeyOf(e) == "k"), Is.True);
        var adds = events.Where(e => e.Kind == CrdtMemberChangeKind.Added).Select(e => e.Ordinal);
        var removes = events.Where(e => e.Kind == CrdtMemberChangeKind.Removed).Select(e => e.Ordinal);
        Assert.Multiple(() =>
        {
            Assert.That(adds, Is.EquivalentTo(new[] { 1L, 2L }));
            Assert.That(removes, Is.EquivalentTo(new[] { 1L }));
        });
    }

    [Test]
    public void DecodeState_groups_by_key_deterministically()
    {
        var map = new OrMap<string, OrFlag>();
        map.Set("kB", "r1", new OrFlag());
        map.Set("kA", "r1", new OrFlag());

        var first = Decoder.DecodeState(map);
        var second = Decoder.DecodeState(map);

        Assert.That(first.Select(KeyOf), Is.EqualTo(second.Select(KeyOf)));
        Assert.That(first.Select(KeyOf), Is.EqualTo(new[] { "kA", "kB" }));
    }

    [Test]
    public void DecodeState_wall_clock_is_always_null()
    {
        var map = new OrMap<string, OrFlag>();
        map.Set("k", "r1", new OrFlag());

        Assert.That(Decoder.DecodeState(map).All(e => e.WallClock is null), Is.True);
    }
}
