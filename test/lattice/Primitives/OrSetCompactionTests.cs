namespace Orleans.Lattice.Tests.Primitives;

/// <summary>
/// Convergence and bounded-state guards for dot-history compaction as applied to <see cref="OrSet"/>.
/// </summary>
[TestFixture]
public class OrSetCompactionTests
{
    private static readonly byte[] Element = "element"u8.ToArray();

    private static void AddAsAccessorWould(OrSet set, byte[] element, string replicaId)
    {
        var key = Key(element);
        var max = MaxCounter(set.Adds, key, replicaId);
        var tombMax = MaxCounter(set.Tombstones, key, replicaId);
        if (tombMax > max) max = tombMax;

        set.Add(element, replicaId, max + 1);
    }

    [Test]
    public void Repeated_add_from_one_replica_keeps_exactly_one_dot()
    {
        var set = new OrSet();
        for (var i = 0; i < 1000; i++) AddAsAccessorWould(set, Element, "local");

        var dots = set.Adds[Key(Element)];
        Assert.Multiple(() =>
        {
            Assert.That(dots, Has.Count.EqualTo(1),
                "A replica re-adding the same element holds one dot, not one per assertion.");
            Assert.That(dots[0].Counter, Is.EqualTo(1000),
                "The surviving dot is the newest, so a later remove tombstones the whole history.");
            Assert.That(set.Contains(Element), Is.True);
        });
    }

    [Test]
    public void State_stays_bounded_by_replica_count_not_assertion_count()
    {
        var set = new OrSet();
        for (var round = 0; round < 200; round++)
        {
            AddAsAccessorWould(set, Element, "A");
            AddAsAccessorWould(set, Element, "B");
            AddAsAccessorWould(set, Element, "C");
        }

        Assert.That(set.Adds[Key(Element)], Has.Count.EqualTo(3),
            "600 assertions across 3 replicas cost 3 dots: state is O(replicas), not O(assertions).");
    }

    [Test]
    public void Add_concurrent_with_remove_elsewhere_still_wins()
    {
        var remover = new OrSet();
        AddAsAccessorWould(remover, Element, "A");
        remover.Remove(Element);

        var adder = new OrSet();
        AddAsAccessorWould(adder, Element, "B");

        Assert.Multiple(() =>
        {
            Assert.That(OrSet.Merge(remover, adder).Contains(Element), Is.True,
                "An add concurrent with a remove it never observed wins: add-wins is preserved.");
            Assert.That(OrSet.Merge(adder, remover).Contains(Element), Is.True,
                "The add-wins tie-break is order-independent.");
        });
    }

    [Test]
    public void Remove_after_a_compacted_readd_still_removes_peer_old_dot()
    {
        var author = new OrSet();
        AddAsAccessorWould(author, Element, "A");
        var peerHoldingOldDot = author.Clone();

        AddAsAccessorWould(author, Element, "A");
        author.Remove(Element);

        var merged = peerHoldingOldDot.Clone();
        merged.MergeFrom(author);

        Assert.Multiple(() =>
        {
            Assert.That(author.Contains(Element), Is.False);
            Assert.That(merged.Contains(Element), Is.False,
                "The remove covers the peer's older dot from the same replica, so the element stays absent.");
        });
    }

    [Test]
    public void Merge_is_commutative_associative_and_idempotent()
    {
        var a = new OrSet();
        AddAsAccessorWould(a, Element, "A");
        AddAsAccessorWould(a, Element, "A");

        var b = new OrSet();
        AddAsAccessorWould(b, "other"u8.ToArray(), "B");
        b.Remove("other"u8.ToArray());

        var c = new OrSet();
        AddAsAccessorWould(c, Element, "C");

        var left = OrSet.Merge(OrSet.Merge(a, b), c);
        var right = OrSet.Merge(a, OrSet.Merge(b, c));
        var once = OrSet.Merge(a, b);
        var twice = OrSet.Merge(once, b);

        Assert.Multiple(() =>
        {
            Assert.That(left.Contains(Element), Is.EqualTo(right.Contains(Element)), "associative membership");
            Assert.That(OrSet.Merge(a, b).Contains(Element), Is.EqualTo(OrSet.Merge(b, a).Contains(Element)),
                "commutative membership");
            Assert.That(twice.Contains(Element), Is.EqualTo(once.Contains(Element)), "idempotent membership");
            Assert.That(twice.Adds[Key(Element)], Has.Count.EqualTo(once.Adds[Key(Element)].Count),
                "Re-merging the same state adds no dots.");
        });
    }

    [Test]
    public void Folding_a_replicated_delta_repeatedly_does_not_grow_state()
    {
        var set = new OrSet();
        var delta = new OrSetDelta
        {
            Adds = new[] { new OrSetDeltaDot { Element = Element, ReplicaId = "peer", Counter = 7 } },
            Removes = Array.Empty<OrSetDeltaDot>(),
        };

        for (var i = 0; i < 50; i++) set.MergeDelta(delta);

        Assert.Multiple(() =>
        {
            Assert.That(set.Adds[Key(Element)], Has.Count.EqualTo(1),
                "Duplicate delta delivery stays idempotent after compaction.");
            Assert.That(set.Contains(Element), Is.True);
        });
    }

    [Test]
    public void An_already_bloated_history_heals_on_the_next_merge()
    {
        var legacy = new OrSet();
        var key = Key(Element);
        legacy.Adds[key] = [];
        for (var i = 1; i <= 500; i++)
        {
            legacy.Adds[key].Add(new OrSetDot { ReplicaId = "local", Counter = i });
        }

        Assume.That(legacy.Adds[key], Has.Count.EqualTo(500), "arranged as a pre-fix row");

        legacy.MergeFrom(new OrSet());

        Assert.Multiple(() =>
        {
            Assert.That(legacy.Adds[key], Has.Count.EqualTo(1),
                "A merge normalises an inherited unbounded history without an operator step.");
            Assert.That(legacy.Adds[key][0].Counter, Is.EqualTo(500));
            Assert.That(legacy.Contains(Element), Is.True, "Healing never changes the observable value.");
        });
    }

    private static string Key(byte[] element) => Convert.ToBase64String(element);

    private static long MaxCounter(Dictionary<string, List<OrSetDot>> map, string key, string replicaId)
    {
        if (!map.TryGetValue(key, out var dots)) return 0;
        long max = 0;
        foreach (var dot in dots)
        {
            if (dot.ReplicaId == replicaId && dot.Counter > max) max = dot.Counter;
        }

        return max;
    }
}
