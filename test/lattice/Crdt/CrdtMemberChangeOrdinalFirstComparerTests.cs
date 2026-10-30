namespace Orleans.Lattice.Tests.Crdt;

[TestFixture]
public class CrdtMemberChangeOrdinalFirstComparerTests
{
    [TestCase(long.MinValue, "z", CrdtMemberChangeKind.Removed, long.MaxValue, "a", CrdtMemberChangeKind.Added, -1)]
    [TestCase(1, "z", CrdtMemberChangeKind.Removed, 2, "a", CrdtMemberChangeKind.Added, -1)]
    [TestCase(1, "a", CrdtMemberChangeKind.Removed, 1, "z", CrdtMemberChangeKind.Added, -1)]
    [TestCase(1, "a", CrdtMemberChangeKind.Added, 1, "a", CrdtMemberChangeKind.Removed, -1)]
    [TestCase(1, "a", CrdtMemberChangeKind.Added, 1, "a", CrdtMemberChangeKind.Added, 0)]
    public void Compare_orders_by_ordinal_then_replica_then_kind(
        long leftOrdinal, string leftReplica, CrdtMemberChangeKind leftKind,
        long rightOrdinal, string rightReplica, CrdtMemberChangeKind rightKind, int expected)
    {
        var left = Change(leftOrdinal, leftReplica, leftKind);
        var right = Change(rightOrdinal, rightReplica, rightKind);

        Assert.Multiple(() =>
        {
            Assert.That(Math.Sign(CrdtMemberChangeOrdinalFirstComparer.Instance.Compare(left, right)), Is.EqualTo(expected));
            Assert.That(Math.Sign(CrdtMemberChangeOrdinalFirstComparer.Comparison(right, left)), Is.EqualTo(-expected));
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public void DecodeState_set_events_keep_ordinal_first_order(bool removeWins)
    {
        var element = "x"u8.ToArray();
        var key = Convert.ToBase64String(element);
        var adds = new List<OrSetDot>
        {
            new() { ReplicaId = "a", Counter = 2 },
            new() { ReplicaId = "z", Counter = 1 },
            new() { ReplicaId = "a", Counter = 1 },
        };
        var removes = new List<OrSetDot> { new() { ReplicaId = "a", Counter = 1 } };
        IReadOnlyList<CrdtMemberChange> events;
        if (removeWins)
        {
            var set = new RwSet();
            set.Adds[key] = adds;
            set.Removes[key] = removes;
            events = RwSetProvenanceDecoder.Instance.DecodeState(set);
        }
        else
        {
            var set = new OrSet();
            set.Adds[key] = adds;
            set.Tombstones[key] = removes;
            events = OrSetProvenanceDecoder.Instance.DecodeState(set);
        }

        Assert.That(events.Select(e => (e.Ordinal, e.ReplicaId, e.Kind)), Is.EqualTo(new[]
        {
            (1L, "a", CrdtMemberChangeKind.Added),
            (1L, "a", CrdtMemberChangeKind.Removed),
            (1L, "z", CrdtMemberChangeKind.Added),
            (2L, "a", CrdtMemberChangeKind.Added),
        }));
    }

    [Test]
    public void Comparison_repeated_in_place_sorts_allocate_nothing()
    {
        var changes = new[]
        {
            Change(2, "a", CrdtMemberChangeKind.Added),
            Change(1, "z", CrdtMemberChangeKind.Removed),
        };
        var comparison = CrdtMemberChangeOrdinalFirstComparer.Comparison;
        for (var i = 0; i < 100; i++) changes.AsSpan().Sort(comparison);

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 100; i++) changes.AsSpan().Sort(comparison);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.That(allocated, Is.Zero);
    }

    private static CrdtMemberChange Change(long ordinal, string replica, CrdtMemberChangeKind kind) =>
        new() { Element = Array.Empty<byte>(), Ordinal = ordinal, ReplicaId = replica, Kind = kind };
}
