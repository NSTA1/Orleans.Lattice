using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// The in-flight / completed reservation split that issue #4465 added, so a
/// duplicate of a delivery that has not completed is distinguishable from a
/// genuine re-delivery.
/// </summary>
[TestFixture]
public class RecentApplyCacheReservationTests
{
    private static WalRecord Entry(string key, long ticks = 10) => new()
    {
        TreeId = "tree",
        Op = MutationKind.Set,
        Key = key,
        Value = new byte[] { 1 },
        Timestamp = new() { WallClockTicks = ticks },
        OriginClusterId = "site-b",
    };

    [Test]
    public void TryAdd_records_a_new_tuple_as_an_in_flight_reservation()
    {
        var cache = new RecentApplyCache(8);

        var added = cache.TryAdd(Entry("k"), out var duplicateInFlight);

        Assert.Multiple(() =>
        {
            Assert.That(added, Is.True);
            Assert.That(duplicateInFlight, Is.False);
            Assert.That(cache.IsInFlight(Entry("k")), Is.True);
        });
    }

    [Test]
    public void TryAdd_reports_a_duplicate_of_an_in_flight_reservation()
    {
        var cache = new RecentApplyCache(8);
        cache.TryAdd(Entry("k"));

        var added = cache.TryAdd(Entry("k"), out var duplicateInFlight);

        Assert.Multiple(() =>
        {
            Assert.That(added, Is.False);
            Assert.That(duplicateInFlight, Is.True);
        });
    }

    [Test]
    public void Complete_turns_a_later_duplicate_into_a_genuine_redelivery()
    {
        var cache = new RecentApplyCache(8);
        cache.TryAdd(Entry("k"));

        cache.Complete(Entry("k"));
        var added = cache.TryAdd(Entry("k"), out var duplicateInFlight);

        Assert.Multiple(() =>
        {
            Assert.That(added, Is.False);
            Assert.That(duplicateInFlight, Is.False);
            Assert.That(cache.IsInFlight(Entry("k")), Is.False);
            Assert.That(cache.Contains(Entry("k")), Is.True);
        });
    }

    [Test]
    public void Complete_is_a_no_op_for_an_absent_tuple()
    {
        var cache = new RecentApplyCache(8);

        cache.Complete(Entry("k"));

        Assert.Multiple(() =>
        {
            Assert.That(cache.Count, Is.Zero);
            Assert.That(cache.IsInFlight(Entry("k")), Is.False);
        });
    }

    [Test]
    public void Remove_rolls_back_an_in_flight_reservation_so_a_redelivery_is_new()
    {
        var cache = new RecentApplyCache(8);
        cache.TryAdd(Entry("k"));

        cache.Remove(Entry("k"));
        var added = cache.TryAdd(Entry("k"), out var duplicateInFlight);

        Assert.Multiple(() =>
        {
            Assert.That(added, Is.True);
            Assert.That(duplicateInFlight, Is.False);
        });
    }

    [Test]
    public void Eviction_recycles_a_completed_slot_as_a_fresh_in_flight_reservation()
    {
        var cache = new RecentApplyCache(1);
        cache.TryAdd(Entry("a"));
        cache.Complete(Entry("a"));

        var added = cache.TryAdd(Entry("b"), out _);

        Assert.Multiple(() =>
        {
            Assert.That(added, Is.True);
            Assert.That(cache.Contains(Entry("a")), Is.False);
            Assert.That(cache.IsInFlight(Entry("b")), Is.True,
                "A recycled node must not carry the evicted tuple's completed state.");
        });
    }
}
