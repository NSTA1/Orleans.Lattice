using System.IO.Hashing;
using System.Buffers;
using System.Buffers.Binary;

using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins the digest bytes <c>BPlusLeafGrain.FeedVectorClock</c> emits.
/// <para>
/// A single-replica clock is already in ordinal order, so it now bypasses the
/// pooled rent, the copy, the one-element <c>Array.Sort</c> and the cleared
/// return that a multi-replica clock still needs. That fast path is a second
/// way of producing the same bytes, so the only thing worth asserting is that
/// it produces exactly those bytes: the digest is a wire-visible projection
/// value, and a divergence would make two replicas of identical data disagree
/// on their leaf digest and force an endless reconcile.
/// </para>
/// <para>
/// Each fixture therefore compares against <see cref="RentAndSortFeed"/>, a
/// verbatim copy of the pre-trim body, rather than against a recorded
/// constant - so the guard is a true equivalence and not a snapshot that would
/// have to be re-baselined whenever the encoding legitimately changes.
/// </para>
/// </summary>
[TestFixture]
public sealed class BPlusLeafGrainDigestVectorClockTests
{
    /// <summary>Verbatim copy of the pre-trim body, rent, sort and all.</summary>
    private static void RentAndSortFeed(XxHash128 hasher, VersionVector? vc, Span<byte> scratch)
    {
        if (vc is null || vc.Entries.Count == 0)
        {
            BinaryPrimitives.WriteInt32LittleEndian(scratch[..4], -1);
            hasher.Append(scratch[..4]);
            return;
        }

        var count = vc.Entries.Count;
        var replicas = ArrayPool<string>.Shared.Rent(count);
        try
        {
            var i = 0;
            foreach (var k in vc.Entries.Keys) replicas[i++] = k;
            Array.Sort(replicas, 0, count, StringComparer.Ordinal);

            BinaryPrimitives.WriteInt32LittleEndian(scratch[..4], count);
            hasher.Append(scratch[..4]);

            for (var j = 0; j < count; j++)
            {
                var replica = replicas[j];
                BPlusLeafGrain.FeedString(hasher, replica, scratch);
                var clock = vc.Entries[replica];
                BinaryPrimitives.WriteInt64LittleEndian(scratch, clock.WallClockTicks);
                hasher.Append(scratch[..8]);
                BinaryPrimitives.WriteInt32LittleEndian(scratch[..4], clock.Counter);
                hasher.Append(scratch[..4]);
            }
        }
        finally
        {
            ArrayPool<string>.Shared.Return(replicas, clearArray: true);
        }
    }

    private static VersionVector Clock(params (string Replica, long Ticks, int Counter)[] entries)
    {
        var vc = new VersionVector();
        foreach (var (replica, ticks, counter) in entries)
        {
            vc.Entries[replica] = new HybridLogicalClock { WallClockTicks = ticks, Counter = counter };
        }

        return vc;
    }

    private static byte[] Feed(Action<XxHash128, Span<byte>> feed)
    {
        Span<byte> scratch = stackalloc byte[16];
        var hasher = new XxHash128();
        feed(hasher, scratch);
        return hasher.GetCurrentHash();
    }

    private static IEnumerable<VersionVector?> Corpus()
    {
        yield return null;
        yield return Clock();
        yield return Clock(("r0", 1L, 0));
        yield return Clock(("replica-with-a-much-longer-id", long.MaxValue, int.MaxValue));
        yield return Clock(("\u65e5\u672c", 42L, 7));
        yield return Clock(("b", 2L, 1), ("a", 1L, 0));
        yield return Clock(("z", 3L, 2), ("m", 2L, 1), ("a", 1L, 0));
        yield return Clock(("r3", 4L, 3), ("r1", 2L, 1), ("r2", 3L, 2), ("r0", 1L, 0));
    }

    /// <summary>
    /// The shipped feed emits byte-for-byte what the rent-and-sort form emits,
    /// for every arity from null and empty through the single-replica fast path
    /// to a four-replica clock inserted out of order.
    /// </summary>
    [Test]
    public void FeedVectorClock_emits_the_same_bytes_as_the_rent_and_sort_form()
    {
        Assert.Multiple(() =>
        {
            foreach (var vc in Corpus())
            {
                var expected = Feed((h, s) => RentAndSortFeed(h, vc, s));
                var actual = Feed((h, s) => BPlusLeafGrain.FeedVectorClock(h, vc, s));
                Assert.That(
                    actual,
                    Is.EqualTo(expected).AsCollection,
                    $"digest diverged for a {vc?.Entries.Count.ToString() ?? "null"}-replica clock");
            }
        });
    }

    /// <summary>
    /// The fast path does not disturb the ordinal ordering the digest depends
    /// on: two clocks carrying the same replicas inserted in opposite orders
    /// still hash identically.
    /// </summary>
    [Test]
    public void FeedVectorClock_is_independent_of_insertion_order()
    {
        var forwards = Clock(("a", 1L, 0), ("b", 2L, 1), ("c", 3L, 2));
        var backwards = Clock(("c", 3L, 2), ("b", 2L, 1), ("a", 1L, 0));

        Assert.That(
            Feed((h, s) => BPlusLeafGrain.FeedVectorClock(h, forwards, s)),
            Is.EqualTo(Feed((h, s) => BPlusLeafGrain.FeedVectorClock(h, backwards, s))).AsCollection);
    }

    /// <summary>
    /// A single-replica clock still hashes differently from an empty one and
    /// from a two-replica clock sharing its first entry, so the fast path has
    /// not dropped the arity prefix that separates them.
    /// </summary>
    [Test]
    public void FeedVectorClock_separates_a_single_replica_clock_from_its_neighbours()
    {
        var single = Feed((h, s) => BPlusLeafGrain.FeedVectorClock(h, Clock(("a", 1L, 0)), s));
        var empty = Feed((h, s) => BPlusLeafGrain.FeedVectorClock(h, Clock(), s));
        var pair = Feed((h, s) => BPlusLeafGrain.FeedVectorClock(h, Clock(("a", 1L, 0), ("b", 2L, 1)), s));

        Assert.Multiple(() =>
        {
            Assert.That(single, Is.Not.EqualTo(empty).AsCollection);
            Assert.That(single, Is.Not.EqualTo(pair).AsCollection);
        });
    }

    /// <summary>
    /// A single-replica clock's replica id and clock both still reach the
    /// digest: changing either changes the hash.
    /// </summary>
    [Test]
    public void FeedVectorClock_feeds_every_field_of_a_single_replica_clock()
    {
        var baseline = Feed((h, s) => BPlusLeafGrain.FeedVectorClock(h, Clock(("a", 1L, 0)), s));

        Assert.Multiple(() =>
        {
            Assert.That(
                Feed((h, s) => BPlusLeafGrain.FeedVectorClock(h, Clock(("b", 1L, 0)), s)),
                Is.Not.EqualTo(baseline).AsCollection,
                "replica id not fed");
            Assert.That(
                Feed((h, s) => BPlusLeafGrain.FeedVectorClock(h, Clock(("a", 2L, 0)), s)),
                Is.Not.EqualTo(baseline).AsCollection,
                "wall-clock ticks not fed");
            Assert.That(
                Feed((h, s) => BPlusLeafGrain.FeedVectorClock(h, Clock(("a", 1L, 1)), s)),
                Is.Not.EqualTo(baseline).AsCollection,
                "counter not fed");
        });
    }
}
