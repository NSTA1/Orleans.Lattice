using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Primitives;

/// <summary>
/// Convergence regression for <see cref="LwwValue{T}.Merge"/> on a full
/// timestamp/identity tie.
/// <para>
/// The tie-break ordered on (<c>Timestamp</c>, <c>OriginClusterId</c>,
/// <c>IsTombstone</c>) and then returned the left operand unconditionally, so
/// two entries that agreed on all three but differed in <c>Value</c>,
/// <c>ExpiresAtTicks</c>, or <c>IsMigrated</c> merged order-dependently:
/// <c>Merge(a, b) == a</c> but <c>Merge(b, a) == b</c>. The XML doc claimed
/// commutativity outright. <c>OriginClusterId</c> is <see langword="null"/> for
/// every purely local write and <see cref="HybridLogicalClock"/> carries no node
/// identity, so a <c>(W, 0)</c> collision between two leaves that first ticked
/// in the same wall-clock tick is an ordinary occurrence, not a contrived one.
/// </para>
/// </summary>
[TestFixture]
public class LwwValueMergeConvergenceTests
{
    private static readonly HybridLogicalClock Tie = new() { WallClockTicks = 12345, Counter = 7 };

    [Test]
    public void Merge_is_commutative_when_local_writes_tie_on_timestamp_and_differ_only_in_value()
    {
        // Both authored locally (OriginClusterId is null), both live, same HLC.
        var a = LwwValue<byte[]>.Create([1, 2, 3], Tie);
        var b = LwwValue<byte[]>.Create([9, 9, 9], Tie);

        Assert.That(LwwValue<byte[]>.Merge(b, a), Is.EqualTo(LwwValue<byte[]>.Merge(a, b)),
            "two local writes at the same HLC must resolve to the same winner on both replicas");
    }

    [Test]
    public void Merge_is_commutative_when_entries_tie_on_identity_and_differ_only_in_expiry()
    {
        var a = LwwValue<byte[]>.CreateWithExpiry([1], Tie, expiresAtTicks: 1000);
        var b = LwwValue<byte[]>.CreateWithExpiry([1], Tie, expiresAtTicks: 5000);

        Assert.That(LwwValue<byte[]>.Merge(b, a), Is.EqualTo(LwwValue<byte[]>.Merge(a, b)),
            "ExpiresAtTicks is durable state and must participate in the tie-break total order");
    }

    [Test]
    public void Merge_is_commutative_when_entries_tie_on_identity_and_differ_only_in_migrated_flag()
    {
        var value = new byte[] { 4 };
        var a = LwwValue<byte[]>.Create(value, Tie);
        var b = a with { IsMigrated = true };

        Assert.That(LwwValue<byte[]>.Merge(b, a), Is.EqualTo(LwwValue<byte[]>.Merge(a, b)),
            "IsMigrated changes the foreground orphan-drain decision, so it must resolve deterministically");
    }

    [Test]
    public void Merge_is_associative_when_three_local_writes_tie_on_timestamp()
    {
        var a = LwwValue<byte[]>.Create([1], Tie);
        var b = LwwValue<byte[]>.Create([2], Tie);
        var c = LwwValue<byte[]>.Create([3], Tie);

        var leftAssociated = LwwValue<byte[]>.Merge(LwwValue<byte[]>.Merge(a, b), c);
        var rightAssociated = LwwValue<byte[]>.Merge(a, LwwValue<byte[]>.Merge(b, c));

        Assert.That(rightAssociated, Is.EqualTo(leftAssociated),
            "the tie-break must be a total order, not a left-bias");
    }

    [Test]
    public void Merge_is_idempotent_on_a_full_tie()
    {
        var a = LwwValue<byte[]>.Create([1, 2, 3], Tie);

        Assert.That(LwwValue<byte[]>.Merge(a, a), Is.EqualTo(a));
    }

    [Test]
    public void Merge_still_prefers_the_higher_timestamp_over_the_tie_break_fields()
    {
        var older = LwwValue<byte[]>.Create([9, 9, 9], new HybridLogicalClock { WallClockTicks = 1, Counter = 0 });
        var newer = LwwValue<byte[]>.Create([1], new HybridLogicalClock { WallClockTicks = 2, Counter = 0 });

        Assert.Multiple(() =>
        {
            Assert.That(LwwValue<byte[]>.Merge(older, newer).Value, Is.EqualTo(new byte[] { 1 }));
            Assert.That(LwwValue<byte[]>.Merge(newer, older).Value, Is.EqualTo(new byte[] { 1 }));
        });
    }

    [Test]
    public void Merge_still_prefers_the_tombstone_over_the_value_tie_break()
    {
        var live = LwwValue<byte[]>.Create([9, 9, 9], Tie);
        var dead = LwwValue<byte[]>.Tombstone(Tie);

        Assert.Multiple(() =>
        {
            Assert.That(LwwValue<byte[]>.Merge(live, dead).IsTombstone, Is.True);
            Assert.That(LwwValue<byte[]>.Merge(dead, live).IsTombstone, Is.True);
        });
    }
}
