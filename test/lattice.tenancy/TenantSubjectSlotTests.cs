using static Orleans.Lattice.Tenancy.Tests.TestClocks;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Convergence unit tests for <see cref="TenantSubjectSlot"/>, the LWW-element-set
/// element behind a tenant's admin subjects. The add/remove presence bit converges
/// deterministically under the shared stamp order.
/// </summary>
public sealed class TenantSubjectSlotTests
{
    private static TenantSubjectSlot Slot(bool present, long ticks, string? writer) =>
        new() { Present = present, Clock = Clock(ticks), WriterId = writer };

    [Test]
    public void Merge_keeps_the_higher_clock_presence()
    {
        var added = Slot(present: true, 10, "w1");
        var removed = Slot(present: false, 20, "w1");

        Assert.That(TenantSubjectSlot.Merge(added, removed).Present, Is.False);
    }

    [Test]
    public void Merge_is_commutative()
    {
        var added = Slot(present: true, 10, "w1");
        var removed = Slot(present: false, 20, "w2");

        Assert.That(TenantSubjectSlot.Merge(added, removed), Is.EqualTo(TenantSubjectSlot.Merge(removed, added)));
    }

    [Test]
    public void Merge_is_associative()
    {
        var a = Slot(present: true, 10, "w1");
        var b = Slot(present: false, 20, "w2");
        var c = Slot(present: true, 30, "w3");

        var left = TenantSubjectSlot.Merge(TenantSubjectSlot.Merge(a, b), c);
        var right = TenantSubjectSlot.Merge(a, TenantSubjectSlot.Merge(b, c));

        Assert.That(left, Is.EqualTo(right));
        Assert.That(left.Present, Is.True);
    }

    [Test]
    public void Merge_is_idempotent()
    {
        var slot = Slot(present: true, 10, "w1");

        Assert.That(TenantSubjectSlot.Merge(slot, slot), Is.EqualTo(slot));
    }

    [Test]
    public void Merge_breaks_a_clock_tie_by_ordinal_writer_id()
    {
        var loser = Slot(present: true, 10, "w1");
        var winner = Slot(present: false, 10, "w2");

        Assert.That(TenantSubjectSlot.Merge(loser, winner).Present, Is.False);
    }
}
