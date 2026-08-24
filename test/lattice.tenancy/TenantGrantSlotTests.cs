using static Orleans.Lattice.Tenancy.Tests.TestClocks;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Convergence unit tests for <see cref="TenantGrantSlot"/>, the LWW-element-map
/// element behind a tenant's cross-tenant grants. Both the grant payload and its
/// presence bit converge deterministically under the shared stamp order.
/// </summary>
public sealed class TenantGrantSlotTests
{
    private static readonly CrossTenantGrant Read =
        CrossTenantGrant.Create("sub-a", TenantGranteeKind.Subject, "tree-x", TenantGrantOperations.Read);

    private static readonly CrossTenantGrant ReadWrite =
        CrossTenantGrant.Create("sub-a", TenantGranteeKind.Subject, "tree-x", TenantGrantOperations.ReadWrite);

    private static TenantGrantSlot Slot(CrossTenantGrant grant, bool present, long ticks, string? writer) =>
        new() { Grant = grant, Present = present, Clock = Clock(ticks), WriterId = writer };

    [Test]
    public void Merge_keeps_the_higher_clock_payload_and_presence()
    {
        var issued = Slot(Read, present: true, 10, "w1");
        var updated = Slot(ReadWrite, present: true, 20, "w1");

        var merged = TenantGrantSlot.Merge(issued, updated);

        Assert.Multiple(() =>
        {
            Assert.That(merged.Present, Is.True);
            Assert.That(merged.Grant.Operations, Is.EqualTo(TenantGrantOperations.ReadWrite));
        });
    }

    [Test]
    public void Merge_revoke_wins_over_a_lower_stamp_issue()
    {
        var issued = Slot(Read, present: true, 10, "w1");
        var revoked = Slot(Read, present: false, 20, "w1");

        Assert.That(TenantGrantSlot.Merge(issued, revoked).Present, Is.False);
    }

    [Test]
    public void Merge_is_commutative()
    {
        var issued = Slot(Read, present: true, 10, "w1");
        var revoked = Slot(Read, present: false, 20, "w2");

        Assert.That(TenantGrantSlot.Merge(issued, revoked), Is.EqualTo(TenantGrantSlot.Merge(revoked, issued)));
    }

    [Test]
    public void Merge_is_associative()
    {
        var a = Slot(Read, present: true, 10, "w1");
        var b = Slot(ReadWrite, present: true, 20, "w2");
        var c = Slot(Read, present: false, 30, "w3");

        var left = TenantGrantSlot.Merge(TenantGrantSlot.Merge(a, b), c);
        var right = TenantGrantSlot.Merge(a, TenantGrantSlot.Merge(b, c));

        Assert.That(left, Is.EqualTo(right));
        Assert.That(left.Present, Is.False);
    }

    [Test]
    public void Merge_is_idempotent()
    {
        var slot = Slot(ReadWrite, present: true, 10, "w1");

        Assert.That(TenantGrantSlot.Merge(slot, slot), Is.EqualTo(slot));
    }

    [Test]
    public void Merge_breaks_a_clock_tie_by_ordinal_writer_id()
    {
        var loser = Slot(Read, present: true, 10, "w1");
        var winner = Slot(Read, present: false, 10, "w2");

        Assert.That(TenantGrantSlot.Merge(loser, winner).Present, Is.False);
    }
}
