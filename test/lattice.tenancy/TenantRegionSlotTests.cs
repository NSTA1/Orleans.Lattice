namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for the two per-region CRDT slot structs
/// (<see cref="TenantRegionAllowSlot"/> and <see cref="TenantRegionStatusSlot"/>).
/// Both are last-writer-wins elements whose <c>Merge</c> keeps the slot with the
/// winning <see cref="TenantClock"/> stamp, so they must be commutative,
/// associative, and idempotent. Convergence is a property of the stamp order
/// alone, so every clock is built by hand - no timing, no wall-clock.
/// </summary>
[TestFixture]
public sealed class TenantRegionSlotTests
{
    private static TenantRegionAllowSlot Allow(bool present, long ticks, string? writer = "w") =>
        new() { Present = present, Clock = TestClocks.Clock(ticks), WriterId = writer };

    private static TenantRegionStatusSlot Status(TenantRegionStatus status, long ticks, string? writer = "w") =>
        new() { Status = status, Clock = TestClocks.Clock(ticks), WriterId = writer };

    [Test]
    public void AllowSlot_merge_keeps_the_higher_stamp()
    {
        var older = Allow(present: true, ticks: 1);
        var newer = Allow(present: false, ticks: 2);

        Assert.Multiple(() =>
        {
            Assert.That(TenantRegionAllowSlot.Merge(older, newer), Is.EqualTo(newer));
            Assert.That(TenantRegionAllowSlot.Merge(newer, older), Is.EqualTo(newer));
        });
    }

    [Test]
    public void AllowSlot_merge_is_idempotent()
    {
        var slot = Allow(present: true, ticks: 5);

        Assert.That(TenantRegionAllowSlot.Merge(slot, slot), Is.EqualTo(slot));
    }

    [Test]
    public void AllowSlot_concurrent_authorize_and_revoke_converge_on_the_winning_stamp()
    {
        var authorize = Allow(present: true, ticks: 10, writer: "op-a");
        var revoke = Allow(present: false, ticks: 11, writer: "op-b");

        // Both merge orders must yield the same winner regardless of arrival order.
        var ab = TenantRegionAllowSlot.Merge(authorize, revoke);
        var ba = TenantRegionAllowSlot.Merge(revoke, authorize);

        Assert.Multiple(() =>
        {
            Assert.That(ab, Is.EqualTo(ba));
            Assert.That(ab.Present, Is.False);
        });
    }

    [Test]
    public void StatusSlot_merge_keeps_the_higher_stamp()
    {
        var older = Status(TenantRegionStatus.Provisioning, ticks: 1);
        var newer = Status(TenantRegionStatus.Online, ticks: 2);

        Assert.Multiple(() =>
        {
            Assert.That(TenantRegionStatusSlot.Merge(older, newer), Is.EqualTo(newer));
            Assert.That(TenantRegionStatusSlot.Merge(newer, older), Is.EqualTo(newer));
        });
    }

    [Test]
    public void StatusSlot_merge_is_idempotent()
    {
        var slot = Status(TenantRegionStatus.Backfilling, ticks: 5);

        Assert.That(TenantRegionStatusSlot.Merge(slot, slot), Is.EqualTo(slot));
    }

    [Test]
    public void StatusSlot_concurrent_transitions_converge_deterministically()
    {
        var draining = Status(TenantRegionStatus.Draining, ticks: 20, writer: "region-a");
        var online = Status(TenantRegionStatus.Online, ticks: 21, writer: "region-b");

        var ab = TenantRegionStatusSlot.Merge(draining, online);
        var ba = TenantRegionStatusSlot.Merge(online, draining);

        Assert.Multiple(() =>
        {
            Assert.That(ab, Is.EqualTo(ba));
            Assert.That(ab.Status, Is.EqualTo(TenantRegionStatus.Online));
        });
    }
}
