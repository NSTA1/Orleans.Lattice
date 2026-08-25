namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantRegionLifecycle"/>, the single source of truth
/// for the per-tenant region-residency lifecycle: residency / online
/// classification and the legal forward promotions on the add and remove paths.
/// Pure table-driven checks - no timing, no state.
/// </summary>
[TestFixture]
public sealed class TenantRegionLifecycleTests
{
    [TestCase(TenantRegionStatus.Provisioning, true)]
    [TestCase(TenantRegionStatus.Backfilling, true)]
    [TestCase(TenantRegionStatus.Online, true)]
    [TestCase(TenantRegionStatus.None, false)]
    [TestCase(TenantRegionStatus.Draining, false)]
    [TestCase(TenantRegionStatus.Offline, false)]
    [TestCase(TenantRegionStatus.Removed, false)]
    public void IsResident_classifies_only_provisioning_backfilling_online_as_resident(
        TenantRegionStatus status, bool expected) =>
        Assert.That(TenantRegionLifecycle.IsResident(status), Is.EqualTo(expected));

    [TestCase(TenantRegionStatus.Online, true)]
    [TestCase(TenantRegionStatus.Provisioning, false)]
    [TestCase(TenantRegionStatus.Backfilling, false)]
    [TestCase(TenantRegionStatus.Draining, false)]
    [TestCase(TenantRegionStatus.Offline, false)]
    [TestCase(TenantRegionStatus.Removed, false)]
    [TestCase(TenantRegionStatus.None, false)]
    public void IsOnline_is_true_only_for_online(TenantRegionStatus status, bool expected) =>
        Assert.That(TenantRegionLifecycle.IsOnline(status), Is.EqualTo(expected));

    [TestCase(TenantRegionStatus.None, TenantRegionStatus.Provisioning)]
    [TestCase(TenantRegionStatus.Removed, TenantRegionStatus.Provisioning)]
    [TestCase(TenantRegionStatus.Offline, TenantRegionStatus.Provisioning)]
    [TestCase(TenantRegionStatus.Draining, TenantRegionStatus.Provisioning)]
    public void NextOnAdd_from_a_non_resident_status_begins_provisioning(
        TenantRegionStatus current, TenantRegionStatus expected) =>
        Assert.That(TenantRegionLifecycle.NextOnAdd(current), Is.EqualTo(expected));

    [TestCase(TenantRegionStatus.Provisioning)]
    [TestCase(TenantRegionStatus.Backfilling)]
    [TestCase(TenantRegionStatus.Online)]
    public void NextOnAdd_from_a_resident_status_is_null(TenantRegionStatus current) =>
        Assert.That(TenantRegionLifecycle.NextOnAdd(current), Is.Null);

    [TestCase(TenantRegionStatus.Provisioning)]
    [TestCase(TenantRegionStatus.Backfilling)]
    [TestCase(TenantRegionStatus.Online)]
    public void NextOnRemove_from_a_resident_status_begins_draining(TenantRegionStatus current) =>
        Assert.That(TenantRegionLifecycle.NextOnRemove(current), Is.EqualTo(TenantRegionStatus.Draining));

    [TestCase(TenantRegionStatus.None)]
    [TestCase(TenantRegionStatus.Draining)]
    [TestCase(TenantRegionStatus.Offline)]
    [TestCase(TenantRegionStatus.Removed)]
    public void NextOnRemove_from_a_non_resident_status_is_null(TenantRegionStatus current) =>
        Assert.That(TenantRegionLifecycle.NextOnRemove(current), Is.Null);

    [TestCase(TenantRegionStatus.Provisioning, TenantRegionStatus.Backfilling, true)]
    [TestCase(TenantRegionStatus.Backfilling, TenantRegionStatus.Online, true)]
    [TestCase(TenantRegionStatus.Draining, TenantRegionStatus.Offline, true)]
    [TestCase(TenantRegionStatus.Offline, TenantRegionStatus.Removed, true)]
    [TestCase(TenantRegionStatus.Provisioning, TenantRegionStatus.Online, false)]
    [TestCase(TenantRegionStatus.Online, TenantRegionStatus.Backfilling, false)]
    [TestCase(TenantRegionStatus.None, TenantRegionStatus.Provisioning, false)]
    [TestCase(TenantRegionStatus.Online, TenantRegionStatus.Draining, false)]
    public void IsLegalPromotion_recognises_only_single_step_forward_advances(
        TenantRegionStatus from, TenantRegionStatus to, bool expected) =>
        Assert.That(TenantRegionLifecycle.IsLegalPromotion(from, to), Is.EqualTo(expected));

    [TestCase(TenantRegionStatus.Provisioning, TenantRegionStatus.Backfilling)]
    [TestCase(TenantRegionStatus.Backfilling, TenantRegionStatus.Online)]
    [TestCase(TenantRegionStatus.Draining, TenantRegionStatus.Offline)]
    [TestCase(TenantRegionStatus.Offline, TenantRegionStatus.Removed)]
    public void TryNextPromotion_advances_a_transitional_status(
        TenantRegionStatus current, TenantRegionStatus expected)
    {
        var advanced = TenantRegionLifecycle.TryNextPromotion(current, out var next);

        Assert.Multiple(() =>
        {
            Assert.That(advanced, Is.True);
            Assert.That(next, Is.EqualTo(expected));
        });
    }

    [TestCase(TenantRegionStatus.None)]
    [TestCase(TenantRegionStatus.Online)]
    [TestCase(TenantRegionStatus.Removed)]
    public void TryNextPromotion_is_a_no_op_at_a_terminal_status(TenantRegionStatus current)
    {
        var advanced = TenantRegionLifecycle.TryNextPromotion(current, out var next);

        Assert.Multiple(() =>
        {
            Assert.That(advanced, Is.False);
            Assert.That(next, Is.EqualTo(current));
        });
    }
}
