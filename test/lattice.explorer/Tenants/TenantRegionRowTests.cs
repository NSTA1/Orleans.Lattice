using Orleans.Lattice.Explorer.Plugins.Tenancy;
using Orleans.Lattice.Explorer.Plugins.Tenants;

namespace Orleans.Lattice.Explorer.Tests.Tenants;

/// <summary>
/// The region row: the two-set model (operator-authorized allowed set versus
/// tenant-managed residency) stays legible, and a pending revocation that would
/// strand residency is recognisable before the call rather than after it.
/// </summary>
[TestFixture]
public sealed class TenantRegionRowTests
{
    private static TenantRegionRow Row(
        ExplorerTenantRegionLifecycle status = ExplorerTenantRegionLifecycle.Online,
        bool isAllowed = true) =>
        new(new ExplorerTenantRegion(SampleTenants.Region, status, isAllowed));

    [Test]
    public void A_row_starts_with_its_pending_intent_matching_the_cluster()
    {
        var allowed = Row(isAllowed: true);
        var notAllowed = Row(status: ExplorerTenantRegionLifecycle.None, isAllowed: false);

        Assert.Multiple(() =>
        {
            Assert.That(allowed.Allow, Is.True);
            Assert.That(allowed.IsChanged, Is.False);
            Assert.That(notAllowed.Allow, Is.False);
            Assert.That(notAllowed.IsChanged, Is.False);
        });
    }

    [Test]
    public void Changing_the_pending_intent_marks_the_row_changed()
    {
        var row = Row();
        row.Allow = false;

        Assert.That(row.IsChanged, Is.True);
    }

    [Test]
    public void Allowed_and_resident_stay_separate_facts()
    {
        var allowedButEmpty = Row(status: ExplorerTenantRegionLifecycle.None, isAllowed: true);
        var residentButRevoked = Row(status: ExplorerTenantRegionLifecycle.Draining, isAllowed: false);

        Assert.Multiple(() =>
        {
            Assert.That(allowedButEmpty.IsAllowed, Is.True);
            Assert.That(allowedButEmpty.IsResident, Is.False);

            // An operator can revoke an authorization while the tenant is still
            // draining out of the region, so the two facts genuinely diverge.
            Assert.That(residentButRevoked.IsAllowed, Is.False);
            Assert.That(residentButRevoked.IsResident, Is.True);
        });
    }

    [Test]
    public void Revoking_a_region_the_tenant_is_resident_in_is_recognised_before_the_call()
    {
        var row = Row(status: ExplorerTenantRegionLifecycle.Online, isAllowed: true);
        row.Allow = false;

        Assert.That(row.WouldRevokeResident, Is.True);
    }

    [Test]
    public void Revoking_an_authorization_the_tenant_never_used_strands_nothing()
    {
        var row = Row(status: ExplorerTenantRegionLifecycle.None, isAllowed: true);
        row.Allow = false;

        Assert.That(row.WouldRevokeResident, Is.False);
    }

    [Test]
    public void Adding_an_authorization_never_strands_residency()
    {
        var row = Row(status: ExplorerTenantRegionLifecycle.None, isAllowed: false);
        row.Allow = true;

        Assert.Multiple(() =>
        {
            Assert.That(row.IsChanged, Is.True);
            Assert.That(row.WouldRevokeResident, Is.False);
        });
    }

    [TestCase(ExplorerTenantRegionLifecycle.None, false)]
    [TestCase(ExplorerTenantRegionLifecycle.Provisioning, true)]
    [TestCase(ExplorerTenantRegionLifecycle.Backfilling, true)]
    [TestCase(ExplorerTenantRegionLifecycle.Online, true)]
    [TestCase(ExplorerTenantRegionLifecycle.Draining, true)]
    [TestCase(ExplorerTenantRegionLifecycle.Offline, false)]
    [TestCase(ExplorerTenantRegionLifecycle.Removed, false)]
    public void Residency_covers_every_state_in_which_the_tenant_holds_data(
        ExplorerTenantRegionLifecycle status,
        bool expected)
    {
        Assert.That(Row(status).IsResident, Is.EqualTo(expected));
    }

    [Test]
    public void Every_residency_state_carries_a_distinct_label()
    {
        var labels = Enum.GetValues<ExplorerTenantRegionLifecycle>()
            .Select(status => Row(status).StatusLabel)
            .ToArray();

        Assert.That(labels, Is.Unique);
    }

    [Test]
    public void The_authorization_state_reads_as_words()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Row(isAllowed: true).AllowedLabel, Is.EqualTo("Allowed"));
            Assert.That(
                Row(status: ExplorerTenantRegionLifecycle.None, isAllowed: false).AllowedLabel,
                Is.EqualTo("Not allowed"));
        });
    }

    [Test]
    public void The_moving_states_are_styled_apart_from_the_settled_ones()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Row(ExplorerTenantRegionLifecycle.Online).StatusClass, Is.EqualTo("is-online"));
            Assert.That(Row(ExplorerTenantRegionLifecycle.Provisioning).StatusClass, Is.EqualTo("is-moving"));
            Assert.That(Row(ExplorerTenantRegionLifecycle.Backfilling).StatusClass, Is.EqualTo("is-moving"));
            Assert.That(Row(ExplorerTenantRegionLifecycle.Draining).StatusClass, Is.EqualTo("is-draining"));
            Assert.That(Row(ExplorerTenantRegionLifecycle.Removed).StatusClass, Is.EqualTo("is-idle"));
        });
    }
}
