using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Plugins.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.Tenancy;

/// <summary>
/// Covers the parts of the projection that carry a state a surface can act on:
/// a grant's lifecycle state and the operations it authorizes, a tenant's
/// lifecycle, and a region's two-set allowed-versus-resident model. Each enum
/// translation is asserted to fail closed on a value this Explorer does not
/// know, so a newer server can never widen what the UI presents as authorized.
/// </summary>
[TestFixture]
public class TenantStateProjectionTests
{
    [Test]
    public void Only_an_active_grant_authorizes_anything()
    {
        var active = TenantProjection.ToGrant(SampleTenant.Grant(TenantGrantLifecycleState.Active));
        var pending = TenantProjection.ToGrant(SampleTenant.Grant(TenantGrantLifecycleState.Pending));
        var rejected = TenantProjection.ToGrant(SampleTenant.Grant(TenantGrantLifecycleState.Rejected));
        var revoked = TenantProjection.ToGrant(SampleTenant.Grant(TenantGrantLifecycleState.Revoked));

        Assert.Multiple(() =>
        {
            Assert.That(active.AuthorizesAccess, Is.True);
            Assert.That(pending.AuthorizesAccess, Is.False);
            Assert.That(rejected.AuthorizesAccess, Is.False);
            Assert.That(revoked.AuthorizesAccess, Is.False);
        });
    }

    [Test]
    public void A_pending_grant_is_the_one_awaiting_approval_and_the_terminal_ones_are_closed()
    {
        var pending = TenantProjection.ToGrant(SampleTenant.Grant(TenantGrantLifecycleState.Pending));
        var active = TenantProjection.ToGrant(SampleTenant.Grant(TenantGrantLifecycleState.Active));
        var rejected = TenantProjection.ToGrant(SampleTenant.Grant(TenantGrantLifecycleState.Rejected));
        var revoked = TenantProjection.ToGrant(SampleTenant.Grant(TenantGrantLifecycleState.Revoked));

        Assert.Multiple(() =>
        {
            Assert.That(pending.IsAwaitingApproval, Is.True);
            Assert.That(pending.IsClosed, Is.False);
            Assert.That(active.IsAwaitingApproval, Is.False);
            Assert.That(active.IsClosed, Is.False);
            Assert.That(rejected.IsClosed, Is.True);
            Assert.That(revoked.IsClosed, Is.True);
        });
    }

    [Test]
    public void An_unknown_grant_state_fails_closed_to_revoked_rather_than_active()
    {
        var grant = TenantProjection.ToGrant(SampleTenant.Grant((TenantGrantLifecycleState)99));

        Assert.Multiple(() =>
        {
            Assert.That(grant.State, Is.EqualTo(ExplorerTenantGrantState.Revoked));
            Assert.That(grant.AuthorizesAccess, Is.False, "an unknown state must never fail open onto Active");
        });
    }

    [Test]
    public void Grant_operations_translate_bit_by_bit_in_both_directions()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                TenantProjection.ToGrant(SampleTenant.Grant(operations: TenantGrantAccess.None)).Operations,
                Is.EqualTo(ExplorerTenantGrantAccess.None));
            Assert.That(
                TenantProjection.ToGrant(SampleTenant.Grant(operations: TenantGrantAccess.Read)).Operations,
                Is.EqualTo(ExplorerTenantGrantAccess.Read));
            Assert.That(
                TenantProjection.ToGrant(SampleTenant.Grant(operations: TenantGrantAccess.Write)).Operations,
                Is.EqualTo(ExplorerTenantGrantAccess.Write));
            Assert.That(
                TenantProjection.ToGrant(SampleTenant.Grant(operations: TenantGrantAccess.ReadWrite)).Operations,
                Is.EqualTo(ExplorerTenantGrantAccess.ReadWrite));
            Assert.That(
                TenantProjection.ToWireGrantAccess(ExplorerTenantGrantAccess.ReadWrite),
                Is.EqualTo(TenantGrantAccess.ReadWrite));
            Assert.That(
                TenantProjection.ToWireGrantAccess(ExplorerTenantGrantAccess.None),
                Is.EqualTo(TenantGrantAccess.None));
        });
    }

    [Test]
    public void An_unknown_grant_operation_bit_is_dropped_rather_than_carried()
    {
        var grant = TenantProjection.ToGrant(SampleTenant.Grant(operations: (TenantGrantAccess)0b1000));

        Assert.That(grant.Operations, Is.EqualTo(ExplorerTenantGrantAccess.None));
    }

    [Test]
    public void A_grant_report_keeps_the_issued_and_received_directions_apart()
    {
        var grants = TenantProjection.ToGrants(SampleTenant.GrantReport());

        Assert.Multiple(() =>
        {
            Assert.That(grants.TenantId, Is.EqualTo(SampleTenant.TenantId));
            Assert.That(grants.Issued[0].GranterTenantId, Is.EqualTo(SampleTenant.TenantId));
            Assert.That(grants.Received[0].GranteeTenantId, Is.EqualTo(SampleTenant.TenantId));
            Assert.That(ExplorerTenantGrants.Empty.Issued, Is.Empty);
            Assert.That(ExplorerTenantGrants.Empty.Received, Is.Empty);
        });
    }

    [Test]
    public void A_grant_change_carries_the_committed_grant_and_whether_it_moved()
    {
        var change = TenantProjection.ToGrantChange(new TenantGrantChangeResult
        {
            Grant = SampleTenant.Grant(TenantGrantLifecycleState.Active),
            Changed = false,
        });

        Assert.Multiple(() =>
        {
            Assert.That(change.Changed, Is.False, "an idempotent repeat reports no change");
            Assert.That(change.Grant.State, Is.EqualTo(ExplorerTenantGrantState.Active));
        });
    }

    [Test]
    public void An_unknown_tenant_lifecycle_fails_closed_to_suspended()
    {
        var summary = TenantProjection.ToSummary(SampleTenant.Descriptor(status: (TenantLifecycleStatus)99));

        Assert.That(summary.Status, Is.EqualTo(ExplorerTenantLifecycle.Suspended));
    }

    [TestCase(TenantRegionLifecycleStatus.None, ExplorerTenantRegionLifecycle.None, false)]
    [TestCase(TenantRegionLifecycleStatus.Provisioning, ExplorerTenantRegionLifecycle.Provisioning, true)]
    [TestCase(TenantRegionLifecycleStatus.Backfilling, ExplorerTenantRegionLifecycle.Backfilling, true)]
    [TestCase(TenantRegionLifecycleStatus.Online, ExplorerTenantRegionLifecycle.Online, true)]
    [TestCase(TenantRegionLifecycleStatus.Draining, ExplorerTenantRegionLifecycle.Draining, true)]
    [TestCase(TenantRegionLifecycleStatus.Offline, ExplorerTenantRegionLifecycle.Offline, false)]
    [TestCase(TenantRegionLifecycleStatus.Removed, ExplorerTenantRegionLifecycle.Removed, false)]
    public void A_region_lifecycle_translates_and_reports_whether_the_tenant_holds_data_there(
        TenantRegionLifecycleStatus wire,
        ExplorerTenantRegionLifecycle expected,
        bool isResident)
    {
        var regions = TenantProjection.ToRegions([SampleTenant.RegionDescriptor(status: wire)]);

        Assert.Multiple(() =>
        {
            Assert.That(regions[0].Status, Is.EqualTo(expected));
            Assert.That(regions[0].IsResident, Is.EqualTo(isResident));
        });
    }

    [Test]
    public void An_unknown_region_lifecycle_fails_closed_to_no_residency()
    {
        var regions = TenantProjection.ToRegions(
            [SampleTenant.RegionDescriptor(status: (TenantRegionLifecycleStatus)99)]);

        Assert.Multiple(() =>
        {
            Assert.That(regions[0].Status, Is.EqualTo(ExplorerTenantRegionLifecycle.None));
            Assert.That(regions[0].IsResident, Is.False);
        });
    }

    [Test]
    public void A_region_keeps_allowed_and_resident_as_two_separate_facts()
    {
        // An operator can revoke a region the tenant is still draining out of, so
        // "not allowed" and "not resident" must stay independent.
        var regions = TenantProjection.ToRegions(
        [
            SampleTenant.RegionDescriptor(status: TenantRegionLifecycleStatus.Draining, isAllowed: false),
            SampleTenant.RegionDescriptor("northeurope", TenantRegionLifecycleStatus.None, isAllowed: true),
        ]);

        Assert.Multiple(() =>
        {
            Assert.That(regions[0].IsAllowed, Is.False);
            Assert.That(regions[0].IsResident, Is.True);
            Assert.That(regions[1].IsAllowed, Is.True);
            Assert.That(regions[1].IsResident, Is.False);
        });
    }

    [Test]
    public void A_creation_result_reports_the_subjects_the_server_actually_seeded()
    {
        var creation = TenantProjection.ToCreation(new TenantCreationResult
        {
            TenantId = "newco",
            Status = TenantLifecycleStatus.Active,
            AdminSubjects = ["user:ada"],
        });

        Assert.Multiple(() =>
        {
            Assert.That(creation.TenantId, Is.EqualTo("newco"));
            Assert.That(creation.Status, Is.EqualTo(ExplorerTenantLifecycle.Active));
            Assert.That(creation.AdminSubjects, Is.EqualTo(new[] { "user:ada" }));
        });
    }

    [Test]
    public void An_admin_change_carries_the_resulting_set_so_a_panel_need_not_re_read()
    {
        var change = TenantProjection.ToAdminChange(new TenantAdminSubjectChangeResult
        {
            TenantId = SampleTenant.TenantId,
            SubjectId = SampleTenant.SubjectId,
            Changed = true,
            Subjects = ["user:ada", "user:grace"],
        });

        Assert.Multiple(() =>
        {
            Assert.That(change.Changed, Is.True);
            Assert.That(change.Subjects, Has.Count.EqualTo(2));
            Assert.That(change.SubjectId, Is.EqualTo(SampleTenant.SubjectId));
        });
    }
}
