using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Plugins.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.Tenancy;

/// <summary>
/// Covers every facade operation reachable through the tenancy seam against a
/// fake client: the reply is projected onto the Explorer's domain model, the
/// arguments actually sent are the ones asked for, and a malformed argument is a
/// caller defect rather than a rendered refusal.
/// </summary>
[TestFixture]
public class TenantAdminServiceTests
{
    private FakeTenantAdminClient _client = null!;
    private TenantAdminService _service = null!;

    [SetUp]
    public void SetUp()
    {
        _client = new FakeTenantAdminClient();
        _service = new TenantAdminService(_client);
    }

    [Test]
    public void Constructor_rejects_a_null_client() =>
        Assert.That(() => new TenantAdminService(null!), Throws.ArgumentNullException);

    [Test]
    public async Task Get_current_tenant_projects_the_descriptor()
    {
        _client.CurrentTenantResult = SampleTenant.Descriptor(isDefault: true);

        var result = await _service.GetCurrentTenantAsync();

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Value.TenantId, Is.EqualTo(SampleTenant.TenantId));
            Assert.That(result.Value.Status, Is.EqualTo(ExplorerTenantLifecycle.Active));
            Assert.That(result.Value.IsDefault, Is.True);
        });
    }

    [Test]
    public async Task List_accessible_tenants_projects_every_row_in_order()
    {
        _client.AccessibleTenantsResult =
        [
            SampleTenant.Descriptor("a"),
            SampleTenant.Descriptor("b", TenantLifecycleStatus.Suspended),
        ];

        var result = await _service.ListAccessibleTenantsAsync();

        Assert.That(result.IsSuccess, Is.True);
        Assert.That(result.Value, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(result.Value!, Has.Count.EqualTo(2));
            Assert.That(result.Value![0].TenantId, Is.EqualTo("a"));
            Assert.That(result.Value![1].Status, Is.EqualTo(ExplorerTenantLifecycle.Suspended));
        });
    }

    [Test]
    public async Task List_accessible_tenants_returns_an_empty_list_rather_than_a_refusal()
    {
        _client.AccessibleTenantsResult = [];

        var result = await _service.ListAccessibleTenantsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Value, Is.Empty);
        });
    }

    [Test]
    public async Task Get_tenant_projects_status_regions_and_quotas()
    {
        var result = await _service.GetTenantAsync(SampleTenant.TenantId);

        Assert.That(result.IsSuccess, Is.True);
        var detail = result.Value!;
        Assert.Multiple(() =>
        {
            Assert.That(_client.LastTenantId, Is.EqualTo(SampleTenant.TenantId));
            Assert.That(detail.Status, Is.EqualTo(ExplorerTenantLifecycle.Active));
            Assert.That(detail.Regions, Has.Count.EqualTo(1));
            Assert.That(detail.Regions[0].RegionId, Is.EqualTo("westeurope"));
            Assert.That(detail.Regions[0].Status, Is.EqualTo(ExplorerTenantRegionLifecycle.Online));
            Assert.That(detail.Regions[0].IsAllowed, Is.True);
            Assert.That(detail.Quotas.MaxBytes, Is.EqualTo(1_000));
            Assert.That(detail.Quotas.MaxTreeCount, Is.Null);
        });
    }

    [Test]
    public void Get_tenant_rejects_an_empty_tenant_id() =>
        Assert.That(async () => await _service.GetTenantAsync(string.Empty), Throws.ArgumentException);

    [Test]
    public async Task Create_tenant_forwards_the_seeded_subjects_and_projects_the_result()
    {
        string[] seeded = ["user:ada"];

        var result = await _service.CreateTenantAsync("newco", seeded);

        Assert.That(result.IsSuccess, Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(_client.LastTenantId, Is.EqualTo("newco"));
            Assert.That(_client.LastAdminSubjects, Is.EqualTo(seeded));
            Assert.That(result.Value!.TenantId, Is.EqualTo("newco"));
            Assert.That(result.Value!.AdminSubjects, Is.EqualTo(seeded));
        });
    }

    [Test]
    public async Task Create_tenant_with_no_subjects_asks_the_server_to_seed_the_caller()
    {
        var result = await _service.CreateTenantAsync("newco");

        Assert.Multiple(() =>
        {
            Assert.That(_client.LastAdminSubjects, Is.Null);
            Assert.That(result.Value!.AdminSubjects, Is.Not.Empty);
        });
    }

    [Test]
    public void Create_tenant_rejects_a_missing_tenant_id() =>
        Assert.Multiple(() =>
        {
            Assert.That(async () => await _service.CreateTenantAsync(null!), Throws.ArgumentNullException);
            Assert.That(async () => await _service.CreateTenantAsync(string.Empty), Throws.ArgumentException);
        });

    [Test]
    public async Task Suspend_tenant_projects_both_ends_of_the_transition()
    {
        var result = await _service.SuspendTenantAsync(SampleTenant.TenantId);

        Assert.That(result.IsSuccess, Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(result.Value.PreviousStatus, Is.EqualTo(ExplorerTenantLifecycle.Active));
            Assert.That(result.Value.NewStatus, Is.EqualTo(ExplorerTenantLifecycle.Suspended));
            Assert.That(result.Value.Changed, Is.True);
        });
    }

    [Test]
    public async Task Resume_tenant_reports_no_change_when_the_server_moved_nothing()
    {
        _client.ChangedResult = false;

        var result = await _service.ResumeTenantAsync(SampleTenant.TenantId);

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Value.NewStatus, Is.EqualTo(ExplorerTenantLifecycle.Active));
            Assert.That(result.Value.Changed, Is.False);
        });
    }

    [Test]
    public void Suspend_and_resume_reject_an_empty_tenant_id() =>
        Assert.Multiple(() =>
        {
            Assert.That(async () => await _service.SuspendTenantAsync(string.Empty), Throws.ArgumentException);
            Assert.That(async () => await _service.ResumeTenantAsync(string.Empty), Throws.ArgumentException);
        });

    [Test]
    public async Task Delete_tenant_carries_the_cascaded_tree_count()
    {
        _client.CascadedTreeCount = 9;

        var result = await _service.DeleteTenantAsync(SampleTenant.TenantId);

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Value.CascadedTreeCount, Is.EqualTo(9));
            Assert.That(result.Value.TenantId, Is.EqualTo(SampleTenant.TenantId));
        });
    }

    [Test]
    public void Delete_tenant_rejects_an_empty_tenant_id() =>
        Assert.That(async () => await _service.DeleteTenantAsync(string.Empty), Throws.ArgumentException);

    [Test]
    public async Task Set_quotas_sends_an_unbounded_ceiling_as_absent_rather_than_zero()
    {
        var limits = new ExplorerTenantQuotaLimits
        {
            MaxBytes = 2_048,
            MaxKeys = null,
            MaxMemoryBytes = 0,
            BurstPercent = 25,
        };

        var result = await _service.SetQuotasAsync(SampleTenant.TenantId, limits);

        Assert.That(result.IsSuccess, Is.True);
        var sent = _client.LastQuotas!.Value;
        Assert.Multiple(() =>
        {
            Assert.That(sent.MaxBytes, Is.EqualTo(2_048));
            Assert.That(sent.MaxKeys, Is.Null, "an unbounded ceiling must stay absent on the wire");
            Assert.That(sent.MaxMemoryBytes, Is.EqualTo(0), "a ceiling of zero is a real cap, not unbounded");
            Assert.That(sent.BurstPercent, Is.EqualTo(25));
            Assert.That(result.Value.MaxKeys, Is.Null);
            Assert.That(result.Value.MaxMemoryBytes, Is.EqualTo(0));
        });
    }

    [Test]
    public void Set_quotas_rejects_an_empty_tenant_id() =>
        Assert.That(
            async () => await _service.SetQuotasAsync(string.Empty, ExplorerTenantQuotaLimits.Unbounded),
            Throws.ArgumentException);

    [Test]
    public async Task Get_quota_usage_projects_the_reading_and_its_enforcement_scope()
    {
        _client.UsageResult = SampleTenant.UsageReport(scope: TenantQuotaEnforcementScope.PerCluster);

        var result = await _service.GetQuotaUsageAsync(SampleTenant.TenantId);

        Assert.That(result.IsSuccess, Is.True);
        var usage = result.Value!;
        Assert.Multiple(() =>
        {
            Assert.That(usage.EnforcementScope, Is.EqualTo(ExplorerTenantQuotaEnforcement.PerCluster));
            Assert.That(usage.HasUsage, Is.True);
            Assert.That(usage.BurstPercent, Is.EqualTo(10));
            Assert.That(usage.Bytes.Usage, Is.EqualTo(250));
            Assert.That(usage.Bytes.Limit, Is.EqualTo(1_000));
            Assert.That(usage.Bytes.BurstLimit, Is.EqualTo(1_100));
            Assert.That(usage.Limits.MaxBytes, Is.EqualTo(1_000));
        });
    }

    [Test]
    public void Get_quota_usage_rejects_an_empty_tenant_id() =>
        Assert.That(async () => await _service.GetQuotaUsageAsync(string.Empty), Throws.ArgumentException);

    [Test]
    public async Task Authorize_allowed_regions_forwards_the_whole_desired_set()
    {
        string[] desired = ["westeurope", "northeurope"];
        _client.AllowedRegionsResult = desired;

        var result = await _service.AuthorizeAllowedRegionsAsync(SampleTenant.TenantId, desired);

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(_client.LastRegions, Is.EqualTo(desired));
            Assert.That(result.Value, Is.EqualTo(desired));
        });
    }

    [Test]
    public void Authorize_allowed_regions_rejects_null_arguments() =>
        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await _service.AuthorizeAllowedRegionsAsync(string.Empty, []),
                Throws.ArgumentException);
            Assert.That(
                async () => await _service.AuthorizeAllowedRegionsAsync(SampleTenant.TenantId, null!),
                Throws.ArgumentNullException);
        });

    [Test]
    public async Task Set_residency_projects_the_added_removed_and_resulting_regions()
    {
        var result = await _service.SetResidencyAsync(SampleTenant.TenantId, ["westeurope", "northeurope"]);

        Assert.That(result.IsSuccess, Is.True);
        var change = result.Value!;
        Assert.Multiple(() =>
        {
            Assert.That(change.AddedRegions, Is.EqualTo(new[] { "northeurope" }));
            Assert.That(change.RemovedRegions, Is.Empty);
            Assert.That(change.Regions, Has.Count.EqualTo(1));
            Assert.That(change.Regions[0].IsResident, Is.True);
        });
    }

    [Test]
    public void Set_residency_rejects_null_arguments() =>
        Assert.Multiple(() =>
        {
            Assert.That(async () => await _service.SetResidencyAsync(string.Empty, []), Throws.ArgumentException);
            Assert.That(
                async () => await _service.SetResidencyAsync(SampleTenant.TenantId, null!),
                Throws.ArgumentNullException);
        });

    [Test]
    public async Task Get_region_status_projects_each_row()
    {
        var result = await _service.GetRegionStatusAsync(SampleTenant.TenantId);

        Assert.That(result.IsSuccess, Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(result.Value!, Has.Count.EqualTo(1));
            Assert.That(result.Value![0].RegionId, Is.EqualTo("westeurope"));
            Assert.That(result.Value![0].Status, Is.EqualTo(ExplorerTenantRegionLifecycle.Online));
        });
    }

    [Test]
    public void Get_region_status_rejects_an_empty_tenant_id() =>
        Assert.That(async () => await _service.GetRegionStatusAsync(string.Empty), Throws.ArgumentException);

    [Test]
    public async Task List_admin_subjects_projects_the_live_set()
    {
        _client.SubjectsResult = ["user:ada"];

        var result = await _service.ListAdminSubjectsAsync(SampleTenant.TenantId);

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Value!.TenantId, Is.EqualTo(SampleTenant.TenantId));
            Assert.That(result.Value!.Subjects, Is.EqualTo(new[] { "user:ada" }));
        });
    }

    [Test]
    public async Task Add_admin_subject_forwards_both_ids_and_reports_the_resulting_set()
    {
        var result = await _service.AddAdminSubjectAsync(SampleTenant.TenantId, SampleTenant.SubjectId);

        Assert.That(result.IsSuccess, Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(_client.LastTenantId, Is.EqualTo(SampleTenant.TenantId));
            Assert.That(_client.LastSubjectId, Is.EqualTo(SampleTenant.SubjectId));
            Assert.That(result.Value!.Changed, Is.True);
            Assert.That(result.Value!.Subjects, Is.Not.Empty);
        });
    }

    [Test]
    public async Task Remove_admin_subject_reports_no_change_on_a_non_member()
    {
        _client.ChangedResult = false;

        var result = await _service.RemoveAdminSubjectAsync(SampleTenant.TenantId, SampleTenant.SubjectId);

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Value!.Changed, Is.False);
        });
    }

    [Test]
    public void Admin_subject_operations_reject_empty_ids() =>
        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await _service.ListAdminSubjectsAsync(string.Empty),
                Throws.ArgumentException);
            Assert.That(
                async () => await _service.AddAdminSubjectAsync(SampleTenant.TenantId, string.Empty),
                Throws.ArgumentException);
            Assert.That(
                async () => await _service.RemoveAdminSubjectAsync(string.Empty, SampleTenant.SubjectId),
                Throws.ArgumentException);
        });

    [Test]
    public async Task List_grants_keeps_the_two_directions_apart_and_carries_each_state()
    {
        var result = await _service.ListGrantsAsync(SampleTenant.TenantId);

        Assert.That(result.IsSuccess, Is.True);
        var grants = result.Value!;
        Assert.Multiple(() =>
        {
            Assert.That(grants.Issued, Has.Count.EqualTo(1));
            Assert.That(grants.Issued[0].State, Is.EqualTo(ExplorerTenantGrantState.Pending));
            Assert.That(grants.Issued[0].AuthorizesAccess, Is.False, "a pending grant authorizes nothing");
            Assert.That(grants.Received, Has.Count.EqualTo(1));
            Assert.That(grants.Received[0].State, Is.EqualTo(ExplorerTenantGrantState.Active));
            Assert.That(grants.Received[0].AuthorizesAccess, Is.True);
            Assert.That(
                grants.Received[0].Operations,
                Is.EqualTo(ExplorerTenantGrantAccess.ReadWrite));
        });
    }

    [Test]
    public async Task Offer_grant_forwards_the_operations_and_reports_the_pending_grant()
    {
        _client.GrantResult = SampleTenant.Grant(TenantGrantLifecycleState.Pending, TenantGrantAccess.ReadWrite);

        var result = await _service.OfferGrantAsync(
            SampleTenant.TenantId,
            SampleTenant.OtherTenantId,
            SampleTenant.Scope,
            ExplorerTenantGrantAccess.ReadWrite);

        Assert.That(result.IsSuccess, Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(_client.LastGrantOperation, Is.EqualTo("offer"));
            Assert.That(_client.LastGranterTenantId, Is.EqualTo(SampleTenant.TenantId));
            Assert.That(_client.LastGranteeTenantId, Is.EqualTo(SampleTenant.OtherTenantId));
            Assert.That(_client.LastScope, Is.EqualTo(SampleTenant.Scope));
            Assert.That(_client.LastOperations, Is.EqualTo(TenantGrantAccess.ReadWrite));
            Assert.That(result.Value.Grant.State, Is.EqualTo(ExplorerTenantGrantState.Pending));
            Assert.That(result.Value.Grant.AuthorizesAccess, Is.False);
        });
    }

    [Test]
    public async Task Approve_grant_reaches_the_approve_transition_and_reports_it_active()
    {
        _client.GrantResult = SampleTenant.Grant(TenantGrantLifecycleState.Active);

        var result = await _service.ApproveGrantAsync(
            SampleTenant.TenantId,
            SampleTenant.OtherTenantId,
            SampleTenant.Scope);

        Assert.Multiple(() =>
        {
            Assert.That(_client.LastGrantOperation, Is.EqualTo("approve"));
            Assert.That(result.Value.Grant.State, Is.EqualTo(ExplorerTenantGrantState.Active));
            Assert.That(result.Value.Grant.AuthorizesAccess, Is.True);
        });
    }

    [Test]
    public async Task Reject_grant_reaches_the_reject_transition_and_reports_it_closed()
    {
        _client.GrantResult = SampleTenant.Grant(TenantGrantLifecycleState.Rejected);

        var result = await _service.RejectGrantAsync(
            SampleTenant.TenantId,
            SampleTenant.OtherTenantId,
            SampleTenant.Scope);

        Assert.Multiple(() =>
        {
            Assert.That(_client.LastGrantOperation, Is.EqualTo("reject"));
            Assert.That(result.Value.Grant.State, Is.EqualTo(ExplorerTenantGrantState.Rejected));
            Assert.That(result.Value.Grant.IsClosed, Is.True);
            Assert.That(result.Value.Grant.AuthorizesAccess, Is.False);
        });
    }

    [Test]
    public async Task Revoke_grant_reaches_the_revoke_transition_and_reports_it_closed()
    {
        _client.GrantResult = SampleTenant.Grant(TenantGrantLifecycleState.Revoked);

        var result = await _service.RevokeGrantAsync(
            SampleTenant.TenantId,
            SampleTenant.OtherTenantId,
            SampleTenant.Scope);

        Assert.Multiple(() =>
        {
            Assert.That(_client.LastGrantOperation, Is.EqualTo("revoke"));
            Assert.That(result.Value.Grant.State, Is.EqualTo(ExplorerTenantGrantState.Revoked));
            Assert.That(result.Value.Grant.IsClosed, Is.True);
            Assert.That(result.Value.Grant.AuthorizesAccess, Is.False);
        });
    }

    [Test]
    public void Grant_operations_reject_an_empty_tenant_id_or_scope() =>
        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await _service.OfferGrantAsync(
                    string.Empty, SampleTenant.OtherTenantId, SampleTenant.Scope, ExplorerTenantGrantAccess.Read),
                Throws.ArgumentException);
            Assert.That(
                async () => await _service.ApproveGrantAsync(
                    SampleTenant.TenantId, string.Empty, SampleTenant.Scope),
                Throws.ArgumentException);
            Assert.That(
                async () => await _service.RejectGrantAsync(
                    SampleTenant.TenantId, SampleTenant.OtherTenantId, string.Empty),
                Throws.ArgumentException);
            Assert.That(
                async () => await _service.RevokeGrantAsync(
                    string.Empty, SampleTenant.OtherTenantId, SampleTenant.Scope),
                Throws.ArgumentException);
            Assert.That(
                async () => await _service.ListGrantsAsync(string.Empty),
                Throws.ArgumentException);
        });
}
